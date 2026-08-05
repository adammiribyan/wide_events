package query

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicyAllowsDuckDBReadStatements(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	for _, statement := range []string{
		"SELECT 1",
		"WITH value AS (SELECT 1 AS n) SELECT n FROM value",
		"SHOW TABLES",
		"DESCRIBE SELECT 1 AS n",
	} {
		t.Run(statement, func(t *testing.T) {
			assert.NoError(t, executor.Validate(context.Background(), statement))
		})
	}
}

func TestPolicyRejectsMutatingAndMultiStatements(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	for _, statement := range []string{
		"DELETE FROM wide_events",
		"COPY wide_events TO '/tmp/x'",
		"ATTACH '/tmp/x.db'",
		"INSTALL httpfs",
		"SELECT 1; SELECT 2",
	} {
		t.Run(statement, func(t *testing.T) {
			assert.ErrorIs(t, executor.Validate(context.Background(), statement), ErrReadOnlyRequired)
		})
	}
}

func TestPolicyRejectsBypassCorpus(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	corpus, err := os.ReadFile("testdata/rejected.sql")
	require.NoError(t, err)
	for _, statement := range strings.Split(string(corpus), "\n----\n") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		t.Run(statement, func(t *testing.T) {
			assert.ErrorIs(t, executor.Validate(context.Background(), statement), ErrReadOnlyRequired)
		})
	}
}

func TestPolicyReliesOnLockedExternalAccessSettings(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	for _, statement := range []string{
		"SELECT * FROM read_csv('/etc/passwd')",
		"SELECT * FROM read_json('/etc/passwd')",
		"SELECT * FROM read_parquet('/etc/passwd')",
		"SELECT * FROM glob('/etc/*')",
		"SELECT * FROM read_csv('https://example.com/data.csv')",
	} {
		t.Run(statement, func(t *testing.T) {
			var output bytes.Buffer
			_, err := executor.Execute(context.Background(), Request{ProtocolVersion: 1, SQL: statement, Format: "json"}, &output)
			assert.Error(t, err)
			assert.NotErrorIs(t, err, ErrReadOnlyRequired)
		})
	}
}

func TestFifthWaiterGetsBusyWithoutEnteringActivePool(t *testing.T) {
	limits := Limits{Active: 2, Waiting: 4, WaitTimeout: 2 * time.Second, ExecutionTimeout: 5 * time.Second}
	executor := newTestExecutor(t, limits)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _ = executor.Execute(ctx, Request{ProtocolVersion: 1, SQL: "SELECT 1", Format: "json"}, &gateWriter{started: started, release: release})
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for active query")
		}
	}
	require.Equal(t, 2, executor.activeCount())
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _ = executor.Execute(ctx, Request{ProtocolVersion: 1, SQL: "SELECT 1", Format: "json"}, &bytes.Buffer{})
		}()
	}
	require.Eventually(t, func() bool { return executor.waitingCount() == 4 }, time.Second, 10*time.Millisecond)

	busyStarted := time.Now()
	_, err := executor.Execute(context.Background(), Request{ProtocolVersion: 1, SQL: "SELECT 1", Format: "json"}, &bytes.Buffer{})
	assert.ErrorIs(t, err, ErrBusy)
	assert.Less(t, time.Since(busyStarted), 500*time.Millisecond)
	cancel()
	close(release)
	wait.Wait()
}

func TestTimeoutInterruptsWorkAndReleasesPermit(t *testing.T) {
	executor := newTestExecutor(t, Limits{Active: 1, Waiting: 1, WaitTimeout: time.Second, ExecutionTimeout: 25 * time.Millisecond})
	_, err := executor.Execute(context.Background(), Request{
		ProtocolVersion: 1,
		SQL:             "SELECT sum(i) FROM range(1000000000000) AS numbers(i)",
		Format:          "json",
	}, &bytes.Buffer{})
	assert.ErrorIs(t, err, ErrExecutionLimit)

	var output bytes.Buffer
	_, err = executor.Execute(context.Background(), Request{ProtocolVersion: 1, SQL: "SELECT 1", Format: "json"}, &output)
	require.NoError(t, err)
	assert.Contains(t, output.String(), "[[1]]")
}

func TestWaitingRequestTimesOutBusyAndCanceledWaitIsExecutionLimit(t *testing.T) {
	executor := newTestExecutor(t, Limits{Active: 1, Waiting: 2, WaitTimeout: 40 * time.Millisecond, ExecutionTimeout: time.Second})
	activeContext, cancelActive := context.WithCancel(context.Background())
	defer cancelActive()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	activeDone := make(chan struct{})
	go func() {
		defer close(activeDone)
		_, _ = executor.Execute(activeContext, Request{ProtocolVersion: 1, SQL: "SELECT 1", Format: "json"}, &gateWriter{started: started, release: release})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for active query")
	}

	waitStarted := time.Now()
	_, err := executor.Execute(context.Background(), Request{ProtocolVersion: 1, SQL: "SELECT 1", Format: "json"}, &bytes.Buffer{})
	assert.ErrorIs(t, err, ErrBusy)
	assert.GreaterOrEqual(t, time.Since(waitStarted), 35*time.Millisecond)

	canceled, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	_, err = executor.Execute(canceled, Request{ProtocolVersion: 1, SQL: "SELECT 1", Format: "json"}, &bytes.Buffer{})
	assert.ErrorIs(t, err, ErrExecutionLimit)

	close(release)
	<-activeDone
}

func TestResultRowLimitReturnsBoundedValidJSON(t *testing.T) {
	executor := newTestExecutor(t, Limits{MaxRows: 3, MaxResponseBytes: 1 << 20})
	var output bytes.Buffer
	meta, err := executor.Execute(context.Background(), Request{
		ProtocolVersion: 1,
		SQL:             "SELECT i FROM range(5) AS values(i) ORDER BY i",
		Format:          "json",
	}, &output)
	assert.ErrorIs(t, err, ErrResultLimit)
	assert.Equal(t, 3, meta.Rows)
	assert.True(t, meta.Truncated)

	var result struct {
		Columns   []string `json:"columns"`
		Rows      [][]any  `json:"rows"`
		Truncated bool     `json:"truncated"`
	}
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	assert.Equal(t, []string{"i"}, result.Columns)
	assert.Equal(t, [][]any{{float64(0)}, {float64(1)}, {float64(2)}}, result.Rows)
	assert.True(t, result.Truncated)
}

func TestResultByteLimitStopsBeforeCrossingBound(t *testing.T) {
	const maxBytes = 120
	executor := newTestExecutor(t, Limits{MaxRows: 100, MaxResponseBytes: maxBytes})
	var output bytes.Buffer
	meta, err := executor.Execute(context.Background(), Request{
		ProtocolVersion: 1,
		SQL:             "SELECT repeat('x', 50) AS value FROM range(10)",
		Format:          "json",
	}, &output)
	assert.ErrorIs(t, err, ErrResultLimit)
	assert.LessOrEqual(t, output.Len(), maxBytes)
	assert.Equal(t, int64(output.Len()), meta.Bytes)
	var decoded any
	assert.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
}

func TestResultNormalizationIsDeterministic(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	request := Request{ProtocolVersion: 1, Format: "json", SQL: `
		SELECT
			TIMESTAMPTZ '2026-08-05 12:34:56+02' AS happened_at,
			blob '\x01\x02' AS payload,
			'018f6f50-2f9d-7fb0-a258-6d9f95f20c01'::UUID AS id,
			12.30::DECIMAL(4,2) AS amount,
			[1, 2] AS items,
			NULL AS missing
	`}
	var first, second bytes.Buffer
	_, err := executor.Execute(context.Background(), request, &first)
	require.NoError(t, err)
	_, err = executor.Execute(context.Background(), request, &second)
	require.NoError(t, err)
	assert.Equal(t, first.String(), second.String())
	assert.JSONEq(t, `{
		"columns":["happened_at","payload","id","amount","items","missing"],
		"rows":[["2026-08-05T10:34:56Z","AQI=","018f6f50-2f9d-7fb0-a258-6d9f95f20c01","12.3",[1,2],null]],
		"truncated":false
	}`, first.String())
}

func TestCSVHasHeaderRowsAndDeterministicValues(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	var output bytes.Buffer
	meta, err := executor.Execute(context.Background(), Request{
		ProtocolVersion: 1,
		SQL:             "SELECT 1 AS number, 'a,b' AS text UNION ALL SELECT 2, NULL ORDER BY number",
		Format:          "csv",
	}, &output)
	require.NoError(t, err)
	assert.False(t, meta.Truncated)
	records, err := csv.NewReader(&output).ReadAll()
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"number", "text"}, {"1", "a,b"}, {"2", ""}}, records)
}

func TestLogsContainHashAndBoundsButNeverRawSQL(t *testing.T) {
	database := openQueryTestDatabase(t)
	var logs bytes.Buffer
	executor := NewExecutor(database, Limits{}, slog.New(slog.NewTextHandler(&logs, nil)))
	secretSQL := "SELECT 'do-not-log-this-query' AS secret"
	_, err := executor.Execute(context.Background(), Request{ProtocolVersion: 1, SQL: secretSQL, Format: "json"}, &bytes.Buffer{})
	require.NoError(t, err)
	assert.NotContains(t, logs.String(), secretSQL)
	assert.NotContains(t, logs.String(), "do-not-log-this-query")
	assert.Contains(t, logs.String(), "query_hash=")
	assert.Contains(t, logs.String(), "duration=")
	assert.Contains(t, logs.String(), "rows=1")
	assert.Contains(t, logs.String(), "bytes=")
	assert.Contains(t, logs.String(), "status=ok")
}

func TestInvalidRequestIsRejectedBeforeExecution(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	for _, request := range []Request{
		{ProtocolVersion: 2, SQL: "SELECT 1", Format: "json"},
		{ProtocolVersion: 1, Format: "json"},
		{ProtocolVersion: 1, SQL: "SELECT 1", Format: "xml"},
	} {
		_, err := executor.Execute(context.Background(), request, &bytes.Buffer{})
		assert.True(t, errors.Is(err, ErrInvalidRequest))
	}
}

func newTestExecutor(t *testing.T, limits Limits) *Executor {
	t.Helper()
	return NewExecutor(openQueryTestDatabase(t), limits, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
}

func openQueryTestDatabase(t *testing.T) *storage.Database {
	t.Helper()
	database, err := storage.Open(context.Background(), config.Config{
		Database:       filepath.Join(t.TempDir(), "wide-events.duckdb"),
		Service:        "checkout",
		Environment:    "test",
		QueryTempBytes: 64 << 20,
		QueryActive:    2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	return database
}

type gateWriter struct {
	once    sync.Once
	started chan<- struct{}
	release <-chan struct{}
}

func (writer *gateWriter) Write(value []byte) (int, error) {
	writer.once.Do(func() {
		writer.started <- struct{}{}
		<-writer.release
	})
	return len(value), nil
}
