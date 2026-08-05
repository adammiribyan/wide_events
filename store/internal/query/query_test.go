package query

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/storage"
	duckdb "github.com/duckdb/duckdb-go/v2"
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

func TestPolicyRejectsSideEffectsBeforeTheyChangeState(t *testing.T) {
	database := openQueryTestDatabase(t)
	connection, err := database.QueryConnection(context.Background())
	require.NoError(t, err)
	_, err = connection.ExecContext(context.Background(), "CREATE TABLE query_policy_victim(value INTEGER)")
	require.NoError(t, err)
	_, err = connection.ExecContext(context.Background(), "INSERT INTO query_policy_victim VALUES (1)")
	require.NoError(t, err)
	_, err = connection.ExecContext(context.Background(), "CREATE SEQUENCE query_policy_sequence START 41")
	require.NoError(t, err)
	require.NoError(t, connection.Close())

	executor := NewExecutor(database, Limits{}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	for _, statement := range []string{
		"SELECT nextval('query_policy_sequence')",
		"SELECT * FROM query('DELETE FROM query_policy_victim RETURNING value')",
		"SELECT * FROM query('SELECT nextval(''query_policy_sequence'')')",
	} {
		t.Run(statement, func(t *testing.T) {
			assert.ErrorIs(t, executor.Validate(context.Background(), statement), ErrReadOnlyRequired)
		})
	}

	connection, err = database.QueryConnection(context.Background())
	require.NoError(t, err)
	defer connection.Close()
	var rows int
	require.NoError(t, connection.QueryRowContext(context.Background(), "SELECT count(*) FROM query_policy_victim").Scan(&rows))
	assert.Equal(t, 1, rows)
	var sequenceValue int64
	require.NoError(t, connection.QueryRowContext(context.Background(), "SELECT nextval('query_policy_sequence')").Scan(&sequenceValue))
	assert.Equal(t, int64(41), sequenceValue)
	_, err = connection.ExecContext(context.Background(), "SET lock_configuration = false")
	assert.Error(t, err)
}

func TestPinnedDuckDBSideEffectFunctionCharacterization(t *testing.T) {
	database := openQueryTestDatabase(t)
	connection, err := database.QueryConnection(context.Background())
	require.NoError(t, err)
	defer connection.Close()
	rows, err := connection.QueryContext(context.Background(), `
		SELECT DISTINCT function_name
		FROM duckdb_functions()
		WHERE has_side_effects
		ORDER BY function_name
	`)
	require.NoError(t, err)
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		actual = append(actual, name)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{
		"current_connection_id", "current_query", "current_query_id", "current_transaction_id",
		"currval", "error", "gen_random_uuid", "nextval", "random", "setseed", "sleep_ms",
		"stats", "uuid", "uuidv4", "uuidv7", "write_log",
	}, actual)

	var disabledFunctionsSettings int
	require.NoError(t, connection.QueryRowContext(context.Background(), `
		SELECT count(*) FROM duckdb_settings() WHERE name = 'disabled_functions'
	`).Scan(&disabledFunctionsSettings))
	assert.Zero(t, disabledFunctionsSettings)

	var dynamicQuerySideEffects bool
	require.NoError(t, connection.QueryRowContext(context.Background(), `
		SELECT coalesce(has_side_effects, false)
		FROM duckdb_functions()
		WHERE function_name = 'query' AND function_type = 'table'
	`).Scan(&dynamicQuerySideEffects))
	assert.False(t, dynamicQuerySideEffects)
}

func TestReadOnlyTransactionBlocksDirectAndDynamicSequenceMutation(t *testing.T) {
	database := openQueryTestDatabase(t)
	connection, err := database.QueryConnection(context.Background())
	require.NoError(t, err)
	defer connection.Close()
	_, err = connection.ExecContext(context.Background(), "CREATE SEQUENCE read_only_guard_sequence START 41")
	require.NoError(t, err)

	err = connection.Raw(func(raw any) error {
		duckConnection := raw.(*duckdb.Conn)
		for _, statement := range []string{
			"SELECT nextval('read_only_guard_sequence')",
			"SELECT * FROM query('SELECT nextval(''read_only_guard_sequence'')')",
		} {
			executionErr := readOnlyTransaction(context.Background(), duckConnection, func() error {
				rows, queryErr := duckConnection.QueryContext(context.Background(), statement, nil)
				if rows != nil {
					_ = rows.Close()
				}
				return queryErr
			})
			if executionErr == nil {
				return fmt.Errorf("read-only transaction admitted %q", statement)
			}
		}
		return nil
	})
	require.NoError(t, err)
	var sequenceValue int64
	require.NoError(t, connection.QueryRowContext(context.Background(), "SELECT nextval('read_only_guard_sequence')").Scan(&sequenceValue))
	assert.Equal(t, int64(41), sequenceValue)
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

func TestDefaultResponseByteBoundary(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	for _, format := range []string{"json", "csv"} {
		t.Run(format, func(t *testing.T) {
			base := len("value\n\n")
			if format == "json" {
				base = len(`{"columns":["value"],"rows":[[""]],"truncated":false}` + "\n")
			}
			valueBytes := defaultMaxResponseBytes - int64(base)

			var exact bytes.Buffer
			meta, err := executor.Execute(context.Background(), Request{
				ProtocolVersion: 1,
				SQL:             fmt.Sprintf("SELECT repeat('x', %d) AS value", valueBytes),
				Format:          format,
			}, &exact)
			require.NoError(t, err)
			assert.Equal(t, defaultMaxResponseBytes, meta.Bytes)
			assert.Equal(t, int(defaultMaxResponseBytes), exact.Len())

			var over bytes.Buffer
			meta, err = executor.Execute(context.Background(), Request{
				ProtocolVersion: 1,
				SQL:             fmt.Sprintf("SELECT repeat('x', %d) AS value", valueBytes+1),
				Format:          format,
			}, &over)
			assert.ErrorIs(t, err, ErrResultLimit)
			assert.True(t, meta.Truncated)
			assert.LessOrEqual(t, meta.Bytes, defaultMaxResponseBytes)
			assert.LessOrEqual(t, int64(over.Len()), defaultMaxResponseBytes)
			if format == "json" {
				var decoded any
				assert.NoError(t, json.Unmarshal(over.Bytes(), &decoded))
			}
		})
	}
}

func TestDefaultRowBoundary(t *testing.T) {
	executor := newTestExecutor(t, Limits{})
	for _, rowCount := range []int{defaultMaxRows, defaultMaxRows + 1} {
		t.Run(fmt.Sprintf("rows_%d", rowCount), func(t *testing.T) {
			var output bytes.Buffer
			meta, err := executor.Execute(context.Background(), Request{
				ProtocolVersion: 1,
				SQL:             fmt.Sprintf("SELECT i FROM range(%d) AS values(i)", rowCount),
				Format:          "json",
			}, &output)
			assert.Equal(t, defaultMaxRows, meta.Rows)
			if rowCount == defaultMaxRows {
				require.NoError(t, err)
				assert.False(t, meta.Truncated)
			} else {
				assert.ErrorIs(t, err, ErrResultLimit)
				assert.True(t, meta.Truncated)
			}
			var decoded any
			assert.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
		})
	}
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
