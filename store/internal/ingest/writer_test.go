package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/protocol"
	"github.com/adammiribyan/wide_events/store/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueRejectsCanonicalBytesBeyondItsLimit(t *testing.T) {
	database := openTestDatabase(t)
	batch, canonical := validBatch(t)
	writer := NewWriter(database, int64(len(canonical)-1))
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })

	_, err := writer.Submit(context.Background(), batch, canonical)
	assert.ErrorIs(t, err, ErrQueueFull)

	status, statusErr := database.Status(context.Background())
	require.NoError(t, statusErr)
	assert.Equal(t, uint64(1), status.RejectedBatches)
	assert.Zero(t, status.QueuedIngestBytes)
}

func TestQueueAcceptsExactCanonicalByteLimitAndAcknowledgesCommit(t *testing.T) {
	database := openTestDatabase(t)
	batch, canonical := validBatch(t)
	writer := NewWriter(database, int64(len(canonical)))
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })

	first, err := writer.Submit(context.Background(), batch, canonical)
	require.NoError(t, err)
	second, err := writer.Submit(context.Background(), batch, canonical)
	require.NoError(t, err)

	assert.False(t, first.Duplicate)
	assert.True(t, second.Duplicate)
	status, err := database.Status(context.Background())
	require.NoError(t, err)
	assert.Zero(t, status.QueuedIngestBytes)
}

func TestQueueRejectsEmptyCanonicalCharge(t *testing.T) {
	database := openTestDatabase(t)
	batch, _ := validBatch(t)
	writer := NewWriter(database, 1024)
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })

	_, err := writer.Submit(context.Background(), batch, nil)
	assert.ErrorIs(t, err, ErrInvalidCanonical)
}

func TestCloseRejectsNewAdmissionsWithoutStartingWork(t *testing.T) {
	database := openTestDatabase(t)
	batch, canonical := validBatch(t)
	writer := NewWriter(database, 1<<20)
	require.NoError(t, writer.Close(context.Background()))

	_, err := writer.Submit(context.Background(), batch, canonical)
	assert.ErrorIs(t, err, ErrClosed)
}

func TestQueueChargesAggregateAndInFlightBytesWhileSerializingCommits(t *testing.T) {
	batch, canonical := validBatch(t)
	database := newBlockingDatabase()
	writer := newWriter(database, int64(2*len(canonical)))
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
	results := make(chan error, 2)

	go func() {
		_, err := writer.Submit(context.Background(), batch, canonical)
		results <- err
	}()
	<-database.started
	go func() {
		_, err := writer.Submit(context.Background(), batch, canonical)
		results <- err
	}()
	waitForQueuedBytes(t, writer, int64(2*len(canonical)))

	_, err := writer.Submit(context.Background(), batch, canonical)
	assert.ErrorIs(t, err, ErrQueueFull)
	close(database.release)
	require.NoError(t, <-results)
	require.NoError(t, <-results)

	database.mutex.Lock()
	defer database.mutex.Unlock()
	assert.Equal(t, 2, database.commits)
	assert.Equal(t, 1, database.maxActive)
	assert.Equal(t, uint64(1), database.rejected)
	require.NotEmpty(t, database.queued)
	assert.Zero(t, database.queued[len(database.queued)-1])
}

func TestCancelledSubmitLeavesAdmittedCommitOwnedByWriter(t *testing.T) {
	batch, canonical := validBatch(t)
	database := newBlockingDatabase()
	writer := newWriter(database, int64(len(canonical)))

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := writer.Submit(ctx, batch, canonical)
		result <- err
	}()
	<-database.started
	cancel()
	assert.ErrorIs(t, <-result, context.Canceled)

	close(database.release)
	require.NoError(t, writer.Close(context.Background()))
	database.mutex.Lock()
	defer database.mutex.Unlock()
	assert.Equal(t, 1, database.commits)
	require.NotEmpty(t, database.queued)
	assert.Zero(t, database.queued[len(database.queued)-1])
}

func TestNoSpaceCommitErrorIsTypedForHTTPMapping(t *testing.T) {
	batch, canonical := validBatch(t)
	database := newBlockingDatabase()
	database.commitErr = syscall.ENOSPC
	close(database.release)
	writer := newWriter(database, int64(len(canonical)))
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })

	_, err := writer.Submit(context.Background(), batch, canonical)
	assert.ErrorIs(t, err, ErrInsufficientStorage)
	assert.ErrorIs(t, err, syscall.ENOSPC)
}

func openTestDatabase(t *testing.T) *storage.Database {
	t.Helper()
	database, err := storage.Open(context.Background(), config.Config{
		Database:       filepath.Join(t.TempDir(), "wide-events.duckdb"),
		Service:        "checkout",
		Environment:    "test",
		QueryTempBytes: 64 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	return database
}

func validBatch(t *testing.T) (protocol.Batch, []byte) {
	t.Helper()
	duration := 12.5
	batch := protocol.Batch{
		ProtocolVersion: protocol.ProtocolVersion,
		BatchID:         "018f6f50-2f9d-7fb0-a258-6d9f95f20c01",
		Service:         "checkout",
		Environment:     "test",
		Events: []protocol.Event{{
			ID:         "018f6f50-2f9d-7fb0-a258-6d9f95f20d01",
			OccurredAt: time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC),
			Kind:       "request",
			DurationMS: &duration,
			Attributes: map[string]any{"report.format": "pdf"},
		}},
	}
	decoded, canonical, err := protocol.DecodeBatch(bytes.NewReader(mustJSON(t, batch)))
	require.NoError(t, err)
	return decoded, canonical
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return encoded
}

type blockingDatabase struct {
	mutex     sync.Mutex
	started   chan struct{}
	release   chan struct{}
	active    int
	maxActive int
	commits   int
	rejected  uint64
	queued    []int64
	commitErr error
}

func newBlockingDatabase() *blockingDatabase {
	return &blockingDatabase{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
}

func (database *blockingDatabase) CommitBatch(context.Context, protocol.Batch) (storage.CommitResult, error) {
	database.mutex.Lock()
	database.active++
	database.commits++
	if database.active > database.maxActive {
		database.maxActive = database.active
	}
	database.mutex.Unlock()
	database.started <- struct{}{}
	<-database.release
	database.mutex.Lock()
	database.active--
	database.mutex.Unlock()
	return storage.CommitResult{}, database.commitErr
}

func (database *blockingDatabase) SetQueuedIngestBytes(_ context.Context, queued int64) error {
	database.mutex.Lock()
	defer database.mutex.Unlock()
	database.queued = append(database.queued, queued)
	return nil
}

func (database *blockingDatabase) AddRejectedBatches(_ context.Context, count uint64) error {
	database.mutex.Lock()
	defer database.mutex.Unlock()
	database.rejected += count
	return nil
}

func waitForQueuedBytes(t *testing.T, writer *Writer, expected int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		writer.mutex.Lock()
		queued := writer.queuedBytes
		writer.mutex.Unlock()
		if queued == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued bytes did not reach %d", expected)
}
