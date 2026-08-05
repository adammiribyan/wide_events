package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/protocol"
	"github.com/adammiribyan/wide_events/store/internal/storage"
	duckdb "github.com/duckdb/duckdb-go/v2"
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

	require.Eventually(t, func() bool {
		status, statusErr := database.Status(context.Background())
		return statusErr == nil && status.RejectedBatches == 1 && status.QueuedIngestBytes == 0
	}, 5*time.Second, time.Millisecond)
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
	require.Eventually(t, func() bool {
		status, statusErr := database.Status(context.Background())
		return statusErr == nil && status.QueuedIngestBytes == 0
	}, 5*time.Second, time.Millisecond)
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

func TestDuckDBIOCommitErrorIsTypedForHTTPMapping(t *testing.T) {
	batch, canonical := validBatch(t)
	database := newBlockingDatabase()
	database.commitErr = &duckdb.Error{
		Type: duckdb.ErrorTypeIO,
		Msg:  "IO Error: failed to write database block",
	}
	close(database.release)
	writer := newWriter(database, int64(len(canonical)))
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })

	_, err := writer.Submit(context.Background(), batch, canonical)
	assert.ErrorIs(t, err, ErrInsufficientStorage)
	var databaseError *duckdb.Error
	assert.ErrorAs(t, err, &databaseError)
	assert.Equal(t, duckdb.ErrorTypeIO, databaseError.Type)
}

func TestTelemetryCoalescesWithoutBlockingQueueAdmission(t *testing.T) {
	batch, canonical := validBatch(t)
	database := newTelemetryBlockingDatabase()
	writer := newWriter(database, int64(2*len(canonical)))
	first := make(chan error, 1)
	second := make(chan error, 1)

	go func() {
		_, err := writer.Submit(context.Background(), batch, canonical)
		first <- err
	}()
	<-database.commitStarted
	go func() {
		_, err := writer.Submit(context.Background(), batch, canonical)
		second <- err
	}()
	waitForQueuedBytes(t, writer, int64(2*len(canonical)))

	rejected := make(chan error, 1)
	go func() {
		_, err := writer.Submit(context.Background(), batch, canonical)
		rejected <- err
	}()
	var rejectionPrompt bool
	select {
	case err := <-rejected:
		rejectionPrompt = errors.Is(err, ErrQueueFull)
	case <-time.After(250 * time.Millisecond):
	}
	gaugePublished := database.waitForQueued(int64(2*len(canonical)), 250*time.Millisecond)

	close(database.telemetryRelease)
	close(database.commitRelease)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.NoError(t, writer.Close(context.Background()))
	assert.True(t, rejectionPrompt, "queue rejection waited for telemetry storage")
	assert.True(t, gaugePublished, "aggregate queued bytes were not offered while commit was in flight")
	database.mutex.Lock()
	defer database.mutex.Unlock()
	assert.Equal(t, uint64(1), database.rejected)
	assert.Zero(t, database.queued[len(database.queued)-1])
}

func TestTelemetryRetriesTransientFailuresWithoutAnotherAdmission(t *testing.T) {
	batch, canonical := validBatch(t)
	database := &flakyTelemetryDatabase{queueFailures: 1, rejectionFailures: 1}
	writer := newWriter(database, int64(len(canonical)-1))
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })

	_, err := writer.Submit(context.Background(), batch, canonical)
	assert.ErrorIs(t, err, ErrQueueFull)
	require.Eventually(t, func() bool {
		database.mutex.Lock()
		defer database.mutex.Unlock()
		return database.rejected == 1 && len(database.queued) > 0 && database.queued[len(database.queued)-1] == 0
	}, time.Second, time.Millisecond)
}

func TestCloseRetriesTransientFinalTelemetryFailure(t *testing.T) {
	database := &flakyTelemetryDatabase{queueFailures: 1}
	writer := newWriter(database, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, writer.Close(ctx))
	database.mutex.Lock()
	defer database.mutex.Unlock()
	assert.GreaterOrEqual(t, database.queueCalls, 2)
	assert.Equal(t, []int64{0}, database.queued)
}

func TestCloseDeadlineStopsPermanentTelemetryRetries(t *testing.T) {
	batch, canonical := validBatch(t)
	database := &flakyTelemetryDatabase{queueFailures: 1000, rejectionFailures: 1000}
	writer := newWriter(database, int64(len(canonical)-1))

	_, err := writer.Submit(context.Background(), batch, canonical)
	assert.ErrorIs(t, err, ErrQueueFull)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, writer.Close(ctx), context.DeadlineExceeded)
}

func TestCloseCancelsAndJoinsInFlightTelemetryCall(t *testing.T) {
	database := newContextBlockingTelemetryDatabase()
	writer := newWriter(database, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- writer.Close(ctx) }()
	<-database.started

	err := <-result
	finishedBeforeReturn := false
	select {
	case <-database.finished:
		finishedBeforeReturn = true
	default:
		database.releaseOnce.Do(func() { close(database.release) })
		<-database.finished
	}
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, finishedBeforeReturn, "Close returned before the telemetry call terminated")
	assert.Zero(t, database.active.Load())
	callsAtReturn := database.calls.Load()
	runtime.Gosched()
	assert.Equal(t, callsAtReturn, database.calls.Load(), "telemetry accessed storage after Close returned")
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

type telemetryBlockingDatabase struct {
	mutex            sync.Mutex
	commitStarted    chan struct{}
	commitRelease    chan struct{}
	telemetryRelease chan struct{}
	queued           []int64
	rejected         uint64
}

func newTelemetryBlockingDatabase() *telemetryBlockingDatabase {
	return &telemetryBlockingDatabase{
		commitStarted:    make(chan struct{}, 1),
		commitRelease:    make(chan struct{}),
		telemetryRelease: make(chan struct{}),
	}
}

func (database *telemetryBlockingDatabase) CommitBatch(context.Context, protocol.Batch) (storage.CommitResult, error) {
	database.commitStarted <- struct{}{}
	<-database.commitRelease
	return storage.CommitResult{}, nil
}

func (database *telemetryBlockingDatabase) SetQueuedIngestBytes(_ context.Context, queued int64) error {
	database.mutex.Lock()
	database.queued = append(database.queued, queued)
	database.mutex.Unlock()
	return nil
}

func (database *telemetryBlockingDatabase) AddRejectedBatches(_ context.Context, count uint64) error {
	<-database.telemetryRelease
	database.mutex.Lock()
	database.rejected += count
	database.mutex.Unlock()
	return nil
}

func (database *telemetryBlockingDatabase) waitForQueued(expected int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		database.mutex.Lock()
		for _, queued := range database.queued {
			if queued == expected {
				database.mutex.Unlock()
				return true
			}
		}
		database.mutex.Unlock()
		time.Sleep(time.Millisecond)
	}
	return false
}

type flakyTelemetryDatabase struct {
	mutex             sync.Mutex
	queueFailures     int
	rejectionFailures int
	queued            []int64
	rejected          uint64
	queueCalls        int
}

func (database *flakyTelemetryDatabase) CommitBatch(context.Context, protocol.Batch) (storage.CommitResult, error) {
	return storage.CommitResult{}, nil
}

func (database *flakyTelemetryDatabase) SetQueuedIngestBytes(_ context.Context, queued int64) error {
	database.mutex.Lock()
	defer database.mutex.Unlock()
	database.queueCalls++
	if database.queueFailures > 0 {
		database.queueFailures--
		return errors.New("transient queue telemetry failure")
	}
	database.queued = append(database.queued, queued)
	return nil
}

type contextBlockingTelemetryDatabase struct {
	started     chan struct{}
	finished    chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	finishOnce  sync.Once
	releaseOnce sync.Once
	calls       atomic.Int32
	active      atomic.Int32
}

func newContextBlockingTelemetryDatabase() *contextBlockingTelemetryDatabase {
	return &contextBlockingTelemetryDatabase{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (database *contextBlockingTelemetryDatabase) CommitBatch(context.Context, protocol.Batch) (storage.CommitResult, error) {
	return storage.CommitResult{}, nil
}

func (database *contextBlockingTelemetryDatabase) SetQueuedIngestBytes(ctx context.Context, _ int64) error {
	database.calls.Add(1)
	database.active.Add(1)
	database.startedOnce.Do(func() { close(database.started) })
	select {
	case <-ctx.Done():
	case <-database.release:
	}
	database.active.Add(-1)
	database.finishOnce.Do(func() { close(database.finished) })
	return ctx.Err()
}

func (database *contextBlockingTelemetryDatabase) AddRejectedBatches(context.Context, uint64) error {
	return nil
}

func (database *flakyTelemetryDatabase) AddRejectedBatches(_ context.Context, count uint64) error {
	database.mutex.Lock()
	defer database.mutex.Unlock()
	if database.rejectionFailures > 0 {
		database.rejectionFailures--
		return errors.New("transient rejection telemetry failure")
	}
	database.rejected += count
	return nil
}

func TestAdmissionHookRejectsBeforeQueueOwnership(t *testing.T) {
	database := newBlockingDatabase()
	writer := newWriter(database, 1024)
	writer.SetAdmissionHook(func(context.Context) (func(), error) {
		return nil, errors.New("maintenance")
	})
	_, err := writer.Submit(context.Background(), protocol.Batch{}, []byte("batch"))
	assert.EqualError(t, err, "maintenance")
	select {
	case <-database.started:
		t.Fatal("rejected admission reached database writer")
	default:
	}
	close(database.release)
	require.NoError(t, writer.Close(context.Background()))
}

func TestAdmissionOwnershipAndDiskCallbackLastThroughCommit(t *testing.T) {
	database := newBlockingDatabase()
	database.commitErr = syscall.ENOSPC
	writer := newWriter(database, 1024)
	released := make(chan struct{})
	notified := make(chan error, 1)
	writer.SetAdmissionHook(func(context.Context) (func(), error) {
		return func() { close(released) }, nil
	})
	writer.SetNoSpaceHook(func(_ context.Context, err error) { notified <- err })
	done := make(chan error, 1)
	go func() {
		_, err := writer.Submit(context.Background(), protocol.Batch{}, []byte("batch"))
		done <- err
	}()
	<-database.started
	select {
	case <-released:
		t.Fatal("writer released admission before commit completed")
	default:
	}
	close(database.release)
	assert.ErrorIs(t, <-done, ErrInsufficientStorage)
	assert.ErrorIs(t, <-notified, ErrInsufficientStorage)
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("writer did not release admission after commit")
	}
	require.NoError(t, writer.Close(context.Background()))
}
