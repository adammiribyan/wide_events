// Package ingest serializes bounded, acknowledged batch commits.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/protocol"
	"github.com/adammiribyan/wide_events/store/internal/storage"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

var (
	ErrQueueFull           = errors.New("ingest writer queue is full")
	ErrInvalidCanonical    = errors.New("canonical batch bytes must not be empty")
	ErrInsufficientStorage = errors.New("insufficient storage for ingest commit")
	ErrClosed              = errors.New("ingest writer is closed")
)

const (
	telemetryRetryMinimum = 10 * time.Millisecond
	telemetryRetryMaximum = time.Second
)

type commitResponse struct {
	result storage.CommitResult
	err    error
}

type submission struct {
	batch     protocol.Batch
	canonical []byte
	response  chan commitResponse
}

type database interface {
	CommitBatch(context.Context, protocol.Batch) (storage.CommitResult, error)
	SetQueuedIngestBytes(context.Context, int64) error
	AddRejectedBatches(context.Context, uint64) error
}

// Writer admits canonical batches under a byte budget and commits them in one
// dedicated goroutine. Admitted bytes remain charged until CommitBatch returns.
type Writer struct {
	database database
	maxBytes int64

	mutex       sync.Mutex
	queue       []*submission
	queuedBytes int64
	closed      bool
	wake        chan struct{}
	done        chan struct{}

	telemetryVersion uint64
	pendingRejected  uint64
	telemetryClosing bool
	telemetryWake    chan struct{}
	telemetryDone    chan struct{}
	telemetryErr     error

	tenancyLogger       *slog.Logger
	expectedService     string
	expectedEnvironment string
}

// NewWriter starts one database writer with a canonical-byte admission limit.
func NewWriter(database *storage.Database, maxBytes int64) *Writer {
	return newWriter(database, maxBytes)
}

func newWriter(database database, maxBytes int64) *Writer {
	writer := &Writer{
		database:      database,
		maxBytes:      maxBytes,
		wake:          make(chan struct{}, 1),
		done:          make(chan struct{}),
		telemetryWake: make(chan struct{}, 1),
		telemetryDone: make(chan struct{}),
	}
	go writer.run()
	go writer.publishTelemetry()
	return writer
}

// ConfigureTenancyLogging installs the process logger and configured tenancy
// used by the commit-owned first-commit drift warning.
func (writer *Writer) ConfigureTenancyLogging(logger *slog.Logger, service, environment string) {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	writer.tenancyLogger = logger
	writer.expectedService = service
	writer.expectedEnvironment = environment
}

// Submit admits one validated batch and waits for its commit result. Request
// cancellation stops the wait, but already admitted work remains owned by the
// writer and completes without spawning another goroutine.
func (writer *Writer) Submit(ctx context.Context, batch protocol.Batch, canonical []byte) (storage.CommitResult, error) {
	if len(canonical) == 0 {
		return storage.CommitResult{}, ErrInvalidCanonical
	}
	if err := ctx.Err(); err != nil {
		return storage.CommitResult{}, err
	}

	item := &submission{
		batch:     batch,
		canonical: canonical,
		response:  make(chan commitResponse, 1),
	}
	charge := int64(len(canonical))

	writer.mutex.Lock()
	if writer.closed {
		writer.mutex.Unlock()
		return storage.CommitResult{}, ErrClosed
	}
	if writer.maxBytes <= 0 || charge > writer.maxBytes || writer.queuedBytes > writer.maxBytes-charge {
		writer.pendingRejected++
		writer.telemetryVersion++
		writer.mutex.Unlock()
		writer.signalTelemetry()
		return storage.CommitResult{}, ErrQueueFull
	}
	writer.queue = append(writer.queue, item)
	writer.queuedBytes += charge
	writer.telemetryVersion++
	writer.mutex.Unlock()
	writer.signalTelemetry()

	select {
	case writer.wake <- struct{}{}:
	default:
	}

	select {
	case response := <-item.response:
		return response.result, response.err
	case <-ctx.Done():
		return storage.CommitResult{}, ctx.Err()
	}
}

// Close stops new admissions, drains all accepted work, and waits for the
// single writer goroutine within ctx's deadline.
func (writer *Writer) Close(ctx context.Context) error {
	writer.mutex.Lock()
	writer.closed = true
	writer.mutex.Unlock()
	select {
	case writer.wake <- struct{}{}:
	default:
	}

	select {
	case <-writer.done:
		select {
		case <-writer.telemetryDone:
			writer.mutex.Lock()
			err := writer.telemetryErr
			writer.mutex.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (writer *Writer) run() {
	defer close(writer.done)
	for {
		writer.mutex.Lock()
		if len(writer.queue) == 0 {
			closed := writer.closed
			if closed {
				writer.telemetryClosing = true
			}
			writer.mutex.Unlock()
			if closed {
				writer.signalTelemetry()
				return
			}
			<-writer.wake
			continue
		}
		item := writer.queue[0]
		writer.queue[0] = nil
		writer.queue = writer.queue[1:]
		if len(writer.queue) == 0 {
			writer.queue = nil
		}
		writer.mutex.Unlock()

		result, err := writer.database.CommitBatch(context.Background(), item.batch)
		err = classifyCommitError(err)
		if err == nil && result.TenancyMismatch && !result.Duplicate {
			writer.logTenancyMismatch(item.batch)
		}

		writer.mutex.Lock()
		writer.queuedBytes -= int64(len(item.canonical))
		writer.telemetryVersion++
		writer.mutex.Unlock()
		writer.signalTelemetry()
		item.response <- commitResponse{result: result, err: err}
	}
}

func (writer *Writer) logTenancyMismatch(batch protocol.Batch) {
	writer.mutex.Lock()
	logger := writer.tenancyLogger
	expectedService := writer.expectedService
	expectedEnvironment := writer.expectedEnvironment
	writer.mutex.Unlock()
	if logger == nil {
		return
	}
	logger.Warn("ingest tenancy mismatch",
		"expected_service", expectedService,
		"received_service", batch.Service,
		"expected_environment", expectedEnvironment,
		"received_environment", batch.Environment,
	)
}

func (writer *Writer) publishTelemetry() {
	defer close(writer.telemetryDone)
	retryDelay := telemetryRetryMinimum
	for {
		<-writer.telemetryWake
		for {
			writer.mutex.Lock()
			version := writer.telemetryVersion
			queuedBytes := writer.queuedBytes
			rejected := writer.pendingRejected
			closing := writer.telemetryClosing
			writer.mutex.Unlock()

			queueErr := writer.database.SetQueuedIngestBytes(context.Background(), queuedBytes)
			var rejectedErr error
			if rejected > 0 {
				rejectedErr = writer.database.AddRejectedBatches(context.Background(), rejected)
			}
			publishErr := errors.Join(queueErr, rejectedErr)

			writer.mutex.Lock()
			if rejectedErr == nil {
				writer.pendingRejected -= rejected
			}
			changed := writer.telemetryVersion != version
			closing = closing || writer.telemetryClosing
			if publishErr != nil && closing {
				writer.telemetryErr = fmt.Errorf("flush ingest telemetry: %w", publishErr)
			}
			writer.mutex.Unlock()

			if publishErr == nil {
				retryDelay = telemetryRetryMinimum
				if closing && !changed {
					return
				}
				if !changed {
					break
				}
				continue
			}
			if closing {
				return
			}

			timer := time.NewTimer(retryDelay)
			select {
			case <-timer.C:
			case <-writer.telemetryWake:
				if !timer.Stop() {
					<-timer.C
				}
			}
			if retryDelay < telemetryRetryMaximum/2 {
				retryDelay *= 2
			} else {
				retryDelay = telemetryRetryMaximum
			}
		}
	}
}

func (writer *Writer) signalTelemetry() {
	select {
	case writer.telemetryWake <- struct{}{}:
	default:
	}
}

func classifyCommitError(err error) error {
	if err == nil {
		return nil
	}
	var databaseError *duckdb.Error
	if errors.Is(err, syscall.ENOSPC) || errors.As(err, &databaseError) && databaseError.Type == duckdb.ErrorTypeIO {
		return fmt.Errorf("%w: %w", ErrInsufficientStorage, err)
	}
	return err
}
