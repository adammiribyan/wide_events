// Package ingest serializes bounded, acknowledged batch commits.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/adammiribyan/wide_events/store/internal/protocol"
	"github.com/adammiribyan/wide_events/store/internal/storage"
)

var (
	ErrQueueFull           = errors.New("ingest writer queue is full")
	ErrInvalidCanonical    = errors.New("canonical batch bytes must not be empty")
	ErrInsufficientStorage = errors.New("insufficient storage for ingest commit")
	ErrClosed              = errors.New("ingest writer is closed")
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
}

// NewWriter starts one database writer with a canonical-byte admission limit.
func NewWriter(database *storage.Database, maxBytes int64) *Writer {
	return newWriter(database, maxBytes)
}

func newWriter(database database, maxBytes int64) *Writer {
	writer := &Writer{
		database: database,
		maxBytes: maxBytes,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	go writer.run()
	return writer
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
		writer.mutex.Unlock()
		_ = writer.database.AddRejectedBatches(context.Background(), 1)
		return storage.CommitResult{}, ErrQueueFull
	}
	writer.queue = append(writer.queue, item)
	writer.queuedBytes += charge
	writer.mutex.Unlock()

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
		return nil
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
			writer.mutex.Unlock()
			if closed {
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
		queuedBytes := writer.queuedBytes
		writer.mutex.Unlock()

		_ = writer.database.SetQueuedIngestBytes(context.Background(), queuedBytes)
		result, err := writer.database.CommitBatch(context.Background(), item.batch)
		if errors.Is(err, syscall.ENOSPC) {
			err = fmt.Errorf("%w: %w", ErrInsufficientStorage, err)
		}

		writer.mutex.Lock()
		writer.queuedBytes -= int64(len(item.canonical))
		queuedBytes = writer.queuedBytes
		writer.mutex.Unlock()
		_ = writer.database.SetQueuedIngestBytes(context.Background(), queuedBytes)
		item.response <- commitResponse{result: result, err: err}
	}
}
