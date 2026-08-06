package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func terminationContext() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	stopped := make(chan struct{})
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			signal.Stop(signals)
			close(stopped)
			cancel()
		})
	}
	go func() {
		select {
		case <-signals:
			// Restore the default disposition before exposing cancellation to
			// shutdown. A second signal therefore terminates a stuck process.
			signal.Stop(signals)
			cancel()
		case <-stopped:
		}
	}()
	return ctx, stop
}

type handlerTracker struct {
	mutex     sync.Mutex
	accepting bool
	wait      sync.WaitGroup
	handler   http.Handler
}

func newHandlerTracker(handler http.Handler) *handlerTracker {
	return &handlerTracker{accepting: true, handler: handler}
}

func (tracker *handlerTracker) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	tracker.mutex.Lock()
	if !tracker.accepting {
		tracker.mutex.Unlock()
		http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	tracker.wait.Add(1)
	tracker.mutex.Unlock()
	defer tracker.wait.Done()
	tracker.handler.ServeHTTP(response, request)
}

func (tracker *handlerTracker) stopAndWait(ctx context.Context) error {
	tracker.mutex.Lock()
	tracker.accepting = false
	tracker.mutex.Unlock()
	done := make(chan struct{})
	go func() {
		tracker.wait.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func stopHTTP(
	server *http.Server,
	serveResult <-chan error,
	cancelHandlerRoot context.CancelFunc,
	tracker *handlerTracker,
) (safe bool, result error) {
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), processShutdownTimeout())
	shutdownErr := server.Shutdown(shutdownContext)
	cancelShutdown()
	result = errors.Join(result, shutdownErr)
	if shutdownErr != nil {
		recordProcessStage("http-shutdown-timeout")
		cancelHandlerRoot()
		recordProcessStage("handler-root-canceled")
		result = errors.Join(result, server.Close())
	} else {
		cancelHandlerRoot()
	}

	if serveResult != nil {
		select {
		case err := <-serveResult:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				result = errors.Join(result, fmt.Errorf("join HTTP server: %w", err))
			}
			recordProcessStage("http-serve-stopped")
		case <-time.After(shutdownTimeout):
			return false, errors.Join(result, fmt.Errorf("join HTTP server: timeout"))
		}
	} else {
		recordProcessStage("http-serve-stopped")
	}

	handlerContext, cancelHandlers := context.WithTimeout(context.Background(), shutdownTimeout)
	handlerErr := tracker.stopAndWait(handlerContext)
	cancelHandlers()
	if handlerErr != nil {
		return false, errors.Join(result, fmt.Errorf("join HTTP handlers: %w", handlerErr))
	}
	recordProcessStage("http-handlers-stopped")
	return true, result
}
