//go:build testprobe

package main

import (
	"context"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/disk"
	"github.com/adammiribyan/wide_events/store/internal/storage"
)

type deterministicProbe struct{}

func (deterministicProbe) Space(string) (disk.Space, error) {
	return disk.Space{Total: 50 << 30, Free: 20 << 30}, nil
}

func processProbe() disk.Probe { return deterministicProbe{} }

func beforeDatabaseOpen(ctx context.Context) error {
	if os.Getenv("WIDE_EVENTS_TESTPROBE_BLOCK_STARTUP") != "1" {
		return ctx.Err()
	}
	recordProcessStage("startup-blocked")
	<-ctx.Done()
	recordProcessStage("startup-canceled")
	return ctx.Err()
}

func wrapProcessHandler(handler http.Handler, database *storage.Database) http.Handler {
	if os.Getenv("WIDE_EVENTS_TESTPROBE_BLOCK_HANDLER") != "1" {
		return handler
	}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/testprobe/block" {
			handler.ServeHTTP(response, request)
			return
		}
		recordProcessStage("handler-start")
		<-request.Context().Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := database.Status(ctx)
		cancel()
		if err == nil {
			recordProcessStage("handler-db-access-ok")
		} else {
			recordProcessStage("handler-db-access-failed")
		}
		time.Sleep(50 * time.Millisecond)
		recordProcessStage("handler-stop")
	})
}

func processShutdownTimeout() time.Duration {
	value := os.Getenv("WIDE_EVENTS_TESTPROBE_SHUTDOWN_TIMEOUT")
	if value == "" {
		return writerDrainTimeout
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return writerDrainTimeout
	}
	return duration
}

var stageMutex sync.Mutex

func recordProcessStage(stage string) {
	path := os.Getenv("WIDE_EVENTS_TESTPROBE_STAGE_FILE")
	if path == "" {
		return
	}
	stageMutex.Lock()
	defer stageMutex.Unlock()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = file.WriteString(stage + "\n")
	_ = file.Close()
}

func beforeWriterShutdown() {
	recordProcessStage("before-writer")
	if os.Getenv("WIDE_EVENTS_TESTPROBE_BLOCK_BEFORE_WRITER") == "1" {
		select {}
	}
}
