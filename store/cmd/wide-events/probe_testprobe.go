//go:build testprobe

package main

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/disk"
	"github.com/adammiribyan/wide_events/store/internal/storage"
)

const (
	defaultProbeTotalBytes = int64(50 << 30)
	defaultProbeFreeBytes  = int64(20 << 30)
)

type deterministicProbe struct{}

// Space reports a fixed 50 GiB volume with 20 GiB free so integration tests
// never inherit the host's real capacity. WIDE_EVENTS_TESTPROBE_TOTAL_BYTES
// and WIDE_EVENTS_TESTPROBE_FREE_BYTES override either figure, which is how
// a test drives the store below its write floor and asserts the 507 path.
func (deterministicProbe) Space(string) (disk.Space, error) {
	return disk.Space{
		Total: probeBytes("WIDE_EVENTS_TESTPROBE_TOTAL_BYTES", defaultProbeTotalBytes),
		Free:  probeBytes("WIDE_EVENTS_TESTPROBE_FREE_BYTES", defaultProbeFreeBytes),
	}, nil
}

func probeBytes(name string, fallback int64) int64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
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
