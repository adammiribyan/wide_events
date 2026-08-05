//go:build !testprobe

package main

import (
	"context"
	"net/http"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/disk"
	"github.com/adammiribyan/wide_events/store/internal/storage"
)

func processProbe() disk.Probe { return disk.StatFSProbe{} }

func beforeDatabaseOpen(context.Context) error { return nil }

func wrapProcessHandler(handler http.Handler, _ *storage.Database) http.Handler { return handler }

func processShutdownTimeout() time.Duration { return writerDrainTimeout }

func recordProcessStage(string) {}

func beforeWriterShutdown() {}
