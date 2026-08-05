package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/admin"
	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/disk"
	"github.com/adammiribyan/wide_events/store/internal/httpapi"
	"github.com/adammiribyan/wide_events/store/internal/ingest"
	"github.com/adammiribyan/wide_events/store/internal/maintenance"
	"github.com/adammiribyan/wide_events/store/internal/query"
	"github.com/adammiribyan/wide_events/store/internal/storage"
)

var version = "dev"

const (
	writerDrainTimeout = 2 * time.Second
	shutdownTimeout    = 5 * time.Second
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 {
		if args[0] == "admin" {
			socket := os.Getenv("WIDE_EVENTS_ADMIN_SOCKET")
			if socket == "" {
				socket = "/run/wide-events/admin.sock"
			}
			return admin.RunClient(socket, args[1:], os.Stdout, os.Stderr)
		}
		_, _ = fmt.Fprintln(os.Stderr, "usage: wide-events [admin backup]")
		return 2
	}
	if err := serve(); err != nil {
		slog.Error("wide-events stopped unsafely", "error", err)
		return 1
	}
	return 0
}

func serve() error {
	return serveWithProbe(disk.StatFSProbe{})
}

func serveWithProbe(probe disk.Probe) (result error) {
	if probe == nil {
		return fmt.Errorf("disk capacity probe is required")
	}
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	space, err := probe.Space(cfg.Database)
	if err != nil {
		return fmt.Errorf("probe database volume: %w", err)
	}
	if spillLimit := disk.QuerySpillLimit(space.Total); spillLimit <= 0 {
		return fmt.Errorf("database volume has invalid capacity %d", space.Total)
	} else if cfg.QueryTempBytes > spillLimit {
		cfg.QueryTempBytes = spillLimit
	}

	startupContext, startupCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	database, err := storage.Open(startupContext, cfg)
	startupCancel()
	if err != nil {
		return err
	}
	databaseOpen := true
	defer func() {
		if databaseOpen {
			result = errors.Join(result, database.Close())
		}
	}()

	versionContext, versionCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	err = database.SetStoreVersion(versionContext, version)
	versionCancel()
	if err != nil {
		return fmt.Errorf("publish store version: %w", err)
	}

	logger := slog.Default()
	writer := ingest.NewWriter(database, cfg.WriterQueueBytes)
	queries := query.NewExecutor(database, query.Limits{
		Active:           cfg.QueryActive,
		Waiting:          cfg.QueryWaiting,
		WaitTimeout:      cfg.QueryWaitTimeout,
		ExecutionTimeout: cfg.QueryTimeout,
	}, logger)
	manager := maintenance.NewManager(maintenance.Dependencies{
		Config: cfg, Database: database, Writer: writer, Queries: queries, Probe: probe, Logger: logger,
	})
	handler := httpapi.New(cfg, httpapi.Dependencies{
		Database: database, Writer: writer, Query: queries, Recovery: manager, Logger: logger,
	})

	adminContext, stopAdmin := context.WithCancel(context.Background())
	adminResult := make(chan error, 1)
	go func() {
		adminResult <- admin.ListenContext(adminContext, cfg.AdminSocket, 0o600, manager)
	}()
	if err, stopped := waitForAdmin(cfg.AdminSocket, adminResult); err != nil {
		stopAdmin()
		wait := (<-chan error)(adminResult)
		if stopped {
			wait = nil
		}
		shutdownErr := shutdown(writer, manager, nil, stopAdmin, wait, database)
		databaseOpen = false
		return errors.Join(err, shutdownErr)
	}

	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		shutdownErr := shutdown(writer, manager, nil, stopAdmin, adminResult, database)
		databaseOpen = false
		return errors.Join(fmt.Errorf("listen on %s: %w", cfg.Listen, err), shutdownErr)
	}
	httpServer := &http.Server{
		Addr:              cfg.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	httpResult := make(chan error, 1)
	go func() { httpResult <- httpServer.Serve(listener) }()
	manager.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	var trigger error
	select {
	case <-signals:
	case err := <-httpResult:
		if !errors.Is(err, http.ErrServerClosed) {
			trigger = fmt.Errorf("HTTP server stopped: %w", err)
		}
	case err := <-adminResult:
		trigger = fmt.Errorf("admin server stopped: %w", err)
		adminResult = nil
	}

	shutdownErr := shutdown(writer, manager, httpServer, stopAdmin, adminResult, database)
	databaseOpen = false
	return errors.Join(trigger, shutdownErr)
}

func waitForAdmin(socket string, result <-chan error) (err error, stopped bool) {
	deadline := time.NewTimer(shutdownTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			if err == nil {
				return fmt.Errorf("admin server stopped during startup"), true
			}
			return fmt.Errorf("start admin server: %w", err), true
		case <-ticker.C:
			info, err := os.Lstat(socket)
			if err == nil && info.Mode()&os.ModeSocket != 0 {
				return nil, false
			}
			if err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("inspect admin socket: %w", err), false
			}
		case <-deadline.C:
			return fmt.Errorf("start admin server: timeout"), false
		}
	}
}

func shutdown(
	writer *ingest.Writer,
	manager *maintenance.Manager,
	httpServer *http.Server,
	stopAdmin context.CancelFunc,
	adminResult <-chan error,
	database *storage.Database,
) error {
	var result error
	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), writerDrainTimeout)
		result = errors.Join(result, httpServer.Shutdown(ctx))
		cancel()
	}

	writerContext, cancelWriter := context.WithTimeout(context.Background(), writerDrainTimeout)
	result = errors.Join(result, writer.Close(writerContext))
	cancelWriter()

	result = errors.Join(result, manager.Close())
	stopAdmin()
	if adminResult != nil {
		select {
		case err := <-adminResult:
			result = errors.Join(result, err)
		case <-time.After(shutdownTimeout):
			result = errors.Join(result, fmt.Errorf("stop admin server: timeout"))
		}
	}

	checkpointContext, cancelCheckpoint := context.WithTimeout(context.Background(), shutdownTimeout)
	result = errors.Join(result, database.Checkpoint(checkpointContext))
	cancelCheckpoint()
	result = errors.Join(result, database.Close())
	return result
}
