// Package admin implements the private local administration protocol.
package admin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	maximumCommandBytes    = 4096
	backupOperationTimeout = 5 * time.Minute
	connectionDeadline     = backupOperationTimeout + 5*time.Second
)

// AdminManager is the only operation exposed on the local V1 protocol.
type AdminManager interface {
	Backup(context.Context) error
}

type command struct {
	Command string `json:"command"`
}

type response struct {
	OK             bool   `json:"ok"`
	Path           string `json:"path,omitempty"`
	Error          string `json:"error,omitempty"`
	RequiredBytes  int64  `json:"required_bytes,omitempty"`
	AvailableBytes int64  `json:"available_bytes,omitempty"`
}

type backupPathReporter interface{ LastBackupPath() string }
type backupCapacityError interface {
	BackupCapacity() (required, available int64)
}

// Listen binds a private Unix socket and serves bounded newline-delimited JSON
// requests. It returns when the listener fails.
func Listen(socket string, mode fs.FileMode, manager AdminManager) error {
	return ListenContext(context.Background(), socket, mode, manager)
}

// ListenContext serves the private protocol until ctx is canceled. Canceling
// also closes the active connection so an in-flight backup observes its
// request cancellation before the listener returns.
func ListenContext(ctx context.Context, socket string, mode fs.FileMode, manager AdminManager) error {
	if ctx == nil {
		return fmt.Errorf("admin context is required")
	}
	if manager == nil {
		return fmt.Errorf("admin manager is required")
	}
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket {
		return fmt.Errorf("admin socket must be an absolute clean path")
	}
	parent := filepath.Dir(socket)
	if err := prepareDirectory(parent); err != nil {
		return err
	}
	if err := prepareSocket(socket); err != nil {
		return err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen on admin socket: %w", err)
	}
	defer listener.Close()
	defer os.Remove(socket)
	if err := os.Chmod(socket, mode.Perm()); err != nil {
		return fmt.Errorf("set admin socket mode: %w", err)
	}
	if parent == "/run/wide-events" {
		if err := os.Chown(socket, 1000, 1000); err != nil {
			return fmt.Errorf("set admin socket owner: %w", err)
		}
	}
	var activeMutex sync.Mutex
	var active net.Conn
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
			activeMutex.Lock()
			if active != nil {
				_ = active.Close()
			}
			activeMutex.Unlock()
		case <-stopped:
		}
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept admin connection: %w", err)
		}
		activeMutex.Lock()
		if ctx.Err() != nil {
			activeMutex.Unlock()
			_ = connection.Close()
			return nil
		}
		active = connection
		activeMutex.Unlock()
		handle(connection, manager)
		activeMutex.Lock()
		active = nil
		activeMutex.Unlock()
	}
}

func prepareDirectory(parent string) error {
	info, err := os.Lstat(parent)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return fmt.Errorf("create admin socket directory: %w", err)
		}
		info, err = os.Lstat(parent)
	}
	if err != nil {
		return fmt.Errorf("inspect admin socket directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("admin socket parent must be a real directory")
	}
	if parent == "/run/wide-events" {
		if err := os.Chmod(parent, 0o700); err != nil {
			return fmt.Errorf("set admin socket directory mode: %w", err)
		}
		if err := os.Chown(parent, 1000, 1000); err != nil {
			return fmt.Errorf("set admin socket directory owner: %w", err)
		}
	}
	return nil
}

func prepareSocket(socket string) error {
	info, err := os.Lstat(socket)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect admin socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refuse to replace non-socket admin path")
	}
	connection, dialErr := net.DialTimeout("unix", socket, 100*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		return fmt.Errorf("admin socket is already active")
	}
	if err := os.Remove(socket); err != nil {
		return fmt.Errorf("remove stale admin socket: %w", err)
	}
	return nil
}

func handle(connection net.Conn, manager AdminManager) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(connectionDeadline))
	limited := io.LimitReader(connection, maximumCommandBytes+1)
	line, err := bufio.NewReader(limited).ReadBytes('\n')
	if err != nil || len(line) > maximumCommandBytes {
		_ = json.NewEncoder(connection).Encode(response{Error: "invalid command"})
		return
	}
	var request command
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.Command != "backup" {
		_ = json.NewEncoder(connection).Encode(response{Error: "unsupported command"})
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		_ = json.NewEncoder(connection).Encode(response{Error: "unsupported command"})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), backupOperationTimeout)
	defer cancel()
	clientGone := make(chan struct{})
	go func() {
		var buffer [1]byte
		_, _ = connection.Read(buffer[:])
		cancel()
		close(clientGone)
	}()
	err = manager.Backup(ctx)
	if err != nil {
		result := response{Error: err.Error()}
		var capacity backupCapacityError
		if errors.As(err, &capacity) {
			result.RequiredBytes, result.AvailableBytes = capacity.BackupCapacity()
		}
		_ = json.NewEncoder(connection).Encode(result)
	} else {
		result := response{OK: true}
		if reporter, ok := manager.(backupPathReporter); ok {
			result.Path = reporter.LastBackupPath()
		}
		_ = json.NewEncoder(connection).Encode(result)
	}
	_ = connection.Close()
	<-clientGone
}
