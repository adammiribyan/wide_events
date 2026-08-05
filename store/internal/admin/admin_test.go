package admin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeAdminManager struct{ calls atomic.Int32 }

func (manager *fakeAdminManager) Backup(context.Context) error {
	manager.calls.Add(1)
	return nil
}

func TestAdminTransportDeadlineCoversTheFiniteBackupOperation(t *testing.T) {
	assert.Greater(t, connectionDeadline, 5*time.Minute)
}

func TestAdminServerRejectsUnknownAndTrailingJSON(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown field":  `{"command":"backup","extra":true}` + "\n",
		"trailing value": `{"command":"backup"} {}` + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			server, client := net.Pipe()
			manager := &fakeAdminManager{}
			done := make(chan struct{})
			go func() { handle(server, manager); close(done) }()
			_, err := io.WriteString(client, payload)
			require.NoError(t, err)
			var result response
			require.NoError(t, json.NewDecoder(bufio.NewReader(client)).Decode(&result))
			assert.False(t, result.OK)
			assert.Zero(t, manager.calls.Load())
			require.NoError(t, client.Close())
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("admin handler did not stop")
			}
		})
	}
}

type cancelAdminManager struct {
	started  chan struct{}
	finished chan struct{}
}

func (manager *cancelAdminManager) Backup(ctx context.Context) error {
	close(manager.started)
	<-ctx.Done()
	close(manager.finished)
	return ctx.Err()
}

func TestAdminClientDisconnectCancelsAndJoinsBackup(t *testing.T) {
	server, client := net.Pipe()
	manager := &cancelAdminManager{started: make(chan struct{}), finished: make(chan struct{})}
	done := make(chan struct{})
	go func() { handle(server, manager); close(done) }()
	_, err := io.WriteString(client, `{"command":"backup"}`+"\n")
	require.NoError(t, err)
	<-manager.started
	require.NoError(t, client.Close())
	select {
	case <-manager.finished:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("client disconnect did not cancel backup")
	}
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("admin handler did not join canceled backup")
	}
}

func TestAdminResponseDecoderIsStrictAndBounded(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown field":  `{"ok":true,"extra":true}` + "\n",
		"trailing value": `{"ok":true} {}` + "\n",
		"oversized":      string(bytes.Repeat([]byte{'x'}, maximumCommandBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			var result response
			assert.Error(t, decodeResponse(strings.NewReader(payload), &result))
		})
	}
	var result response
	require.NoError(t, decodeResponse(strings.NewReader(`{"ok":true,"path":"/backup.duckdb"}`+"\n"), &result))
	assert.True(t, result.OK)
	assert.Equal(t, "/backup.duckdb", result.Path)
}

func TestAdminClientDelegatesBackupOverPrivateSocket(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "we-admin-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "admin.sock")
	manager := &fakeAdminManager{}
	started := make(chan error, 1)
	go func() { started <- Listen(socket, 0o600, manager) }()
	if !assert.Eventually(t, func() bool {
		_, err := os.Lstat(socket)
		return err == nil
	}, time.Second, 5*time.Millisecond) {
		select {
		case err := <-started:
			t.Fatalf("server failed to start: %v", err)
		default:
			t.Fatal("server did not create socket")
		}
	}

	assert.Equal(t, 0, RunClient(socket, []string{"backup"}, io.Discard, io.Discard))
	assert.Equal(t, int32(1), manager.calls.Load())
	info, err := os.Stat(socket)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	select {
	case err := <-started:
		t.Fatalf("server stopped unexpectedly: %v", err)
	default:
	}
}

func TestAdminClientRejectsUnknownCommandsWithoutConnecting(t *testing.T) {
	assert.Equal(t, 2, RunClient(filepath.Join(t.TempDir(), "missing.sock"), []string{"unknown"}, io.Discard, io.Discard))
}
