package admin

import (
	"context"
	"io"
	"os"
	"path/filepath"
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
