package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/disk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testIngestToken = "1111111111111111111111111111111111111111111111111111111111111111"
	testQueryToken  = "2222222222222222222222222222222222222222222222222222222222222222"
)

func TestServerLifecycle(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	binary := filepath.Join(root, "wide-events")
	build := exec.Command("go", "build", "-ldflags", "-X main.version=1.2.3-test", "-o", binary, ".")
	buildOutput, err := build.CombinedOutput()
	require.NoError(t, err, string(buildOutput))

	database := filepath.Join(root, "wide-events.duckdb")
	socket := filepath.Join(root, "admin.sock")
	address := unusedAddress(t)
	server := startServer(t, address, database, socket)
	waitForUp(t, address)

	postJSON(t, address+"/v1/events", testIngestToken, `{
		"protocol_version": 1,
		"batch_id": "018f6f50-2f9d-7fb0-a258-6d9f95f20c01",
		"service": "checkout",
		"environment": "test",
		"dropped_events_since_last_batch": 0,
		"loss_report_id": null,
		"events": [{
			"id": "018f6f50-2f9d-7fb0-a258-6d9f95f20c02",
			"occurred_at": "2026-08-05T12:34:56.789Z",
			"kind": "request",
			"duration_ms": 42.7,
			"attributes": {"http.route.controller": "/checkout"}
		}]
	}`)

	queryResult := postJSON(t, address+"/v1/query", testQueryToken,
		`{"protocol_version":1,"sql":"SELECT count(*) AS count, (SELECT store_version FROM wide_events_store_status) AS store_version FROM wide_events","format":"json"}`)
	assert.JSONEq(t, `{"columns":["count","store_version"],"rows":[[1,"1.2.3-test"]],"truncated":false}`, queryResult)

	backup := exec.Command(binary, "admin", "backup")
	backup.Env = append(os.Environ(), "WIDE_EVENTS_ADMIN_SOCKET="+socket)
	backupOutput, err := backup.CombinedOutput()
	require.NoError(t, err, string(backupOutput))
	backupPath := strings.TrimSpace(string(backupOutput))
	backupDirectory, err := filepath.EvalSymlinks(filepath.Join(root, "backups"))
	require.NoError(t, err)
	assert.Equal(t, backupDirectory, filepath.Dir(backupPath))
	assert.Equal(t, ".duckdb", filepath.Ext(backupPath))
	backupInfo, err := os.Stat(backupPath)
	require.NoError(t, err)
	assert.Positive(t, backupInfo.Size())

	stopServer(t, server)
	_, err = os.Lstat(socket)
	assert.ErrorIs(t, err, os.ErrNotExist, "graceful shutdown must remove the admin socket")

	// A second process must reopen the cleanly checkpointed database and serve
	// the committed row before it too can shut down safely.
	address = unusedAddress(t)
	server = startServer(t, address, database, socket)
	waitForUp(t, address)
	queryResult = postJSON(t, address+"/v1/query", testQueryToken,
		`{"protocol_version":1,"sql":"SELECT count(*) AS count, (SELECT store_version FROM wide_events_store_status) AS store_version FROM wide_events","format":"json"}`)
	assert.JSONEq(t, `{"columns":["count","store_version"],"rows":[[1,"1.2.3-test"]],"truncated":false}`, queryResult)
	stopServer(t, server)
}

func TestServerHelperProcess(t *testing.T) {
	if os.Getenv("WIDE_EVENTS_TEST_HELPER") != "server" {
		return
	}
	version = "1.2.3-test"
	require.NoError(t, serveWithProbe(fixedCapacityProbe{}))
}

type fixedCapacityProbe struct{}

func (fixedCapacityProbe) Space(string) (disk.Space, error) {
	return disk.Space{Total: 50 << 30, Free: 20 << 30}, nil
}

func startServer(t *testing.T, address, database, socket string) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestServerHelperProcess$")
	command.Env = append(os.Environ(),
		"WIDE_EVENTS_TEST_HELPER=server",
		"WIDE_EVENTS_LISTEN="+address,
		"WIDE_EVENTS_DATABASE="+database,
		"WIDE_EVENTS_SERVICE=checkout",
		"WIDE_EVENTS_ENVIRONMENT=test",
		"WIDE_EVENTS_INGEST_TOKEN="+testIngestToken,
		"WIDE_EVENTS_QUERY_TOKEN="+testQueryToken,
		"WIDE_EVENTS_ADMIN_SOCKET="+socket,
	)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_, _ = command.Process.Wait()
		}
	})
	return command
}

func stopServer(t *testing.T, command *exec.Cmd) {
	t.Helper()
	require.NoError(t, command.Process.Signal(syscall.SIGTERM))
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		t.Fatal("server did not finish graceful shutdown")
	}
}

func waitForUp(t *testing.T, address string) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + address + "/up")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not become ready")
}

func postJSON(t *testing.T, endpoint, token, body string) string {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+endpoint, strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	if response.StatusCode != http.StatusOK {
		var failure map[string]any
		_ = json.Unmarshal(payload, &failure)
		t.Fatalf("POST %s returned %s: %s", endpoint, response.Status, payload)
	}
	return string(payload)
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)
}
