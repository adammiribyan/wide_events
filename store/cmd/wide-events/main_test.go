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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testIngestToken = "1111111111111111111111111111111111111111111111111111111111111111"
	testQueryToken  = "2222222222222222222222222222222222222222222222222222222222222222"
)

func TestServerLifecycle(t *testing.T) {
	root := t.TempDir()
	binary := buildServer(t, root)

	database := filepath.Join(root, "wide-events.duckdb")
	socket := filepath.Join(root, "admin.sock")
	address := unusedAddress(t)
	server := startServer(t, binary, address, database, socket, nil)
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
	server = startServer(t, binary, address, database, socket, nil)
	waitForUp(t, address)
	queryResult = postJSON(t, address+"/v1/query", testQueryToken,
		`{"protocol_version":1,"sql":"SELECT count(*) AS count, (SELECT store_version FROM wide_events_store_status) AS store_version FROM wide_events","format":"json"}`)
	assert.JSONEq(t, `{"columns":["count","store_version"],"rows":[[1,"1.2.3-test"]],"truncated":false}`, queryResult)
	stopServer(t, server)
}

func TestShutdownTimeoutCancelsAndJoinsHandlersBeforeDatabaseClose(t *testing.T) {
	root := shortTempDir(t)
	binary := buildServer(t, root)
	database := filepath.Join(root, "wide-events.duckdb")
	socket := filepath.Join(root, "admin.sock")
	address := unusedAddress(t)
	stages := filepath.Join(root, "stages.log")
	server := startServer(t, binary, address, database, socket, []string{
		"WIDE_EVENTS_TESTPROBE_STAGE_FILE=" + stages,
		"WIDE_EVENTS_TESTPROBE_BLOCK_HANDLER=1",
		"WIDE_EVENTS_TESTPROBE_SHUTDOWN_TIMEOUT=100ms",
	})
	waitForUp(t, address)

	requestDone := make(chan error, 1)
	go func() {
		response, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + address + "/testprobe/block")
		if err == nil {
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	waitForStage(t, stages, "handler-start")
	require.NoError(t, server.Process.Signal(syscall.SIGTERM))
	err := waitForProcess(t, server, 10*time.Second)
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	assert.Equal(t, 1, exitError.ExitCode())
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("blocked HTTP client did not finish after forced connection close")
	}
	assertStageOrder(t, stages, "handler-start", "http-shutdown-timeout", "handler-root-canceled")
	assertStageOrder(t, stages, "handler-root-canceled", "handler-db-access-ok", "handler-stop", "http-handlers-stopped", "writer-stop", "checkpoint", "database-close")
	assertStageOrder(t, stages, "handler-root-canceled", "http-serve-stopped", "writer-stop")
}

func TestSignalDuringStartupCancelsBeforeDatabaseOpen(t *testing.T) {
	root := shortTempDir(t)
	binary := buildServer(t, root)
	database := filepath.Join(root, "wide-events.duckdb")
	stages := filepath.Join(root, "stages.log")
	server := startServer(t, binary, unusedAddress(t), database, filepath.Join(root, "admin.sock"), []string{
		"WIDE_EVENTS_TESTPROBE_STAGE_FILE=" + stages,
		"WIDE_EVENTS_TESTPROBE_BLOCK_STARTUP=1",
	})
	waitForStage(t, stages, "startup-blocked")
	require.NoError(t, server.Process.Signal(syscall.SIGTERM))
	err := waitForProcess(t, server, 5*time.Second)
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	assert.Equal(t, 1, exitError.ExitCode())
	assertStageOrder(t, stages, "startup-blocked", "startup-canceled")
	assert.NoFileExists(t, database)
}

func TestSecondSignalForceTerminatesStuckShutdown(t *testing.T) {
	root := shortTempDir(t)
	binary := buildServer(t, root)
	stages := filepath.Join(root, "stages.log")
	address := unusedAddress(t)
	server := startServer(t, binary, address, filepath.Join(root, "wide-events.duckdb"), filepath.Join(root, "admin.sock"), []string{
		"WIDE_EVENTS_TESTPROBE_STAGE_FILE=" + stages,
		"WIDE_EVENTS_TESTPROBE_BLOCK_BEFORE_WRITER=1",
	})
	waitForUp(t, address)
	require.NoError(t, server.Process.Signal(syscall.SIGTERM))
	waitForStage(t, stages, "before-writer")
	require.NoError(t, server.Process.Signal(syscall.SIGTERM))
	err := waitForProcess(t, server, 5*time.Second)
	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	waitStatus, ok := exitError.ProcessState.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	assert.True(t, waitStatus.Signaled())
	assert.Equal(t, syscall.SIGTERM, waitStatus.Signal())
}

func buildServer(t *testing.T, root string) string {
	t.Helper()
	binary := filepath.Join(root, "wide-events")
	build := exec.Command("go", "build", "-tags", "testprobe", "-ldflags", "-X main.version=1.2.3-test", "-o", binary, ".")
	buildOutput, err := build.CombinedOutput()
	require.NoError(t, err, string(buildOutput))
	return binary
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "we-main-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func startServer(t *testing.T, binary, address, database, socket string, extraEnv []string) *exec.Cmd {
	t.Helper()
	command := exec.Command(binary)
	command.Env = append(os.Environ(),
		"WIDE_EVENTS_LISTEN="+address,
		"WIDE_EVENTS_DATABASE="+database,
		"WIDE_EVENTS_SERVICE=checkout",
		"WIDE_EVENTS_ENVIRONMENT=test",
		"WIDE_EVENTS_INGEST_TOKEN="+testIngestToken,
		"WIDE_EVENTS_QUERY_TOKEN="+testQueryToken,
		"WIDE_EVENTS_ADMIN_SOCKET="+socket,
	)
	command.Env = append(command.Env, extraEnv...)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_, _ = command.Process.Wait()
		}
		if t.Failed() {
			t.Logf("server output:\n%s", output.String())
		}
	})
	return command
}

func stopServer(t *testing.T, command *exec.Cmd) {
	t.Helper()
	require.NoError(t, command.Process.Signal(syscall.SIGTERM))
	require.NoError(t, waitForProcess(t, command, 10*time.Second))
}

func waitForProcess(t *testing.T, command *exec.Cmd, timeout time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		_ = command.Process.Kill()
		<-done
		t.Fatal("server process did not exit")
		return nil
	}
}

func waitForStage(t *testing.T, path, stage string) {
	t.Helper()
	require.Eventually(t, func() bool {
		payload, err := os.ReadFile(path)
		return err == nil && strings.Contains(string(payload), stage+"\n")
	}, 10*time.Second, 10*time.Millisecond, "stage %q was not recorded", stage)
}

func assertStageOrder(t *testing.T, path string, stages ...string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	log := string(payload)
	offset := 0
	for _, stage := range stages {
		index := strings.Index(log[offset:], stage+"\n")
		if !assert.NotEqual(t, -1, index, "stage %q missing or out of order in:\n%s", stage, log) {
			return
		}
		offset += index + len(stage) + 1
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
