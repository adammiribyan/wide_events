package httpapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/ingest"
	"github.com/adammiribyan/wide_events/store/internal/protocol"
	"github.com/adammiribyan/wide_events/store/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testIngestToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestAuthRejectsMissingMalformedAndWrongBearerTokens(t *testing.T) {
	server := newTestServer(t)
	body := validBatchJSON(t, "checkout", "test")

	for name, authorization := range map[string]string{
		"missing":   "",
		"malformed": "Basic dXNlcjpwYXNz",
		"wrong":     "Bearer abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
	} {
		t.Run(name, func(t *testing.T) {
			response := postEvents(t, server.handler, authorization, body, "")
			assert.Equal(t, http.StatusUnauthorized, response.Code)
		})
	}

	response := postEvents(t, server.handler, "Bearer "+testIngestToken, body, "")
	assert.Equal(t, http.StatusOK, response.Code)
}

func TestGzipExpansionStopsAtTwoMiB(t *testing.T) {
	server := newTestServer(t)
	body := gzipBytes(t, bytes.Repeat([]byte(" "), protocol.MaxDecodedBodyBytes+1))

	response := postEvents(t, server.handler, "Bearer "+testIngestToken, body, "gzip")
	assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
}

func TestCompressedBodyStopsAt512KiB(t *testing.T) {
	server := newTestServer(t)
	body := bytes.Repeat([]byte(" "), maxCompressedBodyBytes+1)

	response := postEvents(t, server.handler, "Bearer "+testIngestToken, body, "")
	assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
}

func TestGzipStreamStopsAtCompressedLimit(t *testing.T) {
	server := newTestServer(t)
	incompressible := make([]byte, 600<<10)
	_, err := rand.Read(incompressible)
	require.NoError(t, err)
	body := gzipBytes(t, incompressible)
	require.Greater(t, len(body), maxCompressedBodyBytes)

	response := postEvents(t, server.handler, "Bearer "+testIngestToken, body, "gzip")
	assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
}

func TestUnsupportedContentEncodingIsRejected(t *testing.T) {
	server := newTestServer(t)
	response := postEvents(t, server.handler, "Bearer "+testIngestToken, validBatchJSON(t, "checkout", "test"), "br")
	assert.Equal(t, http.StatusUnsupportedMediaType, response.Code)
}

func TestValidationAndProtocolErrorsHaveDistinctStatuses(t *testing.T) {
	server := newTestServer(t)

	invalid := postEvents(t, server.handler, "Bearer "+testIngestToken, []byte("not-json"), "")
	assert.Equal(t, http.StatusBadRequest, invalid.Code)

	var unsupported map[string]any
	require.NoError(t, json.Unmarshal(validBatchJSON(t, "checkout", "test"), &unsupported))
	unsupported["protocol_version"] = 2
	protocolResponse := postEvents(t, server.handler, "Bearer "+testIngestToken, mustJSON(t, unsupported), "")
	assert.Equal(t, http.StatusUpgradeRequired, protocolResponse.Code)
}

func TestDecodeAdmissionRejectsRequestTwentyOne(t *testing.T) {
	server := newTestServer(t)
	release := make(chan struct{})
	started := make(chan struct{}, maxConcurrentDecodes)
	var wait sync.WaitGroup

	for range maxConcurrentDecodes {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodPost, "/v1/events", &blockingBody{
				started: started,
				release: release,
			})
			request.Header.Set("Authorization", "Bearer "+testIngestToken)
			server.handler.ServeHTTP(httptest.NewRecorder(), request)
		}()
	}

	for range maxConcurrentDecodes {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for decode handler admission")
		}
	}

	response := postEvents(t, server.handler, "Bearer "+testIngestToken, validBatchJSON(t, "checkout", "test"), "")
	assert.Equal(t, http.StatusTooManyRequests, response.Code)
	close(release)
	wait.Wait()
}

func TestAcknowledgementReportsCommitAndDuplicate(t *testing.T) {
	server := newTestServer(t)
	body := validBatchJSON(t, "checkout", "test")

	first := postEvents(t, server.handler, "Bearer "+testIngestToken, body, "")
	second := postEvents(t, server.handler, "Bearer "+testIngestToken, body, "")
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)

	assert.Equal(t, protocol.Ack{
		ProtocolVersion: protocol.ProtocolVersion,
		BatchID:         "018f6f50-2f9d-7fb0-a258-6d9f95f20c01",
		Warnings:        []string{},
	}, decodeAck(t, first))
	assert.Equal(t, protocol.Ack{
		ProtocolVersion: protocol.ProtocolVersion,
		BatchID:         "018f6f50-2f9d-7fb0-a258-6d9f95f20c01",
		Duplicate:       true,
		Warnings:        []string{},
	}, decodeAck(t, second))
}

func TestTenancyMismatchWarnsLogsAndIncrementsCounter(t *testing.T) {
	server := newTestServer(t)
	body := validBatchJSON(t, "billing", "production")

	first := postEvents(t, server.handler, "Bearer "+testIngestToken, body, "")
	second := postEvents(t, server.handler, "Bearer "+testIngestToken, body, "")
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	assert.Len(t, decodeAck(t, first).Warnings, 1)
	assert.Len(t, decodeAck(t, second).Warnings, 1)

	status, err := server.database.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), status.TenancyMismatches)
	logOutput := server.logs.String()
	assert.Contains(t, logOutput, "checkout")
	assert.Contains(t, logOutput, "billing")
	assert.Contains(t, logOutput, "test")
	assert.Contains(t, logOutput, "production")
	assert.NotContains(t, logOutput, testIngestToken)
	assert.Equal(t, 1, strings.Count(logOutput, "ingest tenancy mismatch"))
}

func TestTenancyMismatchCountSurvivesFailureAfterCommitAndWarnsOnRetry(t *testing.T) {
	database := openHTTPTestDatabase(t)
	writer := ingest.NewWriter(database, 32<<20)
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
	submitter := &responseLossSubmitter{inner: writer}
	logs := &bytes.Buffer{}
	handler := New(testConfig(), Dependencies{
		Database: database,
		Writer:   submitter,
		Logger:   slog.New(slog.NewTextHandler(logs, nil)),
	})
	body := validBatchJSON(t, "billing", "production")

	first := postEvents(t, handler, "Bearer "+testIngestToken, body, "")
	second := postEvents(t, handler, "Bearer "+testIngestToken, body, "")
	assert.Equal(t, http.StatusServiceUnavailable, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	assert.True(t, decodeAck(t, second).Duplicate)
	assert.Len(t, decodeAck(t, second).Warnings, 1)

	status, err := database.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), status.TenancyMismatches)
	assert.Equal(t, 1, strings.Count(logs.String(), "ingest tenancy mismatch"))
}

func TestDecoderAdmissionIsReleasedBeforeCommitAcknowledgement(t *testing.T) {
	database := openHTTPTestDatabase(t)
	submitter := &blockingSubmitter{started: make(chan struct{}, maxConcurrentDecodes+1), release: make(chan struct{})}
	handler := New(testConfig(), Dependencies{Database: database, Writer: submitter})
	var wait sync.WaitGroup

	for range maxConcurrentDecodes + 1 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response := postEvents(t, handler, "Bearer "+testIngestToken, validBatchJSON(t, "checkout", "test"), "")
			assert.Equal(t, http.StatusOK, response.Code)
		}()
	}

	for range maxConcurrentDecodes + 1 {
		select {
		case <-submitter.started:
		case <-time.After(5 * time.Second):
			t.Fatal("decoder admission remained held during commit acknowledgement")
		}
	}
	close(submitter.release)
	wait.Wait()
	assert.Equal(t, int32(maxConcurrentDecodes+1), submitter.calls.Load())
}

func TestTypedInsufficientStorageCommitErrorMapsTo507(t *testing.T) {
	database := openHTTPTestDatabase(t)
	handler := New(testConfig(), Dependencies{
		Database: database,
		Writer: submitterFunc(func(context.Context, protocol.Batch, []byte) (storage.CommitResult, error) {
			return storage.CommitResult{}, ingest.ErrInsufficientStorage
		}),
	})

	response := postEvents(t, handler, "Bearer "+testIngestToken, validBatchJSON(t, "checkout", "test"), "")
	assert.Equal(t, http.StatusInsufficientStorage, response.Code)
}

type testServer struct {
	handler  http.Handler
	database *storage.Database
	logs     *bytes.Buffer
}

func newTestServer(t *testing.T) testServer {
	t.Helper()
	database := openHTTPTestDatabase(t)

	writer := ingest.NewWriter(database, 32<<20)
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
	logs := &bytes.Buffer{}
	cfg := testConfig()
	return testServer{
		handler:  New(cfg, Dependencies{Database: database, Writer: writer, Logger: slog.New(slog.NewTextHandler(logs, nil))}),
		database: database,
		logs:     logs,
	}
}

func openHTTPTestDatabase(t *testing.T) *storage.Database {
	t.Helper()
	database, err := storage.Open(context.Background(), config.Config{
		Database:       filepath.Join(t.TempDir(), "wide-events.duckdb"),
		Service:        "checkout",
		Environment:    "test",
		QueryTempBytes: 64 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	return database
}

func testConfig() config.Config {
	return config.Config{
		Service:          "checkout",
		Environment:      "test",
		IngestToken:      testIngestToken,
		QueryToken:       testQueryToken,
		WriterQueueBytes: 32 << 20,
	}
}

func postEvents(t *testing.T, handler http.Handler, authorization string, body []byte, encoding string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	request.Header.Set("Content-Type", "application/json")
	if encoding != "" {
		request.Header.Set("Content-Encoding", encoding)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func validBatchJSON(t *testing.T, service, environment string) []byte {
	t.Helper()
	duration := 12.5
	return mustJSON(t, protocol.Batch{
		ProtocolVersion: protocol.ProtocolVersion,
		BatchID:         "018f6f50-2f9d-7fb0-a258-6d9f95f20c01",
		Service:         service,
		Environment:     environment,
		Events: []protocol.Event{{
			ID:         "018f6f50-2f9d-7fb0-a258-6d9f95f20d01",
			OccurredAt: time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC),
			Kind:       "request",
			DurationMS: &duration,
			Attributes: map[string]any{"report.format": "pdf"},
		}},
	})
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return encoded
}

func gzipBytes(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	zipper := gzip.NewWriter(&compressed)
	_, err := zipper.Write(body)
	require.NoError(t, err)
	require.NoError(t, zipper.Close())
	return compressed.Bytes()
}

func decodeAck(t *testing.T, response *httptest.ResponseRecorder) protocol.Ack {
	t.Helper()
	var ack protocol.Ack
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &ack))
	return ack
}

type blockingBody struct {
	once    sync.Once
	started chan<- struct{}
	release <-chan struct{}
}

func (body *blockingBody) Read(buffer []byte) (int, error) {
	delivered := false
	body.once.Do(func() {
		delivered = true
		buffer[0] = '{'
		body.started <- struct{}{}
	})
	if delivered {
		return 1, nil
	}
	<-body.release
	return 0, io.EOF
}

func (body *blockingBody) Close() error { return nil }

type blockingSubmitter struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (submitter *blockingSubmitter) Submit(context.Context, protocol.Batch, []byte) (storage.CommitResult, error) {
	submitter.calls.Add(1)
	submitter.started <- struct{}{}
	<-submitter.release
	return storage.CommitResult{}, nil
}

type submitterFunc func(context.Context, protocol.Batch, []byte) (storage.CommitResult, error)

func (submitter submitterFunc) Submit(ctx context.Context, batch protocol.Batch, canonical []byte) (storage.CommitResult, error) {
	return submitter(ctx, batch, canonical)
}

type responseLossSubmitter struct {
	inner submitter
	lost  atomic.Bool
}

func (submitter *responseLossSubmitter) Submit(ctx context.Context, batch protocol.Batch, canonical []byte) (storage.CommitResult, error) {
	result, err := submitter.inner.Submit(ctx, batch, canonical)
	if err != nil {
		return storage.CommitResult{}, err
	}
	if submitter.lost.CompareAndSwap(false, true) {
		return storage.CommitResult{}, errors.New("simulated response loss after commit")
	}
	return result, nil
}

func (submitter *responseLossSubmitter) ConfigureTenancyLogging(logger *slog.Logger, service, environment string) {
	if configurable, ok := submitter.inner.(tenancyLoggingConfigurer); ok {
		configurable.ConfigureTenancyLogging(logger, service, environment)
	}
}
