package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/ingest"
	"github.com/adammiribyan/wide_events/store/internal/protocol"
	"github.com/adammiribyan/wide_events/store/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskDegradedHealthRemainsRoutable(t *testing.T) {
	server := newTestServer(t)
	require.NoError(t, server.database.SetWriteReady(context.Background(), false, "disk_space_low"))

	health := httptest.NewRecorder()
	server.handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/up", nil))
	assert.Equal(t, http.StatusOK, health.Code)
	assert.JSONEq(t, `{"protocol_version":1,"write_ready":false,"degradation_reason":"disk_space_low"}`, health.Body.String())

	ingestion := postEvents(t, server.handler, "Bearer "+testIngestToken, validBatchJSON(t, "checkout", "test"), "")
	assert.Equal(t, http.StatusInsufficientStorage, ingestion.Code)
}

func TestHealthOmitsStoreAndDatabaseVersions(t *testing.T) {
	server := newTestServer(t)
	response := httptest.NewRecorder()
	server.handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/up", nil))
	require.Equal(t, http.StatusOK, response.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, map[string]any{
		"protocol_version":   float64(protocol.ProtocolVersion),
		"write_ready":        true,
		"degradation_reason": "",
	}, body)
}

func TestHealthReturnsUnavailableWhenDatabaseCannotServeTraffic(t *testing.T) {
	database, err := storage.Open(context.Background(), config.Config{
		Database:       filepath.Join(t.TempDir(), "wide-events.duckdb"),
		Service:        "checkout",
		Environment:    "test",
		QueryTempBytes: 64 << 20,
	})
	require.NoError(t, err)
	writer := ingest.NewWriter(database, 32<<20)
	require.NoError(t, writer.Close(context.Background()))
	require.NoError(t, database.Close())
	handler := New(config.Config{IngestToken: testIngestToken}, Dependencies{Database: database, Writer: writer})

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/up", bytes.NewReader(nil)))
	assert.Equal(t, http.StatusServiceUnavailable, response.Code)
	assert.JSONEq(t, `{"protocol_version":1,"write_ready":false,"degradation_reason":"database_unavailable"}`, response.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	assert.Len(t, body, 3)
}
