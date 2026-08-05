// Package httpapi exposes the authenticated ingestion and routing-health API.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/ingest"
	"github.com/adammiribyan/wide_events/store/internal/protocol"
	"github.com/adammiribyan/wide_events/store/internal/storage"
)

const maxConcurrentDecodes = 20

type submitter interface {
	Submit(context.Context, protocol.Batch, []byte) (storage.CommitResult, error)
}

// Dependencies are the owned process resources used by the HTTP routes.
type Dependencies struct {
	Database *storage.Database
	Writer   submitter
	Logger   *slog.Logger
}

type server struct {
	config   config.Config
	database *storage.Database
	writer   submitter
	logger   *slog.Logger
	decoders chan struct{}
}

// New builds the public HTTP routes without starting a listener.
func New(cfg config.Config, dependencies Dependencies) http.Handler {
	logger := dependencies.Logger
	if logger == nil {
		logger = slog.Default()
	}
	api := &server{
		config:   cfg,
		database: dependencies.Database,
		writer:   dependencies.Writer,
		logger:   logger,
		decoders: make(chan struct{}, maxConcurrentDecodes),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", api.ingest)
	mux.HandleFunc("GET /up", api.health)
	return mux
}

func (server *server) ingest(response http.ResponseWriter, request *http.Request) {
	if !validBearer(request.Header.Get("Authorization"), server.config.IngestToken) {
		writeError(response, http.StatusUnauthorized)
		return
	}

	writeReady, degradationReason, err := server.database.WriteStatus(request.Context())
	if err != nil {
		writeError(response, http.StatusServiceUnavailable)
		return
	}
	if !writeReady {
		code := http.StatusServiceUnavailable
		if degradationReason == "disk_space_low" {
			code = http.StatusInsufficientStorage
		}
		writeError(response, code)
		return
	}

	select {
	case server.decoders <- struct{}{}:
	default:
		writeError(response, http.StatusTooManyRequests)
		return
	}

	body, err := readBoundedBody(response, request)
	if err != nil {
		<-server.decoders
		switch {
		case errors.Is(err, errCompressedBodyTooLarge), errors.Is(err, errDecodedBodyTooLarge):
			writeError(response, http.StatusRequestEntityTooLarge)
		case errors.Is(err, errUnsupportedEncoding):
			writeError(response, http.StatusUnsupportedMediaType)
		default:
			writeError(response, http.StatusBadRequest)
		}
		return
	}

	batch, canonical, err := protocol.DecodeBatch(bytes.NewReader(body))
	body = nil
	<-server.decoders
	if err != nil {
		if errors.Is(err, protocol.ErrUnsupportedProtocol) {
			writeError(response, http.StatusUpgradeRequired)
			return
		}
		if errors.Is(err, protocol.ErrBodyTooLarge) {
			writeError(response, http.StatusRequestEntityTooLarge)
			return
		}
		writeError(response, http.StatusBadRequest)
		return
	}

	result, err := server.writer.Submit(request.Context(), batch, canonical)
	if err != nil {
		switch {
		case errors.Is(err, ingest.ErrQueueFull):
			writeError(response, http.StatusTooManyRequests)
			return
		case errors.Is(err, ingest.ErrInsufficientStorage):
			writeError(response, http.StatusInsufficientStorage)
			return
		}
		writeError(response, http.StatusServiceUnavailable)
		return
	}

	warnings := []string{}
	if batch.Service != server.config.Service || batch.Environment != server.config.Environment {
		warnings = append(warnings, "client service/environment differs from store configuration")
		if !result.Duplicate {
			if err := server.database.AddTenancyMismatches(context.Background(), 1); err != nil {
				server.logger.Error("record tenancy mismatch", "error", err)
				writeError(response, http.StatusServiceUnavailable)
				return
			}
			server.logger.Warn("ingest tenancy mismatch",
				"expected_service", server.config.Service,
				"received_service", batch.Service,
				"expected_environment", server.config.Environment,
				"received_environment", batch.Environment,
			)
		}
	}

	writeJSON(response, http.StatusOK, protocol.Ack{
		ProtocolVersion: protocol.ProtocolVersion,
		BatchID:         batch.BatchID,
		Duplicate:       result.Duplicate,
		Warnings:        warnings,
	})
}

func (server *server) health(response http.ResponseWriter, request *http.Request) {
	writeReady, degradationReason, err := server.database.WriteStatus(request.Context())
	if err != nil {
		writeError(response, http.StatusServiceUnavailable)
		return
	}
	writeJSON(response, http.StatusOK, struct {
		ProtocolVersion   int    `json:"protocol_version"`
		WriteReady        bool   `json:"write_ready"`
		DegradationReason string `json:"degradation_reason"`
	}{
		ProtocolVersion:   protocol.ProtocolVersion,
		WriteReady:        writeReady,
		DegradationReason: degradationReason,
	})
}

func writeError(response http.ResponseWriter, code int) {
	writeJSON(response, code, struct {
		Error string `json:"error"`
	}{Error: http.StatusText(code)})
}

func writeJSON(response http.ResponseWriter, code int, body any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(code)
	_ = json.NewEncoder(response).Encode(body)
}
