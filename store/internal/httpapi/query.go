package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/adammiribyan/wide_events/store/internal/query"
)

const maxQueryRequestBytes = 1 << 20

func (server *server) query(response http.ResponseWriter, request *http.Request) {
	if !validBearer(request.Header.Get("Authorization"), server.config.QueryToken) {
		writeError(response, http.StatusUnauthorized)
		return
	}

	request.Body = http.MaxBytesReader(response, request.Body, maxQueryRequestBytes)
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var queryRequest query.Request
	if err := decoder.Decode(&queryRequest); err != nil {
		writeError(response, http.StatusBadRequest)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(response, http.StatusBadRequest)
		return
	}

	var result bytes.Buffer
	meta, err := server.queries.Execute(request.Context(), queryRequest, &result)
	if err == nil {
		writeQueryResult(response, http.StatusOK, queryRequest.Format, result.Bytes())
		return
	}

	switch {
	case errors.Is(err, query.ErrBusy):
		response.Header().Set("Retry-After", "2")
		writeError(response, http.StatusTooManyRequests)
	case errors.Is(err, query.ErrResultLimit):
		if meta.Truncated && result.Len() > 0 {
			writeQueryResult(response, http.StatusUnprocessableEntity, queryRequest.Format, result.Bytes())
			return
		}
		writeError(response, http.StatusUnprocessableEntity)
	case errors.Is(err, query.ErrExecutionLimit):
		writeError(response, http.StatusUnprocessableEntity)
	case errors.Is(err, query.ErrUnavailable):
		writeError(response, http.StatusServiceUnavailable)
	default:
		writeError(response, http.StatusBadRequest)
	}
}

func writeQueryResult(response http.ResponseWriter, status int, format string, result []byte) {
	contentType := "application/json"
	if format == "csv" {
		contentType = "text/csv"
	}
	response.Header().Set("Content-Type", contentType)
	response.WriteHeader(status)
	_, _ = response.Write(result)
}
