package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/adammiribyan/wide_events/store/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testQueryToken = "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"

func TestQueryRequiresQueryTokenAndRejectsIngestToken(t *testing.T) {
	server := newTestServer(t)
	body := []byte(`{"protocol_version":1,"sql":"SELECT 1 AS value","format":"json"}`)
	for name, authorization := range map[string]string{
		"missing":      "",
		"wrong":        "Bearer ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		"ingest token": "Bearer " + testIngestToken,
	} {
		t.Run(name, func(t *testing.T) {
			response := postQuery(t, server.handler, authorization, body)
			assert.Equal(t, http.StatusUnauthorized, response.Code)
		})
	}

	response := postQuery(t, server.handler, "Bearer "+testQueryToken, body)
	require.Equal(t, http.StatusOK, response.Code)
	assert.JSONEq(t, `{"columns":["value"],"rows":[[1]],"truncated":false}`, response.Body.String())
}

func TestQueryReturnsCSVWithContentType(t *testing.T) {
	server := newTestServer(t)
	response := postQuery(t, server.handler, "Bearer "+testQueryToken, []byte(`{
		"protocol_version":1,
		"sql":"SELECT 1 AS value",
		"format":"csv"
	}`))
	require.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "text/csv", response.Header().Get("Content-Type"))
	assert.Equal(t, "value\n1\n", response.Body.String())
}

func TestQueryHTTPErrorMappings(t *testing.T) {
	for name, testCase := range map[string]struct {
		err        error
		status     int
		retryAfter string
	}{
		"busy":         {err: query.ErrBusy, status: http.StatusTooManyRequests, retryAfter: "2"},
		"result limit": {err: query.ErrResultLimit, status: http.StatusUnprocessableEntity},
		"timeout":      {err: query.ErrExecutionLimit, status: http.StatusUnprocessableEntity},
		"read write":   {err: query.ErrReadOnlyRequired, status: http.StatusBadRequest},
		"invalid":      {err: query.ErrInvalidRequest, status: http.StatusBadRequest},
		"maintenance":  {err: query.ErrUnavailable, status: http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			database := openHTTPTestDatabase(t)
			handler := New(testConfig(), Dependencies{
				Database: database,
				Writer:   submitterFunc(nil),
				Query: queryExecutorFunc(func(context.Context, query.Request, io.Writer) (query.ResultMeta, error) {
					return query.ResultMeta{}, testCase.err
				}),
			})
			response := postQuery(t, handler, "Bearer "+testQueryToken, []byte(`{"protocol_version":1,"sql":"SELECT 1","format":"json"}`))
			assert.Equal(t, testCase.status, response.Code)
			assert.Equal(t, testCase.retryAfter, response.Header().Get("Retry-After"))
		})
	}
}

func TestQueryLimitResponseKeepsBoundedPartialResult(t *testing.T) {
	database := openHTTPTestDatabase(t)
	handler := New(testConfig(), Dependencies{
		Database: database,
		Writer:   submitterFunc(nil),
		Query: queryExecutorFunc(func(_ context.Context, _ query.Request, output io.Writer) (query.ResultMeta, error) {
			payload := []byte(`{"columns":["value"],"rows":[[1]],"truncated":true}` + "\n")
			_, err := output.Write(payload)
			return query.ResultMeta{Rows: 1, Bytes: int64(len(payload)), Truncated: true}, errors.Join(query.ErrResultLimit, err)
		}),
	})
	response := postQuery(t, handler, "Bearer "+testQueryToken, []byte(`{"protocol_version":1,"sql":"SELECT 1","format":"json"}`))
	require.Equal(t, http.StatusUnprocessableEntity, response.Code)
	var result struct {
		Truncated bool `json:"truncated"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	assert.True(t, result.Truncated)
}

func TestQueryRejectsMalformedOrTrailingRequestJSON(t *testing.T) {
	server := newTestServer(t)
	for _, body := range [][]byte{
		[]byte(`not-json`),
		[]byte(`{"protocol_version":1,"sql":"SELECT 1","format":"json"}{}`),
		bytes.Repeat([]byte(" "), maxQueryRequestBytes+1),
	} {
		response := postQuery(t, server.handler, "Bearer "+testQueryToken, body)
		assert.Equal(t, http.StatusBadRequest, response.Code)
	}
}

func postQuery(t *testing.T, handler http.Handler, authorization string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

type queryExecutorFunc func(context.Context, query.Request, io.Writer) (query.ResultMeta, error)

func (execute queryExecutorFunc) Execute(ctx context.Context, request query.Request, output io.Writer) (query.ResultMeta, error) {
	return execute(ctx, request, output)
}
