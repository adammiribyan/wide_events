package query

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/storage"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

const (
	defaultActive           = 2
	defaultWaiting          = 4
	defaultWaitTimeout      = 2 * time.Second
	defaultExecutionTimeout = 15 * time.Second
	defaultMaxRows          = 10_000
	defaultMaxResponseBytes = int64(10 << 20)
)

type Limits struct {
	Active           int
	Waiting          int
	WaitTimeout      time.Duration
	ExecutionTimeout time.Duration
	MaxRows          int
	MaxResponseBytes int64
}

type Request struct {
	ProtocolVersion int    `json:"protocol_version"`
	SQL             string `json:"sql"`
	Format          string `json:"format"`
}

type ResultMeta struct {
	Columns   []string
	Rows      int
	Bytes     int64
	Truncated bool
}

type Executor struct {
	database      *storage.Database
	limits        Limits
	logger        *slog.Logger
	active        chan struct{}
	waiting       atomic.Int32
	hookMutex     sync.RWMutex
	admissionHook func(context.Context) (func(), error)
}

// SetAdmissionHook installs the maintenance admission used to pause queries
// during exclusive backup copies.
func (executor *Executor) SetAdmissionHook(hook func(context.Context) (func(), error)) {
	executor.hookMutex.Lock()
	executor.admissionHook = hook
	executor.hookMutex.Unlock()
}

func NewExecutor(database *storage.Database, limits Limits, logger *slog.Logger) *Executor {
	limits = withDefaults(limits)
	if logger == nil {
		logger = slog.Default()
	}
	return &Executor{
		database: database,
		limits:   limits,
		logger:   logger,
		active:   make(chan struct{}, limits.Active),
	}
}

func withDefaults(limits Limits) Limits {
	if limits.Active <= 0 {
		limits.Active = defaultActive
	}
	if limits.Waiting <= 0 {
		limits.Waiting = defaultWaiting
	}
	if limits.WaitTimeout <= 0 {
		limits.WaitTimeout = defaultWaitTimeout
	}
	if limits.ExecutionTimeout <= 0 {
		limits.ExecutionTimeout = defaultExecutionTimeout
	}
	if limits.MaxRows <= 0 {
		limits.MaxRows = defaultMaxRows
	}
	if limits.MaxResponseBytes <= 0 {
		limits.MaxResponseBytes = defaultMaxResponseBytes
	}
	return limits
}

func (executor *Executor) activeCount() int  { return len(executor.active) }
func (executor *Executor) waitingCount() int { return int(executor.waiting.Load()) }

func (executor *Executor) Validate(ctx context.Context, sqlText string) error {
	if sqlText == "" {
		return ErrReadOnlyRequired
	}
	connection, err := executor.database.QueryConnection(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer connection.Close()
	err = connection.Raw(func(raw any) error {
		duckConnection, ok := raw.(*duckdb.Conn)
		if !ok {
			return ErrUnavailable
		}
		return readOnlyTransaction(ctx, duckConnection, func() error {
			statement, prepareErr := prepareReadOnly(ctx, raw, sqlText)
			if prepareErr != nil {
				return prepareErr
			}
			return statement.Close()
		})
	})
	if err != nil && !errors.Is(err, ErrUnavailable) && ctx.Err() == nil {
		return errors.Join(ErrReadOnlyRequired, err)
	}
	return err
}

func (executor *Executor) Execute(ctx context.Context, request Request, output io.Writer) (meta ResultMeta, err error) {
	startedAt := time.Now()
	queryHash := sha256.Sum256([]byte(request.SQL))
	status := "error"
	defer func() {
		if err == nil {
			status = "ok"
		} else if errors.Is(err, ErrBusy) {
			status = "busy"
		} else if errors.Is(err, ErrExecutionLimit) {
			status = "execution_limit"
		} else if errors.Is(err, ErrResultLimit) {
			status = "result_limit"
		} else if errors.Is(err, ErrReadOnlyRequired) || errors.Is(err, ErrInvalidRequest) {
			status = "rejected"
		}
		executor.logger.Info("query completed",
			"duration", time.Since(startedAt),
			"rows", meta.Rows,
			"bytes", meta.Bytes,
			"status", status,
			"query_hash", hex.EncodeToString(queryHash[:]),
		)
	}()

	if request.ProtocolVersion != 1 || request.SQL == "" || (request.Format != "json" && request.Format != "csv") || output == nil {
		return meta, ErrInvalidRequest
	}
	executor.hookMutex.RLock()
	hook := executor.admissionHook
	executor.hookMutex.RUnlock()
	if hook != nil {
		release, hookErr := hook(ctx)
		if hookErr != nil {
			return meta, fmt.Errorf("%w: %v", ErrUnavailable, hookErr)
		}
		if release != nil {
			defer release()
		}
	}
	if err := executor.acquire(ctx); err != nil {
		return meta, err
	}
	defer func() { <-executor.active }()

	executionContext, cancel := context.WithTimeout(ctx, executor.limits.ExecutionTimeout)
	defer cancel()
	connection, err := executor.database.QueryConnection(executionContext)
	if err != nil {
		if executionContext.Err() != nil {
			return meta, ErrExecutionLimit
		}
		return meta, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer connection.Close()

	bounded := newLimitedWriter(output, executor.limits.MaxResponseBytes)
	err = connection.Raw(func(raw any) error {
		duckConnection, ok := raw.(*duckdb.Conn)
		if !ok {
			return ErrUnavailable
		}
		return readOnlyTransaction(executionContext, duckConnection, func() error {
			statement, prepareErr := prepareReadOnly(executionContext, raw, request.SQL)
			if prepareErr != nil {
				return prepareErr
			}
			defer statement.Close()

			rows, queryErr := statement.QueryContext(executionContext, nil)
			if queryErr != nil {
				return queryErr
			}
			defer rows.Close()
			meta.Columns = append([]string(nil), rows.Columns()...)
			if request.Format == "json" {
				return executor.writeJSON(executionContext, cancel, bounded, rows, &meta)
			}
			return executor.writeCSV(executionContext, cancel, bounded, rows, &meta)
		})
	})
	meta.Bytes = bounded.written
	if errors.Is(err, ErrResultLimit) {
		return meta, ErrResultLimit
	}
	if executionContext.Err() != nil {
		return meta, ErrExecutionLimit
	}
	if err != nil {
		return meta, err
	}
	return meta, nil
}

func (executor *Executor) acquire(ctx context.Context) error {
	select {
	case executor.active <- struct{}{}:
		return nil
	default:
	}

	for {
		waiting := executor.waiting.Load()
		if waiting >= int32(executor.limits.Waiting) {
			return ErrBusy
		}
		if executor.waiting.CompareAndSwap(waiting, waiting+1) {
			break
		}
	}
	defer executor.waiting.Add(-1)
	timer := time.NewTimer(executor.limits.WaitTimeout)
	defer timer.Stop()
	select {
	case executor.active <- struct{}{}:
		return nil
	case <-timer.C:
		return ErrBusy
	case <-ctx.Done():
		return ErrExecutionLimit
	}
}

func (executor *Executor) writeJSON(ctx context.Context, cancel context.CancelFunc, output *limitedWriter, rows driver.Rows, meta *ResultMeta) error {
	columns, err := json.Marshal(meta.Columns)
	if err != nil {
		return fmt.Errorf("encode columns: %w", err)
	}
	prefix := append(append([]byte(`{"columns":`), columns...), []byte(`,"rows":[`)...)
	const truncatedSuffix = `],"truncated":true}` + "\n"
	const completeSuffix = `],"truncated":false}` + "\n"
	if !output.canWrite(len(prefix) + len(completeSuffix)) {
		cancel()
		meta.Truncated = true
		return ErrResultLimit
	}
	if _, err := output.Write(prefix); err != nil {
		return err
	}

	values := make([]driver.Value, len(meta.Columns))
	types := databaseTypes(rows, len(meta.Columns))
	for {
		if err := rows.Next(values); err != nil {
			if errors.Is(err, io.EOF) {
				_, writeErr := output.Write([]byte(completeSuffix))
				return writeErr
			}
			if ctx.Err() != nil {
				return ErrExecutionLimit
			}
			return fmt.Errorf("read query row: %w", err)
		}
		if meta.Rows >= executor.limits.MaxRows {
			cancel()
			meta.Truncated = true
			_, writeErr := output.Write([]byte(truncatedSuffix))
			if writeErr != nil {
				return writeErr
			}
			return ErrResultLimit
		}
		normalized := make([]any, len(values))
		for index, value := range values {
			normalized[index] = normalize(value, types[index])
		}
		encoded, err := json.Marshal(normalized)
		if err != nil {
			return fmt.Errorf("encode query row: %w", err)
		}
		if meta.Rows > 0 {
			encoded = append([]byte{','}, encoded...)
		}
		if !output.canWrite(len(encoded) + len(completeSuffix)) {
			cancel()
			meta.Truncated = true
			if _, err := output.Write([]byte(truncatedSuffix)); err != nil {
				return err
			}
			return ErrResultLimit
		}
		if _, err := output.Write(encoded); err != nil {
			return err
		}
		meta.Rows++
	}
}

func (executor *Executor) writeCSV(ctx context.Context, cancel context.CancelFunc, output *limitedWriter, rows driver.Rows, meta *ResultMeta) error {
	if err := writeCSVRecord(output, meta.Columns); err != nil {
		cancel()
		meta.Truncated = true
		if errors.Is(err, errResponseTooLarge) {
			return ErrResultLimit
		}
		return err
	}
	values := make([]driver.Value, len(meta.Columns))
	types := databaseTypes(rows, len(meta.Columns))
	for {
		if err := rows.Next(values); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctx.Err() != nil {
				return ErrExecutionLimit
			}
			return fmt.Errorf("read query row: %w", err)
		}
		if meta.Rows >= executor.limits.MaxRows {
			cancel()
			meta.Truncated = true
			return ErrResultLimit
		}
		record := make([]string, len(values))
		for index, value := range values {
			record[index] = csvValue(normalize(value, types[index]))
		}
		if err := writeCSVRecord(output, record); err != nil {
			cancel()
			meta.Truncated = true
			if errors.Is(err, errResponseTooLarge) {
				return ErrResultLimit
			}
			return err
		}
		meta.Rows++
	}
}

func writeCSVRecord(output *limitedWriter, record []string) error {
	var encoded bytes.Buffer
	writer := csv.NewWriter(&encoded)
	if err := writer.Write(record); err != nil {
		return err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	_, err := output.Write(encoded.Bytes())
	return err
}

func databaseTypes(rows driver.Rows, count int) []string {
	types := make([]string, count)
	typedRows, ok := rows.(driver.RowsColumnTypeDatabaseTypeName)
	if !ok {
		return types
	}
	for index := range count {
		types[index] = typedRows.ColumnTypeDatabaseTypeName(index)
	}
	return types
}

func normalize(value any, databaseType string) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	case []byte:
		if databaseType == "UUID" && len(typed) == 16 {
			var id duckdb.UUID
			copy(id[:], typed)
			return (&id).String()
		}
		return base64.StdEncoding.EncodeToString(typed)
	case duckdb.UUID:
		return (&typed).String()
	case duckdb.Decimal:
		return typed.String()
	case *big.Int:
		return typed.String()
	case big.Int:
		return typed.String()
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Array, reflect.Slice:
		values := make([]any, reflected.Len())
		for index := range reflected.Len() {
			values[index] = normalize(reflected.Index(index).Interface(), "")
		}
		return values
	case reflect.Map:
		if reflected.Type().Key().Kind() == reflect.String {
			values := make(map[string]any, reflected.Len())
			iterator := reflected.MapRange()
			for iterator.Next() {
				values[iterator.Key().String()] = normalize(iterator.Value().Interface(), "")
			}
			return values
		}
	}
	return value
}

func csvValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case bool:
		return strconv.FormatBool(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32)
	}
	encoded, err := json.Marshal(value)
	if err == nil {
		return string(encoded)
	}
	return fmt.Sprint(value)
}
