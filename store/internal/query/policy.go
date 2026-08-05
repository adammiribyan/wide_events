// Package query executes strictly bounded, read-only DuckDB queries.
package query

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

var (
	ErrReadOnlyRequired = errors.New("a single read-only statement is required")
	ErrBusy             = errors.New("query service is busy")
	ErrExecutionLimit   = errors.New("query execution limit reached")
	ErrResultLimit      = errors.New("query result limit reached")
	ErrInvalidRequest   = errors.New("invalid query request")
	ErrUnavailable      = errors.New("query service unavailable")
)

func prepareReadOnly(ctx context.Context, connection any, sqlText string) (*duckdb.Stmt, error) {
	duckConnection, ok := connection.(*duckdb.Conn)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported database connection", ErrUnavailable)
	}
	if err := proveSingleSelect(ctx, duckConnection, sqlText); err != nil {
		return nil, err
	}
	prepared, err := duckConnection.PrepareContext(ctx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("prepare read statement: %w", err)
	}
	statement, ok := prepared.(*duckdb.Stmt)
	if !ok {
		_ = prepared.Close()
		return nil, fmt.Errorf("%w: unsupported prepared statement", ErrUnavailable)
	}
	statementType, err := statement.StatementType()
	if err != nil {
		_ = statement.Close()
		return nil, fmt.Errorf("%w: classify statement: %v", ErrReadOnlyRequired, err)
	}
	if !allowed(statementType) {
		_ = statement.Close()
		return nil, ErrReadOnlyRequired
	}
	return statement, nil
}

// proveSingleSelect asks DuckDB's own parser to serialize the SQL as a SELECT
// AST. The SQL is data in a bound parameter, so this check cannot execute it.
// This catches multi-statements before PrepareContext, whose driver contract
// otherwise executes every statement except the final one while preparing.
func proveSingleSelect(ctx context.Context, connection *duckdb.Conn, sqlText string) error {
	rows, err := connection.QueryContext(ctx, "SELECT json_serialize_sql(?::VARCHAR)", []driver.NamedValue{{Ordinal: 1, Value: sqlText}})
	if err != nil {
		return fmt.Errorf("%w: parser unavailable", ErrReadOnlyRequired)
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		return fmt.Errorf("%w: parser result unavailable", ErrReadOnlyRequired)
	}
	result, ok := values[0].(map[string]any)
	if !ok {
		return fmt.Errorf("%w: parser result invalid", ErrReadOnlyRequired)
	}
	failed, _ := result["error"].(bool)
	statements, ok := result["statements"].([]any)
	if failed || !ok || len(statements) != 1 {
		return ErrReadOnlyRequired
	}
	return nil
}

func allowed(statementType duckdb.StmtType) bool {
	return statementType == duckdb.STATEMENT_TYPE_SELECT
}

var _ driver.Stmt = (*duckdb.Stmt)(nil)
