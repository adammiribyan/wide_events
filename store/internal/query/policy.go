// Package query executes strictly bounded, read-only DuckDB queries.
package query

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

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

// DuckDB 1.5.5 leaves has_side_effects NULL for these required table
// functions. range is an in-engine generator. The external readers are safe
// only because storage locks enable_external_access=false before serving SQL.
// Keep this pinned exception set minimal; every other NULL remains rejected.
type functionReference struct {
	name    string
	context string
}

var controlledNullFunctions = map[functionReference]struct{}{
	{name: "glob", context: "table"}:         {},
	{name: "range", context: "table"}:        {},
	{name: "read_csv", context: "table"}:     {},
	{name: "read_json", context: "table"}:    {},
	{name: "read_parquet", context: "table"}: {},
}

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
	functionNames := collectFunctions(statements[0])
	for reference := range functionNames {
		if reference.name == "query" {
			// DuckDB's dynamic query table function is catalogued as pure, but
			// can tunnel calls to side-effecting functions in its SQL string.
			return ErrReadOnlyRequired
		}
		metadata, metadataErr := connection.QueryContext(ctx, `
			SELECT
				count(*),
				count(has_side_effects),
				coalesce(bool_or(has_side_effects), false)
			FROM duckdb_functions()
			WHERE function_name = ?::VARCHAR
				AND CASE
					WHEN ?::VARCHAR = 'table' THEN function_type IN ('table', 'table_macro')
					ELSE function_type NOT IN ('table', 'table_macro')
				END
		`, []driver.NamedValue{
			{Ordinal: 1, Value: reference.name},
			{Ordinal: 2, Value: reference.context},
		})
		if metadataErr != nil {
			return fmt.Errorf("%w: function metadata unavailable", ErrReadOnlyRequired)
		}
		values := make([]driver.Value, 3)
		nextErr := metadata.Next(values)
		closeErr := metadata.Close()
		if nextErr != nil || closeErr != nil {
			return fmt.Errorf("%w: function metadata invalid", ErrReadOnlyRequired)
		}
		overloadCount, overloadCountOK := values[0].(int64)
		knownCount, knownCountOK := values[1].(int64)
		hasSideEffects, sideEffectsOK := values[2].(bool)
		if !overloadCountOK || !knownCountOK || !sideEffectsOK || overloadCount == 0 ||
			hasSideEffects {
			return ErrReadOnlyRequired
		}
		if knownCount != overloadCount {
			if _, controlled := controlledNullFunctions[reference]; !controlled {
				return ErrReadOnlyRequired
			}
		}
	}
	return nil
}

type astValue struct {
	value   any
	context string
}

func collectFunctions(root any) map[functionReference]struct{} {
	names := make(map[functionReference]struct{})
	pending := []astValue{{value: root, context: "expression"}}
	for len(pending) > 0 {
		item := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		switch typed := item.value.(type) {
		case map[string]any:
			if class, _ := typed["class"].(string); class == "FUNCTION" {
				if name, _ := typed["function_name"].(string); name != "" {
					names[functionReference{name: strings.ToLower(name), context: item.context}] = struct{}{}
				}
			}
			tableFunction, _ := typed["type"].(string)
			for key, child := range typed {
				context := "expression"
				if tableFunction == "TABLE_FUNCTION" && key == "function" {
					context = "table"
				}
				pending = append(pending, astValue{value: child, context: context})
			}
		case []any:
			for _, child := range typed {
				pending = append(pending, astValue{value: child, context: "expression"})
			}
		}
	}
	return names
}

func readOnlyTransaction(ctx context.Context, connection *duckdb.Conn, operation func() error) (err error) {
	if _, err := connection.ExecContext(ctx, "BEGIN TRANSACTION READ ONLY", nil); err != nil {
		return fmt.Errorf("%w: begin read-only transaction: %v", ErrUnavailable, err)
	}
	defer func() {
		_, rollbackErr := connection.ExecContext(context.Background(), "ROLLBACK", nil)
		if rollbackErr != nil {
			rollbackErr = fmt.Errorf("%w: rollback read-only transaction: %v", ErrUnavailable, rollbackErr)
			if err == nil {
				err = rollbackErr
			} else {
				err = errors.Join(err, rollbackErr)
			}
		}
	}()
	return operation()
}

func allowed(statementType duckdb.StmtType) bool {
	return statementType == duckdb.STATEMENT_TYPE_SELECT
}

var _ driver.Stmt = (*duckdb.Stmt)(nil)
