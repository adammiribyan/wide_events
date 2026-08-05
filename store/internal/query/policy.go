// Package query executes strictly bounded, read-only DuckDB queries.
package query

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
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

type functionReference struct {
	catalog string
	schema  string
	name    string
	kind    string
}

type functionIdentity struct {
	catalog  string
	schema   string
	name     string
	kind     string
	aliasOf  string
	internal bool
}

// DuckDB 1.5.5 leaves has_side_effects NULL for these required built-in table
// functions. range is an in-engine generator. The external readers are safe
// only because storage locks enable_external_access=false before serving SQL.
// Identity and kind are pinned so this exception never covers macros, aliases,
// or user-defined objects with the same base name.
var controlledNullFunctions = map[functionIdentity]struct{}{
	{catalog: "system", schema: "main", name: "glob", kind: "table", internal: true}:         {},
	{catalog: "system", schema: "main", name: "range", kind: "table", internal: true}:        {},
	{catalog: "system", schema: "main", name: "read_csv", kind: "table", internal: true}:     {},
	{catalog: "system", schema: "main", name: "read_json", kind: "table", internal: true}:    {},
	{catalog: "system", schema: "main", name: "read_parquet", kind: "table", internal: true}: {},
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
				lower(database_name),
				lower(schema_name),
				lower(function_type),
				coalesce(lower(alias_of), ''),
				internal,
				count(*),
				count(has_side_effects),
				coalesce(bool_or(has_side_effects), false)
			FROM duckdb_functions()
			WHERE lower(function_name) = ?::VARCHAR
				AND (?::VARCHAR = '' OR lower(database_name) = ?::VARCHAR)
				AND (?::VARCHAR = '' OR lower(schema_name) = ?::VARCHAR)
				AND CASE
					WHEN ?::VARCHAR = 'table' THEN function_type IN ('table', 'table_macro')
					ELSE function_type NOT IN ('table', 'table_macro')
				END
			GROUP BY database_name, schema_name, function_type, alias_of, internal
		`, []driver.NamedValue{
			{Ordinal: 1, Value: reference.name},
			{Ordinal: 2, Value: reference.catalog},
			{Ordinal: 3, Value: reference.catalog},
			{Ordinal: 4, Value: reference.schema},
			{Ordinal: 5, Value: reference.schema},
			{Ordinal: 6, Value: reference.kind},
		})
		if metadataErr != nil {
			return fmt.Errorf("%w: function metadata unavailable", ErrReadOnlyRequired)
		}
		identities := make(map[functionIdentity]struct{})
		for {
			values := make([]driver.Value, 8)
			nextErr := metadata.Next(values)
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				_ = metadata.Close()
				return fmt.Errorf("%w: function metadata invalid", ErrReadOnlyRequired)
			}
			identity := functionIdentity{name: reference.name}
			var identityOK bool
			identity.catalog, identityOK = values[0].(string)
			if !identityOK {
				_ = metadata.Close()
				return ErrReadOnlyRequired
			}
			identity.schema, identityOK = values[1].(string)
			if !identityOK {
				_ = metadata.Close()
				return ErrReadOnlyRequired
			}
			identity.kind, identityOK = values[2].(string)
			if !identityOK {
				_ = metadata.Close()
				return ErrReadOnlyRequired
			}
			identity.aliasOf, identityOK = values[3].(string)
			if !identityOK {
				_ = metadata.Close()
				return ErrReadOnlyRequired
			}
			identity.internal, identityOK = values[4].(bool)
			overloadCount, overloadCountOK := values[5].(int64)
			knownCount, knownCountOK := values[6].(int64)
			hasSideEffects, sideEffectsOK := values[7].(bool)
			if !identityOK || !overloadCountOK || !knownCountOK || !sideEffectsOK ||
				overloadCount == 0 || hasSideEffects {
				_ = metadata.Close()
				return ErrReadOnlyRequired
			}
			identities[identity] = struct{}{}
			if knownCount != overloadCount {
				if _, controlled := controlledNullFunctions[identity]; !controlled {
					_ = metadata.Close()
					return ErrReadOnlyRequired
				}
			}
		}
		if closeErr := metadata.Close(); closeErr != nil {
			return fmt.Errorf("%w: function metadata invalid", ErrReadOnlyRequired)
		}
		// The parser carries explicit qualification but does not bind an
		// unqualified call to a catalog object. Multiple identities therefore
		// represent ambiguity or shadowing and must fail closed.
		if len(identities) != 1 {
			return ErrReadOnlyRequired
		}
	}
	return nil
}

type astValue struct {
	value any
	kind  string
}

func collectFunctions(root any) map[functionReference]struct{} {
	names := make(map[functionReference]struct{})
	pending := []astValue{{value: root, kind: "expression"}}
	for len(pending) > 0 {
		item := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		switch typed := item.value.(type) {
		case map[string]any:
			if class, _ := typed["class"].(string); class == "FUNCTION" {
				if name, _ := typed["function_name"].(string); name != "" {
					catalog, _ := typed["catalog"].(string)
					schema, _ := typed["schema"].(string)
					names[functionReference{
						catalog: strings.ToLower(catalog),
						schema:  strings.ToLower(schema),
						name:    strings.ToLower(name),
						kind:    item.kind,
					}] = struct{}{}
				}
			}
			tableFunction, _ := typed["type"].(string)
			for key, child := range typed {
				kind := "expression"
				if tableFunction == "TABLE_FUNCTION" && key == "function" {
					kind = "table"
				}
				pending = append(pending, astValue{value: child, kind: kind})
			}
		case []any:
			for _, child := range typed {
				pending = append(pending, astValue{value: child, kind: "expression"})
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
