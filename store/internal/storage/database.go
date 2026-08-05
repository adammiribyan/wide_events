// Package storage owns the process's single DuckDB instance.
package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/adammiribyan/wide_events/store/internal/config"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// Database is the sole owner of the store's DuckDB instance.
type Database struct {
	sql         *sql.DB
	service     string
	environment string
}

// Open initializes the configured DuckDB instance, applies pending migrations,
// and locks its security-sensitive configuration before returning it.
func Open(ctx context.Context, cfg config.Config) (*Database, error) {
	databasePath, err := filepath.Abs(cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	tempDirectory := filepath.Join(filepath.Dir(databasePath), "tmp")
	if err := os.MkdirAll(tempDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("create DuckDB temp directory: %w", err)
	}

	maxTempSize := strconv.FormatInt(cfg.QueryTempBytes, 10) + "B"
	dsn := databasePath + "?storage_compatibility_version=v1.5.0"
	connector, err := duckdb.NewConnector(dsn, func(execer driver.ExecerContext) error {
		statements := []struct {
			query string
			args  []driver.NamedValue
		}{
			{query: "SET memory_limit = '1GiB'"},
			{query: "SET temp_directory = ?", args: []driver.NamedValue{{Ordinal: 1, Value: tempDirectory}}},
			{query: "SET max_temp_directory_size = ?", args: []driver.NamedValue{{Ordinal: 1, Value: maxTempSize}}},
			{query: "SET enable_external_access = false"},
			{query: "SET autoinstall_known_extensions = false"},
			{query: "SET autoload_known_extensions = false"},
			{query: "SET allow_community_extensions = false"},
			{query: "SET lock_configuration = true"},
		}
		for _, statement := range statements {
			if _, err := execer.ExecContext(ctx, statement.query, statement.args); err != nil {
				return fmt.Errorf("initialize DuckDB setting %q: %w", statement.query, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("open DuckDB connector: %w", err)
	}

	sqlDB := sql.OpenDB(connector)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	database := &Database{sql: sqlDB, service: cfg.Service, environment: cfg.Environment}
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("connect to DuckDB: %w", err)
	}
	if err := database.migrate(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return database, nil
}

// Close closes the store's sole DuckDB instance.
func (database *Database) Close() error {
	if err := database.sql.Close(); err != nil {
		return fmt.Errorf("close DuckDB: %w", err)
	}
	return nil
}
