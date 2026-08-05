package storage

import (
	"context"
	"database/sql"
	"fmt"
)

type migration struct {
	version     int
	description string
	statements  []string
}

var migrations = []migration{{
	version:     1,
	description: "initial storage schema",
	statements: []string{
		`CREATE TABLE wide_events (
			id UUID NOT NULL,
			occurred_at TIMESTAMPTZ NOT NULL,
			received_at TIMESTAMPTZ NOT NULL,
			service VARCHAR NOT NULL,
			environment VARCHAR NOT NULL,
			deployment VARCHAR,
			source VARCHAR,
			kind VARCHAR NOT NULL,
			duration_ms DOUBLE,
			route VARCHAR,
			job_class VARCHAR,
			status INTEGER,
			error BOOLEAN NOT NULL,
			request_id VARCHAR,
			attributes VARIANT NOT NULL
		)`,
		`CREATE TABLE wide_events_ingestion_batches (
			batch_id UUID PRIMARY KEY,
			committed_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE wide_events_loss_reports (
			loss_report_id UUID PRIMARY KEY,
			dropped_events UBIGINT NOT NULL,
			committed_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE wide_events_store_state (
			singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
			database_file_bytes BIGINT NOT NULL DEFAULT 0,
			free_disk_bytes BIGINT NOT NULL DEFAULT 0,
			queued_ingest_bytes BIGINT NOT NULL DEFAULT 0,
			rejected_batches UBIGINT NOT NULL DEFAULT 0,
			reported_dropped_events UBIGINT NOT NULL DEFAULT 0,
			tenancy_mismatches UBIGINT NOT NULL DEFAULT 0,
			write_ready BOOLEAN NOT NULL DEFAULT true,
			degradation_reason VARCHAR NOT NULL DEFAULT '',
			last_checkpoint_at TIMESTAMPTZ,
			last_retention_at TIMESTAMPTZ,
			last_backup_at TIMESTAMPTZ,
			last_maintenance_error VARCHAR,
			last_backup_error VARCHAR
		)`,
		`INSERT INTO wide_events_store_state (singleton) VALUES (true)`,
		`CREATE VIEW wide_events_store_status AS
		SELECT
			(SELECT count(*)::UBIGINT FROM wide_events WHERE received_at >= current_timestamp - INTERVAL '1 hour') AS rows_last_hour,
			(SELECT count(*)::UBIGINT FROM wide_events WHERE received_at >= current_timestamp - INTERVAL '1 day') AS rows_last_day,
			(SELECT min(received_at) FROM wide_events) AS oldest_received_at,
			(SELECT max(received_at) FROM wide_events) AS newest_received_at,
			state.database_file_bytes,
			state.free_disk_bytes,
			state.queued_ingest_bytes,
			state.rejected_batches,
			state.reported_dropped_events,
			state.tenancy_mismatches,
			(SELECT count(*)::UBIGINT FROM wide_events_ingestion_batches) AS batch_ledger_rows,
			(SELECT count(*)::UBIGINT FROM wide_events_loss_reports) AS loss_ledger_rows,
			state.write_ready,
			state.degradation_reason,
			state.last_checkpoint_at,
			state.last_retention_at,
			state.last_backup_at,
			state.last_maintenance_error,
			state.last_backup_error,
			'dev'::VARCHAR AS store_version,
			1::INTEGER AS protocol_version,
			version() AS duckdb_version
		FROM wide_events_store_state AS state
		WHERE state.singleton`,
	},
}}

func (database *Database) migrate(ctx context.Context) (err error) {
	tx, err := database.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema migration: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS wide_events_schema_migrations (
		version INTEGER PRIMARY KEY,
		description VARCHAR NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
	)`); err != nil {
		return fmt.Errorf("create schema migration ledger: %w", err)
	}

	var latest int
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(max(version), 0) FROM wide_events_schema_migrations").Scan(&latest); err != nil {
		return fmt.Errorf("read schema migration version: %w", err)
	}
	if latest > migrations[len(migrations)-1].version {
		return fmt.Errorf("database schema version %d is newer than supported version %d", latest, migrations[len(migrations)-1].version)
	}

	for _, migration := range migrations {
		var applied bool
		err = tx.QueryRowContext(ctx,
			"SELECT true FROM wide_events_schema_migrations WHERE version = ?",
			migration.version,
		).Scan(&applied)
		if err == nil {
			continue
		}
		if err != sql.ErrNoRows {
			return fmt.Errorf("check schema migration %d: %w", migration.version, err)
		}
		for _, statement := range migration.statements {
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply schema migration %d: %w", migration.version, err)
			}
		}
		if _, err = tx.ExecContext(ctx,
			"INSERT INTO wide_events_schema_migrations (version, description) VALUES (?, ?)",
			migration.version,
			migration.description,
		); err != nil {
			return fmt.Errorf("record schema migration %d: %w", migration.version, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit schema migrations: %w", err)
	}
	return nil
}
