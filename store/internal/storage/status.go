package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// Status is the typed representation of wide_events_store_status.
type Status struct {
	RowsLastHour          uint64
	RowsLastDay           uint64
	OldestReceivedAt      *time.Time
	NewestReceivedAt      *time.Time
	DatabaseFileBytes     int64
	FreeDiskBytes         int64
	QueuedIngestBytes     int64
	RejectedBatches       uint64
	ReportedDroppedEvents uint64
	TenancyMismatches     uint64
	BatchLedgerRows       uint64
	LossLedgerRows        uint64
	WriteReady            bool
	DegradationReason     string
	LastCheckpointAt      *time.Time
	LastRetentionAt       *time.Time
	LastBackupAt          *time.Time
	LastMaintenanceError  *string
	LastBackupError       *string
	StoreVersion          string
	ProtocolVersion       int
	DuckDBVersion         string
}

// UpdateCapacity publishes the latest size of the database file and free bytes
// on its filesystem without exposing the underlying SQL handle.
func (database *Database) UpdateCapacity(ctx context.Context, databaseFileBytes, freeDiskBytes int64) error {
	if databaseFileBytes < 0 || freeDiskBytes < 0 {
		return fmt.Errorf("capacity bytes must be non-negative")
	}
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET database_file_bytes = ?, free_disk_bytes = ?
		WHERE singleton
	`, databaseFileBytes, freeDiskBytes); err != nil {
		return fmt.Errorf("update store capacity: %w", err)
	}
	return nil
}

// SetQueuedIngestBytes publishes the current canonical-byte queue occupancy.
func (database *Database) SetQueuedIngestBytes(ctx context.Context, queuedBytes int64) error {
	if queuedBytes < 0 {
		return fmt.Errorf("queued ingest bytes must be non-negative")
	}
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state SET queued_ingest_bytes = ? WHERE singleton
	`, queuedBytes); err != nil {
		return fmt.Errorf("update queued ingest bytes: %w", err)
	}
	return nil
}

// AddRejectedBatches adds newly rejected admissions to the lifetime counter.
func (database *Database) AddRejectedBatches(ctx context.Context, count uint64) error {
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET rejected_batches = rejected_batches + ?::UBIGINT
		WHERE singleton
	`, strconv.FormatUint(count, 10)); err != nil {
		return fmt.Errorf("update rejected batch total: %w", err)
	}
	return nil
}

// AddTenancyMismatches adds newly observed client/store tenancy drift to the
// lifetime counter without exposing the store's SQL handle.
func (database *Database) AddTenancyMismatches(ctx context.Context, count uint64) error {
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET tenancy_mismatches = tenancy_mismatches + ?::UBIGINT
		WHERE singleton
	`, strconv.FormatUint(count, 10)); err != nil {
		return fmt.Errorf("update tenancy mismatch total: %w", err)
	}
	return nil
}

// SetWriteReady publishes whether writes may be admitted and why not.
func (database *Database) SetWriteReady(ctx context.Context, ready bool, reason string) error {
	if ready {
		reason = ""
	}
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET write_ready = ?, degradation_reason = ?
		WHERE singleton
	`, ready, reason); err != nil {
		return fmt.Errorf("update write readiness: %w", err)
	}
	return nil
}

// WriteStatus reads only the one-row admission state. It avoids the aggregate
// operational view on request and routing-health hot paths.
func (database *Database) WriteStatus(ctx context.Context) (ready bool, reason string, err error) {
	if err := database.sql.QueryRowContext(ctx, `
		SELECT write_ready, degradation_reason
		FROM wide_events_store_state
		WHERE singleton
	`).Scan(&ready, &reason); err != nil {
		return false, "", fmt.Errorf("read write status: %w", err)
	}
	return ready, reason, nil
}

// RecordRetention publishes a retention result. A successful result advances
// the timestamp and clears the previous maintenance error; a failed result
// preserves the previous successful timestamp and records the error.
func (database *Database) RecordRetention(ctx context.Context, completedAt time.Time, result error) error {
	if result != nil {
		if _, err := database.sql.ExecContext(ctx, `
			UPDATE wide_events_store_state SET last_maintenance_error = ? WHERE singleton
		`, result.Error()); err != nil {
			return fmt.Errorf("record retention error: %w", err)
		}
		return nil
	}
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET last_retention_at = ?, last_maintenance_error = NULL
		WHERE singleton
	`, completedAt); err != nil {
		return fmt.Errorf("record retention success: %w", err)
	}
	return nil
}

// RecordBackup publishes a backup result with the same success/error semantics
// as RecordRetention, using the backup-specific status fields.
func (database *Database) RecordBackup(ctx context.Context, completedAt time.Time, result error) error {
	if result != nil {
		if _, err := database.sql.ExecContext(ctx, `
			UPDATE wide_events_store_state SET last_backup_error = ? WHERE singleton
		`, result.Error()); err != nil {
			return fmt.Errorf("record backup error: %w", err)
		}
		return nil
	}
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET last_backup_at = ?, last_backup_error = NULL
		WHERE singleton
	`, completedAt); err != nil {
		return fmt.Errorf("record backup success: %w", err)
	}
	return nil
}

// SetStoreVersion publishes the semantic version injected by the executable.
func (database *Database) SetStoreVersion(ctx context.Context, version string) error {
	if version == "" {
		return fmt.Errorf("store version must not be empty")
	}
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state SET store_version = ? WHERE singleton
	`, version); err != nil {
		return fmt.Errorf("update store version: %w", err)
	}
	return nil
}

// PruneLedger removes only batch and loss-report ledger entries strictly older
// than seven days. Entries exactly on the boundary remain deduplication keys.
func (database *Database) PruneLedger(ctx context.Context, now time.Time) (err error) {
	tx, err := database.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ledger pruning: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	for _, table := range []string{"wide_events_ingestion_batches", "wide_events_loss_reports"} {
		query := "DELETE FROM " + table + " WHERE committed_at < ?::TIMESTAMPTZ - INTERVAL '7 days'"
		if _, err = tx.ExecContext(ctx, query, now); err != nil {
			return fmt.Errorf("prune %s: %w", table, err)
		}
	}
	if _, err = tx.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET last_retention_at = ?, last_maintenance_error = NULL
		WHERE singleton
	`, now); err != nil {
		return fmt.Errorf("record ledger pruning status: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit ledger pruning: %w", err)
	}
	return nil
}

// Checkpoint checkpoints the database and publishes the result in store state.
func (database *Database) Checkpoint(ctx context.Context) error {
	if _, err := database.sql.ExecContext(ctx, "CHECKPOINT"); err != nil {
		_, _ = database.sql.ExecContext(context.Background(), `
			UPDATE wide_events_store_state
			SET last_maintenance_error = ?
			WHERE singleton
		`, err.Error())
		return fmt.Errorf("checkpoint DuckDB: %w", err)
	}
	if _, err := database.sql.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET last_checkpoint_at = current_timestamp,
			last_maintenance_error = NULL
		WHERE singleton
	`); err != nil {
		return fmt.Errorf("record checkpoint status: %w", err)
	}
	return nil
}

// Status returns the store's single operational status row.
func (database *Database) Status(ctx context.Context) (Status, error) {
	var status Status
	var oldestReceivedAt, newestReceivedAt sql.NullTime
	var lastCheckpointAt, lastRetentionAt, lastBackupAt sql.NullTime
	var lastMaintenanceError, lastBackupError sql.NullString
	err := database.sql.QueryRowContext(ctx, `
		SELECT
			rows_last_hour, rows_last_day, oldest_received_at, newest_received_at,
			database_file_bytes, free_disk_bytes, queued_ingest_bytes,
			rejected_batches, reported_dropped_events, tenancy_mismatches,
			batch_ledger_rows, loss_ledger_rows, write_ready, degradation_reason,
			last_checkpoint_at, last_retention_at, last_backup_at,
			last_maintenance_error, last_backup_error,
			store_version, protocol_version, duckdb_version
		FROM wide_events_store_status
	`).Scan(
		&status.RowsLastHour,
		&status.RowsLastDay,
		&oldestReceivedAt,
		&newestReceivedAt,
		&status.DatabaseFileBytes,
		&status.FreeDiskBytes,
		&status.QueuedIngestBytes,
		&status.RejectedBatches,
		&status.ReportedDroppedEvents,
		&status.TenancyMismatches,
		&status.BatchLedgerRows,
		&status.LossLedgerRows,
		&status.WriteReady,
		&status.DegradationReason,
		&lastCheckpointAt,
		&lastRetentionAt,
		&lastBackupAt,
		&lastMaintenanceError,
		&lastBackupError,
		&status.StoreVersion,
		&status.ProtocolVersion,
		&status.DuckDBVersion,
	)
	if err != nil {
		return Status{}, fmt.Errorf("read store status: %w", err)
	}
	status.OldestReceivedAt = timePointer(oldestReceivedAt)
	status.NewestReceivedAt = timePointer(newestReceivedAt)
	status.LastCheckpointAt = timePointer(lastCheckpointAt)
	status.LastRetentionAt = timePointer(lastRetentionAt)
	status.LastBackupAt = timePointer(lastBackupAt)
	status.LastMaintenanceError = stringPointer(lastMaintenanceError)
	status.LastBackupError = stringPointer(lastBackupError)
	return status, nil
}

func timePointer(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	return &value.Time
}

func stringPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}
