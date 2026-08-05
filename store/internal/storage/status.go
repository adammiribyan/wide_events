package storage

import (
	"context"
	"database/sql"
	"fmt"
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
