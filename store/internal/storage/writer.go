package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/adammiribyan/wide_events/store/internal/protocol"
)

// CommitResult reports whether a batch was already committed.
type CommitResult struct {
	Duplicate       bool
	TenancyMismatch bool
}

// CommitBatch atomically records a batch ledger entry, its loss report, and all
// events. A previously committed batch is acknowledged without reinsertion.
func (database *Database) CommitBatch(ctx context.Context, batch protocol.Batch) (result CommitResult, err error) {
	tenancyMismatch := batch.Service != database.service || batch.Environment != database.environment
	tx, err := database.sql.BeginTx(ctx, nil)
	if err != nil {
		return CommitResult{}, fmt.Errorf("begin batch transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var insertedBatchID string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO wide_events_ingestion_batches (batch_id, committed_at)
		VALUES (?, current_timestamp)
		ON CONFLICT DO NOTHING
		RETURNING batch_id::VARCHAR
	`, batch.BatchID).Scan(&insertedBatchID)
	if err == sql.ErrNoRows {
		if err = tx.Commit(); err != nil {
			return CommitResult{}, fmt.Errorf("commit duplicate batch transaction: %w", err)
		}
		return CommitResult{Duplicate: true, TenancyMismatch: tenancyMismatch}, nil
	}
	if err != nil {
		return CommitResult{}, fmt.Errorf("record ingestion batch: %w", err)
	}

	if batch.DroppedEventsSinceLastBatch > 0 && batch.LossReportID != nil {
		if err = recordLossReport(ctx, tx, *batch.LossReportID, batch.DroppedEventsSinceLastBatch); err != nil {
			return CommitResult{}, err
		}
	}

	for _, event := range batch.Events {
		attributes, marshalErr := json.Marshal(event.Attributes)
		if marshalErr != nil {
			return CommitResult{}, fmt.Errorf("marshal event attributes: %w", marshalErr)
		}
		if _, err = tx.ExecContext(ctx, `
			INSERT INTO wide_events (
				id, occurred_at, received_at, service, environment,
				deployment, source, kind, duration_ms, route, job_class,
				status, error, request_id, attributes
			) VALUES (
				?, ?, current_timestamp, ?, ?,
				?, ?, ?, ?, ?, ?,
				?, ?, ?, json(?::VARCHAR)::VARIANT
			)
		`,
			event.ID,
			event.OccurredAt,
			database.service,
			database.environment,
			event.Deployment,
			event.Source,
			event.Kind,
			event.DurationMS,
			stringAttr(event.Attributes, "http.route.controller"),
			stringAttr(event.Attributes, "job.class"),
			intAttr(event.Attributes, "http.response.status_code"),
			boolAttr(event.Attributes, "error", false),
			stringAttr(event.Attributes, "http.request.id"),
			string(attributes),
		); err != nil {
			return CommitResult{}, fmt.Errorf("insert event: %w", err)
		}
	}
	if tenancyMismatch {
		if _, err = tx.ExecContext(ctx, `
			UPDATE wide_events_store_state
			SET tenancy_mismatches = tenancy_mismatches + 1
			WHERE singleton
		`); err != nil {
			return CommitResult{}, fmt.Errorf("update tenancy mismatch total: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return CommitResult{}, fmt.Errorf("commit batch transaction: %w", err)
	}
	return CommitResult{TenancyMismatch: tenancyMismatch}, nil
}

func recordLossReport(ctx context.Context, tx *sql.Tx, lossReportID string, dropped uint64) error {
	var insertedLossReportID string
	err := tx.QueryRowContext(ctx, `
		INSERT INTO wide_events_loss_reports (loss_report_id, dropped_events, committed_at)
		VALUES (?, ?::UBIGINT, current_timestamp)
		ON CONFLICT DO NOTHING
		RETURNING loss_report_id::VARCHAR
	`, lossReportID, strconv.FormatUint(dropped, 10)).Scan(&insertedLossReportID)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("record loss report: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE wide_events_store_state
		SET reported_dropped_events = reported_dropped_events + ?::UBIGINT
		WHERE singleton
	`, strconv.FormatUint(dropped, 10)); err != nil {
		return fmt.Errorf("update reported drop total: %w", err)
	}
	return nil
}

func stringAttr(attributes map[string]any, key string) any {
	value, ok := attributes[key].(string)
	if !ok {
		return nil
	}
	return value
}

func intAttr(attributes map[string]any, key string) any {
	value, ok := attributes[key].(float64)
	if !ok || math.Trunc(value) != value || value < math.MinInt32 || value > math.MaxInt32 {
		return nil
	}
	return int32(value)
}

func boolAttr(attributes map[string]any, key string, fallback bool) bool {
	value, ok := attributes[key].(bool)
	if !ok {
		return fallback
	}
	return value
}
