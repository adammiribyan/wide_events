package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/disk"
	"github.com/adammiribyan/wide_events/store/internal/protocol"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommitBatchIsAtomicAndIdempotent(t *testing.T) {
	db := openTestDatabase(t)
	batch := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c01")

	first, err := db.CommitBatch(context.Background(), batch)
	require.NoError(t, err)
	second, err := db.CommitBatch(context.Background(), batch)
	require.NoError(t, err)

	assert.False(t, first.Duplicate)
	assert.True(t, second.Duplicate)
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events"))
}

func TestVariantQueryCorpus(t *testing.T) {
	db := openTestDatabase(t)
	batch := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c02")
	require.NoError(t, commit(t, db, batch))

	query, err := os.ReadFile(filepath.Join("testdata", "variant_queries.sql"))
	require.NoError(t, err)
	assert.Equal(t, "pdf", queryString(t, db, string(query)))
}

func TestOpenAppliesMigrationsOnceAndCreatesOneStatusRow(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(filepath.Join(dir, "wide-events.duckdb"))
	db, err := Open(context.Background(), cfg)
	require.NoError(t, err)

	assert.Equal(t, 2, queryInt(t, db, "SELECT count(*) FROM wide_events_schema_migrations"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_store_status"))
	require.NoError(t, db.Close())

	db, err = Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	assert.Equal(t, 2, queryInt(t, db, "SELECT count(*) FROM wide_events_schema_migrations"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_store_status"))
}

func TestOpenLocksSecurityConfiguration(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(context.Background(), testConfig(filepath.Join(dir, "wide-events.duckdb")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	assert.Equal(t, "false", queryString(t, db, "SELECT current_setting('enable_external_access')::VARCHAR"))
	assert.Equal(t, "false", queryString(t, db, "SELECT current_setting('autoinstall_known_extensions')::VARCHAR"))
	assert.Equal(t, "false", queryString(t, db, "SELECT current_setting('autoload_known_extensions')::VARCHAR"))
	assert.Equal(t, "false", queryString(t, db, "SELECT current_setting('allow_community_extensions')::VARCHAR"))
	assert.Equal(t, filepath.Join(dir, "tmp"), queryString(t, db, "SELECT current_setting('temp_directory')::VARCHAR"))
	assert.Equal(t, "v1.5.0", queryString(t, db, "SELECT current_setting('storage_compatibility_version')::VARCHAR"))
	assert.Equal(t, "v1.5.5", queryString(t, db, "SELECT version()"))

	_, err = db.sql.ExecContext(context.Background(), "SET enable_external_access = true")
	require.Error(t, err)
}

func TestOpenBindsQuerySpillToLesserOfFourGiBAndQuarterVolume(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wide-events.duckdb")
	db, err := Open(context.Background(), testConfig(path))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	space, err := (disk.StatFSProbe{}).Space(path)
	require.NoError(t, err)
	want := space.Total / 4
	if want > 4<<30 {
		want = 4 << 30
	}
	setting := queryString(t, db, "SELECT current_setting('max_temp_directory_size')::VARCHAR")
	assert.Equal(t, want, parseDuckDBBytes(t, setting))
}

func parseDuckDBBytes(t *testing.T, value string) int64 {
	t.Helper()
	parts := strings.Fields(value)
	require.Len(t, parts, 2)
	number, err := strconv.ParseFloat(parts[0], 64)
	require.NoError(t, err)
	multiplier := map[string]float64{"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}[parts[1]]
	require.NotZero(t, multiplier)
	return int64(number * multiplier)
}

func TestEveryPooledConnectionUsesLockedSecurityConfiguration(t *testing.T) {
	db := openTestDatabase(t)
	first, err := db.QueryConnection(context.Background())
	require.NoError(t, err)
	defer first.Close()
	second, err := db.QueryConnection(context.Background())
	require.NoError(t, err)
	defer second.Close()

	for _, connection := range []*sql.Conn{first, second} {
		var externalAccess string
		require.NoError(t, connection.QueryRowContext(context.Background(),
			"SELECT current_setting('enable_external_access')::VARCHAR").Scan(&externalAccess))
		assert.Equal(t, "false", externalAccess)
		_, err := connection.ExecContext(context.Background(), "SET enable_external_access = true")
		require.Error(t, err)
	}
}

func TestCommitBatchRollsBackLedgerAndEventsOnFailure(t *testing.T) {
	db := openTestDatabase(t)
	batch := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c03")
	batch.Events[0].Attributes["invalid"] = func() {}

	_, err := db.CommitBatch(context.Background(), batch)
	require.Error(t, err)
	assert.Equal(t, 0, queryInt(t, db, "SELECT count(*) FROM wide_events"))
	assert.Equal(t, 0, queryInt(t, db, "SELECT count(*) FROM wide_events_ingestion_batches"))

	delete(batch.Events[0].Attributes, "invalid")
	result, err := db.CommitBatch(context.Background(), batch)
	require.NoError(t, err)
	assert.False(t, result.Duplicate)
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events"))
}

func TestCommitBatchCountsTenancyMismatchTransactionallyAcrossFailureAndRetry(t *testing.T) {
	db := openTestDatabase(t)
	batch := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c0a")
	batch.Events[0].Attributes["invalid"] = func() {}

	failed, err := db.CommitBatch(context.Background(), batch)
	require.Error(t, err)
	assert.False(t, failed.TenancyMismatch)
	status, statusErr := db.Status(context.Background())
	require.NoError(t, statusErr)
	assert.Zero(t, status.TenancyMismatches)

	delete(batch.Events[0].Attributes, "invalid")
	first, err := db.CommitBatch(context.Background(), batch)
	require.NoError(t, err)
	second, err := db.CommitBatch(context.Background(), batch)
	require.NoError(t, err)
	assert.True(t, first.TenancyMismatch)
	assert.False(t, first.Duplicate)
	assert.True(t, second.TenancyMismatch)
	assert.True(t, second.Duplicate)
	status, err = db.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), status.TenancyMismatches)
}

func TestCommitBatchPromotesColumnsAndOverridesPayloadTenancy(t *testing.T) {
	db := openTestDatabase(t)
	batch := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c04")
	batch.Events[0].DurationMS = nil
	batch.Events[0].Attributes = map[string]any{
		"http.route.controller":     "OrdersController#create",
		"job.class":                 "ReceiptJob",
		"http.response.status_code": float64(503),
		"error":                     true,
		"http.request.id":           "request-123",
	}
	require.NoError(t, commit(t, db, batch))

	var service, environment, route, jobClass, requestID string
	var status sql.NullInt64
	var isError bool
	var duration sql.NullFloat64
	require.NoError(t, db.sql.QueryRowContext(context.Background(), `
		SELECT service, environment, route, job_class, status, error, request_id, duration_ms
		FROM wide_events
	`).Scan(&service, &environment, &route, &jobClass, &status, &isError, &requestID, &duration))
	assert.Equal(t, "checkout", service)
	assert.Equal(t, "test", environment)
	assert.Equal(t, "OrdersController#create", route)
	assert.Equal(t, "ReceiptJob", jobClass)
	assert.Equal(t, int64(503), status.Int64)
	assert.True(t, status.Valid)
	assert.True(t, isError)
	assert.Equal(t, "request-123", requestID)
	assert.False(t, duration.Valid)
}

func TestCommitBatchDeduplicatesRecarriedLossReport(t *testing.T) {
	db := openTestDatabase(t)
	lossReportID := "018f6f50-2f9d-7fb0-a258-6d9f95f20e01"
	first := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c05")
	first.DroppedEventsSinceLastBatch = 9
	first.LossReportID = &lossReportID
	require.NoError(t, commit(t, db, first))

	second := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c06")
	second.Events[0].ID = "018f6f50-2f9d-7fb0-a258-6d9f95f20d02"
	second.DroppedEventsSinceLastBatch = 9
	second.LossReportID = &lossReportID
	require.NoError(t, commit(t, db, second))

	assert.Equal(t, 2, queryInt(t, db, "SELECT count(*) FROM wide_events"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_loss_reports"))
	assert.Equal(t, 9, queryInt(t, db, "SELECT reported_dropped_events FROM wide_events_store_status"))
}

func TestPruneLedgerUsesExclusiveSevenDayBoundary(t *testing.T) {
	db := openTestDatabase(t)
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	boundary := now.Add(-7 * 24 * time.Hour)
	old := boundary.Add(-time.Nanosecond)

	for _, row := range []struct {
		batchID, lossID string
		committedAt     time.Time
	}{
		{"018f6f50-2f9d-7fb0-a258-6d9f95f20b01", "018f6f50-2f9d-7fb0-a258-6d9f95f20a01", old},
		{"018f6f50-2f9d-7fb0-a258-6d9f95f20b02", "018f6f50-2f9d-7fb0-a258-6d9f95f20a02", boundary},
	} {
		_, err := db.sql.ExecContext(context.Background(), "INSERT INTO wide_events_ingestion_batches VALUES (?, ?)", row.batchID, row.committedAt)
		require.NoError(t, err)
		_, err = db.sql.ExecContext(context.Background(), "INSERT INTO wide_events_loss_reports VALUES (?, 1, ?)", row.lossID, row.committedAt)
		require.NoError(t, err)
	}

	require.NoError(t, db.PruneLedger(context.Background(), now))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_ingestion_batches"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_loss_reports"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_ingestion_batches WHERE committed_at = ?", boundary))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_loss_reports WHERE committed_at = ?", boundary))
	status, err := db.Status(context.Background())
	require.NoError(t, err)
	require.NotNil(t, status.LastRetentionAt)
	assert.Equal(t, now, *status.LastRetentionAt)
}

func TestStatusAndCheckpointExposeOneRowOperationalState(t *testing.T) {
	db := openTestDatabase(t)
	batch := validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c07")
	lossReportID := "018f6f50-2f9d-7fb0-a258-6d9f95f20e02"
	batch.DroppedEventsSinceLastBatch = 4
	batch.LossReportID = &lossReportID
	require.NoError(t, commit(t, db, batch))
	require.NoError(t, db.Checkpoint(context.Background()))

	status, err := db.Status(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), status.RowsLastHour)
	assert.Equal(t, uint64(1), status.RowsLastDay)
	assert.NotNil(t, status.OldestReceivedAt)
	assert.NotNil(t, status.NewestReceivedAt)
	assert.Equal(t, uint64(1), status.BatchLedgerRows)
	assert.Equal(t, uint64(1), status.LossLedgerRows)
	assert.Equal(t, uint64(4), status.ReportedDroppedEvents)
	assert.True(t, status.WriteReady)
	assert.Empty(t, status.DegradationReason)
	assert.NotNil(t, status.LastCheckpointAt)
	assert.Equal(t, protocol.ProtocolVersion, status.ProtocolVersion)
	assert.Equal(t, "v1.5.5", status.DuckDBVersion)
	assert.Equal(t, "dev", status.StoreVersion)
}

func TestOperationalCountersAndWriteStateAppearInStatus(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()

	require.NoError(t, db.UpdateCapacity(ctx, 1_000_000, 750_000))
	require.NoError(t, db.SetQueuedIngestBytes(ctx, 4096))
	require.NoError(t, db.AddRejectedBatches(ctx, 3))
	require.NoError(t, db.SetWriteReady(ctx, false, "disk_space_low"))

	status, err := db.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000), status.DatabaseFileBytes)
	assert.Equal(t, int64(750_000), status.FreeDiskBytes)
	assert.Equal(t, int64(4096), status.QueuedIngestBytes)
	assert.Equal(t, uint64(3), status.RejectedBatches)
	assert.False(t, status.WriteReady)
	assert.Equal(t, "disk_space_low", status.DegradationReason)
}

func TestOperationalMaintenanceResultsAppearInStatus(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()
	retentionAt := time.Date(2026, 8, 5, 1, 2, 3, 0, time.UTC)
	backupAt := time.Date(2026, 8, 5, 4, 5, 6, 0, time.UTC)

	require.NoError(t, db.RecordRetention(ctx, retentionAt, errors.New("retention failed")))
	require.NoError(t, db.RecordBackup(ctx, backupAt, errors.New("backup failed")))
	failed, err := db.Status(ctx)
	require.NoError(t, err)
	assert.Nil(t, failed.LastRetentionAt)
	assert.Nil(t, failed.LastBackupAt)
	require.NotNil(t, failed.LastMaintenanceError)
	require.NotNil(t, failed.LastBackupError)
	assert.Equal(t, "retention failed", *failed.LastMaintenanceError)
	assert.Equal(t, "backup failed", *failed.LastBackupError)

	require.NoError(t, db.RecordRetention(ctx, retentionAt, nil))
	require.NoError(t, db.RecordBackup(ctx, backupAt, nil))
	succeeded, err := db.Status(ctx)
	require.NoError(t, err)
	require.NotNil(t, succeeded.LastRetentionAt)
	require.NotNil(t, succeeded.LastBackupAt)
	assert.Equal(t, retentionAt, *succeeded.LastRetentionAt)
	assert.Equal(t, backupAt, *succeeded.LastBackupAt)
	assert.Nil(t, succeeded.LastMaintenanceError)
	assert.Nil(t, succeeded.LastBackupError)
}

func TestStoreVersionCanBeInjectedIntoStatusRelation(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()

	_, err := db.sql.ExecContext(ctx, "UPDATE wide_events_store_state SET store_version = NULL WHERE singleton")
	require.Error(t, err)
	require.NoError(t, db.SetStoreVersion(ctx, "2.3.4"))
	status, err := db.Status(ctx)
	require.NoError(t, err)
	assert.Equal(t, "2.3.4", status.StoreVersion)
	assert.Equal(t, "2.3.4", queryString(t, db, "SELECT store_version FROM wide_events_store_status"))
}

func TestRetainDeletesEventsByReceivedAtAndBothLedgersAtExactlySevenDays(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	for _, receivedAt := range []time.Time{now.Add(-31 * 24 * time.Hour), now.Add(-30 * 24 * time.Hour)} {
		_, err := db.sql.ExecContext(ctx, `INSERT INTO wide_events (
			id, occurred_at, received_at, service, environment, kind, error, attributes
		) VALUES (uuid(), ?, ?, 'billing', 'production', 'request', false, json('{}')::VARIANT)`, receivedAt, receivedAt)
		require.NoError(t, err)
	}
	for _, committedAt := range []time.Time{now.Add(-8 * 24 * time.Hour), now.Add(-7 * 24 * time.Hour)} {
		_, err := db.sql.ExecContext(ctx, "INSERT INTO wide_events_ingestion_batches VALUES (uuid(), ?)", committedAt)
		require.NoError(t, err)
		_, err = db.sql.ExecContext(ctx, "INSERT INTO wide_events_loss_reports VALUES (uuid(), 1, ?)", committedAt)
		require.NoError(t, err)
	}

	require.NoError(t, db.Retain(ctx, now.Add(-30*24*time.Hour), now.Add(-7*24*time.Hour)))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_ingestion_batches"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_loss_reports"))
}

func TestCreateBackupProducesAReadOnlyVerifiableCopy(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()
	require.NoError(t, commit(t, db, validBatch("01989a64-d4c2-7720-b228-b56053627101")))
	directory := filepath.Join(filepath.Dir(db.path), "backups")
	require.NoError(t, os.Mkdir(directory, 0o700))
	path := filepath.Join(directory, "backup.duckdb")
	require.NoError(t, db.CreateBackup(ctx, path))
	require.FileExists(t, path)
	require.NoError(t, db.VerifyBackup(ctx, path))
}

func TestDuckDBHeldDescriptorPathSupportIsCharacterized(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()
	directory := filepath.Join(filepath.Dir(db.path), "backups")
	require.NoError(t, os.Mkdir(directory, 0o700))
	path := filepath.Join(directory, "descriptor.duckdb")
	require.NoError(t, db.CreateBackup(ctx, path))
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	fdPath := fmt.Sprintf("/dev/fd/%d", file.Fd())
	if runtime.GOOS == "linux" {
		fdPath = fmt.Sprintf("/proc/self/fd/%d", file.Fd())
	}
	connector, err := duckdb.NewConnector(fdPath+"?access_mode=read_only", func(driver.ExecerContext) error { return nil })
	if runtime.GOOS == "darwin" && err != nil {
		require.ErrorContains(t, err, `Cannot open database "/dev/fd/descriptor.duckdb" in read-only mode`)
		return
	}
	require.NoError(t, err)
	copyDatabase := sql.OpenDB(connector)
	copyDatabase.SetMaxOpenConns(1)
	defer copyDatabase.Close()
	pingErr := copyDatabase.PingContext(ctx)
	if runtime.GOOS == "darwin" {
		require.ErrorContains(t, pingErr, `Cannot open database "/dev/fd/descriptor.duckdb" in read-only mode`)
		return
	}
	require.NoError(t, pingErr)
	var rows uint64
	require.NoError(t, copyDatabase.QueryRowContext(ctx, "SELECT count(*)::UBIGINT FROM wide_events").Scan(&rows))
}

func TestVerifyBackupFileUsesTheHeldDescriptorStrategyForThisOS(t *testing.T) {
	db := openTestDatabase(t)
	ctx := context.Background()
	directory := filepath.Join(filepath.Dir(db.path), "backups")
	require.NoError(t, os.Mkdir(directory, 0o700))
	path := filepath.Join(directory, "held.duckdb")
	require.NoError(t, db.CreateBackup(ctx, path))
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	require.NoError(t, db.VerifyBackupFile(ctx, path, file))
}

func TestVerifyBackupRejectsAnInvalidCopy(t *testing.T) {
	db := openTestDatabase(t)
	path := filepath.Join(t.TempDir(), "invalid.duckdb")
	require.NoError(t, os.WriteFile(path, []byte("not duckdb"), 0o600))
	assert.Error(t, db.VerifyBackup(context.Background(), path))
}

func TestCommittedBatchSurvivesCloseAndReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(filepath.Join(dir, "wide-events.duckdb"))
	db, err := Open(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, commit(t, db, validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c08")))
	require.NoError(t, db.Close())

	db, err = Open(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_ingestion_batches"))
}

func TestCommittedBatchRecoversFromUncleanProcessWAL(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "wide-events.duckdb")
	command := exec.Command(os.Args[0], "-test.run=^TestUncleanWALWriterProcess$")
	command.Env = append(os.Environ(), "WIDE_EVENTS_TEST_WAL_DATABASE="+databasePath)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))

	walInfo, err := os.Stat(databasePath + ".wal")
	require.NoError(t, err)
	assert.Positive(t, walInfo.Size())

	db, err := Open(context.Background(), testConfig(databasePath))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events"))
	assert.Equal(t, 1, queryInt(t, db, "SELECT count(*) FROM wide_events_ingestion_batches"))
	assert.Equal(t, 2, queryInt(t, db, "SELECT count(*) FROM wide_events_schema_migrations"))
}

func TestUncleanWALWriterProcess(t *testing.T) {
	databasePath := os.Getenv("WIDE_EVENTS_TEST_WAL_DATABASE")
	if databasePath == "" {
		t.Skip("subprocess helper")
	}

	db, err := Open(context.Background(), testConfig(databasePath))
	require.NoError(t, err)
	_, err = db.CommitBatch(context.Background(), validBatch("018f6f50-2f9d-7fb0-a258-6d9f95f20c09"))
	require.NoError(t, err)
	os.Exit(0)
}

func openTestDatabase(t *testing.T) *Database {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(context.Background(), testConfig(filepath.Join(dir, "wide-events.duckdb")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func testConfig(database string) config.Config {
	return config.Config{
		Database:       database,
		Service:        "checkout",
		Environment:    "test",
		QueryTempBytes: 64 << 20,
	}
}

func validBatch(batchID string) protocol.Batch {
	duration := 12.5
	return protocol.Batch{
		ProtocolVersion: protocol.ProtocolVersion,
		BatchID:         batchID,
		Service:         "payload-service",
		Environment:     "payload-environment",
		Events: []protocol.Event{{
			ID:         "018f6f50-2f9d-7fb0-a258-6d9f95f20d01",
			OccurredAt: time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC),
			Kind:       "request",
			DurationMS: &duration,
			Attributes: map[string]any{"report.format": "pdf"},
		}},
	}
}

func commit(t *testing.T, db *Database, batch protocol.Batch) error {
	t.Helper()
	_, err := db.CommitBatch(context.Background(), batch)
	return err
}

func queryInt(t *testing.T, db *Database, query string, args ...any) int {
	t.Helper()
	var got int
	require.NoError(t, db.sql.QueryRowContext(context.Background(), query, args...).Scan(&got))
	return got
}

func queryString(t *testing.T, db *Database, query string, args ...any) string {
	t.Helper()
	var got string
	require.NoError(t, db.sql.QueryRowContext(context.Background(), query, args...).Scan(&got))
	return got
}
