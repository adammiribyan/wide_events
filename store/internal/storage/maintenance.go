package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// Retain atomically deletes expired events and both retry ledgers. Boundary
// rows are preserved by the strict less-than predicates.
func (database *Database) Retain(ctx context.Context, eventCutoff, ledgerCutoff time.Time) (err error) {
	tx, err := database.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin retention: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, "DELETE FROM wide_events WHERE received_at < ?", eventCutoff); err != nil {
		return fmt.Errorf("delete expired events: %w", err)
	}
	for _, table := range []string{"wide_events_ingestion_batches", "wide_events_loss_reports"} {
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE committed_at < ?", ledgerCutoff); err != nil {
			return fmt.Errorf("delete expired %s rows: %w", table, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit retention: %w", err)
	}
	return nil
}

// CreateBackup copies the owned catalog to a validated, previously
// non-existent DuckDB file without exposing the database handle.
func (database *Database) CreateBackup(ctx context.Context, path string) (err error) {
	path, err = validateBackupTarget(path)
	if err != nil {
		return err
	}
	var catalog string
	if err := database.sql.QueryRowContext(ctx, "SELECT current_database()").Scan(&catalog); err != nil {
		return fmt.Errorf("read main catalog name: %w", err)
	}
	attached := false
	defer func() {
		if attached {
			detachContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, detachErr := database.sql.ExecContext(detachContext, "DETACH wide_events_backup")
			if err == nil && detachErr != nil {
				err = fmt.Errorf("detach backup catalog: %w", detachErr)
			}
		}
	}()
	attach := "ATTACH " + quoteSQLString(path) + " AS wide_events_backup"
	if _, err = database.sql.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("attach backup catalog: %w", err)
	}
	attached = true
	copyStatement := "COPY FROM DATABASE " + quoteIdentifier(catalog) + " TO wide_events_backup"
	if _, err = database.sql.ExecContext(ctx, copyStatement); err != nil {
		return fmt.Errorf("copy database to backup: %w", err)
	}
	if _, err = database.sql.ExecContext(ctx, "CHECKPOINT wide_events_backup"); err != nil {
		return fmt.Errorf("checkpoint backup catalog: %w", err)
	}
	return nil
}

// VerifyBackup opens a completed copy read-only in this process and verifies
// both its schema ledger and event relation are queryable.
func (database *Database) VerifyBackup(ctx context.Context, path string) error {
	path, err := validateExistingBackup(path)
	if err != nil {
		return err
	}
	return database.verifyBackupContents(ctx, path)
}

// VerifyBackupFile verifies the held backup object. Linux can bind DuckDB to
// the descriptor through /proc; Darwin falls back to the validated pathname
// because DuckDB 1.5.5 rewrites /dev/fd/N to /dev/fd/<backup-basename> and
// fails to open it. Callers retain the descriptor and check identity before
// and after this call under the trusted-store-UID threat model.
func (database *Database) VerifyBackupFile(ctx context.Context, path string, file *os.File) error {
	path, err := validateExistingBackup(path)
	if err != nil {
		return err
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect backup path identity: %w", err)
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect backup descriptor identity: %w", err)
	}
	if !os.SameFile(pathInfo, fileInfo) {
		return fmt.Errorf("backup descriptor does not identify the validated path")
	}
	return database.verifyBackupContents(ctx, backupDescriptorPath(path, file.Fd()))
}

func (database *Database) verifyBackupContents(ctx context.Context, path string) error {
	connector, err := duckdb.NewConnector(path+"?access_mode=read_only", func(driver.ExecerContext) error { return nil })
	if err != nil {
		return fmt.Errorf("open backup connector: %w", err)
	}
	copyDatabase := sql.OpenDB(connector)
	copyDatabase.SetMaxOpenConns(1)
	defer copyDatabase.Close()
	if err := copyDatabase.PingContext(ctx); err != nil {
		return fmt.Errorf("open backup read-only: %w", err)
	}
	var schemaVersion int
	if err := copyDatabase.QueryRowContext(ctx, "SELECT coalesce(max(version), 0) FROM wide_events_schema_migrations").Scan(&schemaVersion); err != nil {
		return fmt.Errorf("verify backup schema: %w", err)
	}
	wantVersion := migrations[len(migrations)-1].version
	if schemaVersion != wantVersion {
		return fmt.Errorf("verify backup schema: got version %d, want %d", schemaVersion, wantVersion)
	}
	var rows uint64
	if err := copyDatabase.QueryRowContext(ctx, "SELECT count(*)::UBIGINT FROM wide_events").Scan(&rows); err != nil {
		return fmt.Errorf("verify backup events: %w", err)
	}
	return nil
}

func validateBackupTarget(path string) (string, error) {
	resolved, err := filepath.Abs(path)
	if err != nil || resolved != filepath.Clean(resolved) {
		return "", fmt.Errorf("resolve backup target: %w", err)
	}
	parentInfo, err := os.Lstat(filepath.Dir(resolved))
	if err != nil {
		return "", fmt.Errorf("inspect backup parent: %w", err)
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("backup parent must be a real directory")
	}
	if _, err := os.Lstat(resolved); err == nil {
		return "", fmt.Errorf("backup target already exists")
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("inspect backup target: %w", err)
	}
	return resolved, nil
}

func validateExistingBackup(path string) (string, error) {
	resolved, err := filepath.Abs(path)
	if err != nil || resolved != filepath.Clean(resolved) {
		return "", fmt.Errorf("resolve backup path: %w", err)
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect backup: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("backup must be a regular file")
	}
	return resolved, nil
}

func quoteSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
