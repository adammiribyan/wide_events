package maintenance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/disk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGateClosesOnlyRequestedAdmissionsAndIsFinite(t *testing.T) {
	gate := newGate()
	releaseWrite, err := gate.admitWrite(context.Background())
	require.NoError(t, err)
	releaseQuery, err := gate.admitQuery(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = gate.closeWrites(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	releaseWrite()
	releaseQuery()

	require.NoError(t, gate.closeWrites(context.Background()))
	_, err = gate.admitWrite(context.Background())
	assert.ErrorIs(t, err, ErrMaintenance)
	release, err := gate.admitQuery(context.Background())
	require.NoError(t, err)
	release()
	gate.open()
}

func TestBackupSpaceErrorCarriesRequiredAndAvailableBytes(t *testing.T) {
	err := newBackupSpaceError(9<<30, 5<<30)
	assert.ErrorIs(t, err, ErrInsufficientBackupSpace)
	var detail *BackupSpaceError
	require.True(t, errors.As(err, &detail))
	assert.Equal(t, int64(9<<30), detail.RequiredBytes)
	assert.Equal(t, int64(5<<30), detail.AvailableBytes)
}

func TestBackupRequiresDatabaseBytesPlusMarginAndTheGenericFloor(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 5 << 30, Total: 50 << 30}}}, 8<<30)
	err := manager.Backup(context.Background())
	assert.ErrorIs(t, err, ErrInsufficientBackupSpace)
	var detail *BackupSpaceError
	require.ErrorAs(t, err, &detail)
	assert.Equal(t, int64(8<<30+(8<<30)/10), detail.RequiredBytes)
	assert.Equal(t, int64(5<<30), detail.AvailableBytes)
	assert.Empty(t, database.created)
}

func TestBackupReportsTypedCapacityErrorWhenGenericFloorIsBinding(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 1 << 30, Total: 50 << 30}}}, 64<<20)
	err := manager.Backup(context.Background())
	assert.ErrorIs(t, err, ErrInsufficientBackupSpace)
	assert.NotErrorIs(t, err, ErrDiskSpaceLow)
	var detail *BackupSpaceError
	require.ErrorAs(t, err, &detail)
	assert.Equal(t, int64(5<<30), detail.RequiredBytes)
	assert.Equal(t, "disk_space_low", database.reason)
}

func TestBackupMarginFailureClearsEarlierDiskDegradationWhenFloorNowPasses(t *testing.T) {
	probe := &fakeProbe{spaces: []disk.Space{
		{Free: 1 << 30, Total: 10 << 30},
		{Free: 3 << 30, Total: 10 << 30},
	}}
	manager, database := newTestManager(t, probe, 3<<30)
	_, err := manager.AdmitWrite(context.Background())
	require.ErrorIs(t, err, ErrDiskSpaceLow)
	assert.False(t, database.ready)
	err = manager.Backup(context.Background())
	require.ErrorIs(t, err, ErrInsufficientBackupSpace)
	assert.True(t, database.ready)
	assert.Empty(t, database.reason)
}

func TestBackupRepeatsCapacityPreflightAfterClosingTheGate(t *testing.T) {
	probe := &fakeProbe{spaces: []disk.Space{
		{Free: 10 << 30, Total: 50 << 30},
		{Free: 5 << 30, Total: 50 << 30},
	}}
	manager, database := newTestManager(t, probe, 8<<30)
	err := manager.Backup(context.Background())
	assert.ErrorIs(t, err, ErrInsufficientBackupSpace)
	assert.Empty(t, database.created)
	assert.Equal(t, 2, probe.calls)
}

func TestWriteAdmissionPublishesCapacityAndClearsOnlyAfterPassingProbe(t *testing.T) {
	probe := &fakeProbe{spaces: []disk.Space{
		{Free: 1 << 30, Total: 10 << 30},
		{Free: 3 << 30, Total: 10 << 30},
	}}
	manager, database := newTestManager(t, probe, 64<<20)
	_, err := manager.AdmitWrite(context.Background())
	assert.ErrorIs(t, err, ErrDiskSpaceLow)
	assert.Equal(t, "disk_space_low", database.reason)

	release, err := manager.AdmitWrite(context.Background())
	require.NoError(t, err)
	release()
	assert.True(t, database.ready)
	assert.Empty(t, database.reason)
	assert.Equal(t, int64(64<<20), database.databaseBytes)
	assert.Equal(t, int64(3<<30), database.freeBytes)
}

func TestRunDailyRunsRetentionAndAutomaticBackupAtMostOncePerUTCDay(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	manager.now = func() time.Time { return time.Date(2026, 8, 5, 23, 30, 0, 0, time.FixedZone("west", -7*60*60)) }
	require.NoError(t, manager.RunDaily(context.Background()))
	require.NoError(t, manager.RunDaily(context.Background()))
	assert.Equal(t, 1, database.retentions)
	assert.Equal(t, 1, database.backups)
	assert.Equal(t, 1, database.verifications)
}

func TestBackupRetentionUsesVerifiedFileAgeDespiteClockShapedNames(t *testing.T) {
	manager, _ := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	require.NoError(t, manager.prepareBackupDirectory())
	old := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	for index := 1; index <= 3; index++ {
		path := filepath.Join(manager.backupDir, "wide-events-2099010"+string(rune('0'+index))+"T000000.000000000Z.duckdb")
		require.NoError(t, os.WriteFile(path, []byte("old"), 0o600))
		require.NoError(t, os.Chtimes(path, old.Add(time.Duration(index)*time.Hour), old.Add(time.Duration(index)*time.Hour)))
	}
	manager.now = func() time.Time { return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC) }
	require.NoError(t, manager.Backup(context.Background()))
	entries, err := os.ReadDir(manager.backupDir)
	require.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.FileExists(t, manager.LastBackupPath())
}

func TestFailedBackupVerificationRemovesPartialCopy(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	database.verifyErr = errors.New("verification failed")
	assert.ErrorContains(t, manager.Backup(context.Background()), "verification failed")
	entries, err := os.ReadDir(manager.backupDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestCloseCancelsAndJoinsTheFiniteScheduler(t *testing.T) {
	manager, _ := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	manager.Start(context.Background())
	require.NoError(t, manager.Close())
	select {
	case <-manager.done:
	default:
		t.Fatal("Close returned before the scheduler stopped")
	}
}

type fakeProbe struct {
	mutex  sync.Mutex
	spaces []disk.Space
	calls  int
}

func (probe *fakeProbe) Space(string) (disk.Space, error) {
	probe.mutex.Lock()
	defer probe.mutex.Unlock()
	probe.calls++
	if len(probe.spaces) == 0 {
		return disk.Space{}, errors.New("unexpected probe")
	}
	space := probe.spaces[0]
	if len(probe.spaces) > 1 {
		probe.spaces = probe.spaces[1:]
	}
	return space, nil
}

type fakeDatabase struct {
	mutex         sync.Mutex
	databaseBytes int64
	freeBytes     int64
	ready         bool
	reason        string
	retentions    int
	backups       int
	verifications int
	created       []string
	verifyErr     error
}

func (database *fakeDatabase) UpdateCapacity(_ context.Context, databaseBytes, freeBytes int64) error {
	database.mutex.Lock()
	defer database.mutex.Unlock()
	database.databaseBytes = databaseBytes
	database.freeBytes = freeBytes
	return nil
}

func (database *fakeDatabase) SetWriteReady(_ context.Context, ready bool, reason string) error {
	database.mutex.Lock()
	defer database.mutex.Unlock()
	database.ready = ready
	database.reason = reason
	return nil
}

func (database *fakeDatabase) Retain(context.Context, time.Time, time.Time) error {
	database.retentions++
	return nil
}

func (*fakeDatabase) Checkpoint(context.Context) error                        { return nil }
func (*fakeDatabase) RecordRetention(context.Context, time.Time, error) error { return nil }
func (*fakeDatabase) RecordBackup(context.Context, time.Time, error) error    { return nil }

func (database *fakeDatabase) CreateBackup(_ context.Context, path string) error {
	database.backups++
	database.created = append(database.created, path)
	return os.WriteFile(path, []byte("verified"), 0o600)
}

func (database *fakeDatabase) VerifyBackup(context.Context, string) error {
	database.verifications++
	return database.verifyErr
}

func newTestManager(t *testing.T, probe disk.Probe, databaseBytes int64) (*Manager, *fakeDatabase) {
	t.Helper()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "wide-events.duckdb")
	require.NoError(t, os.WriteFile(databasePath, nil, 0o600))
	require.NoError(t, os.Truncate(databasePath, databaseBytes))
	database := &fakeDatabase{}
	manager := NewManager(Dependencies{
		Config:          config.Config{Database: databasePath, RetentionDays: 30},
		Database:        database,
		Probe:           probe,
		BackupDirectory: filepath.Join(directory, "backups"),
		Now:             func() time.Time { return time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC) },
	})
	t.Cleanup(func() { _ = manager.Close() })
	return manager, database
}
