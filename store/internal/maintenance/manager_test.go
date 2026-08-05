package maintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/disk"
	"github.com/adammiribyan/wide_events/store/internal/storage"
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

func TestSuccessfulSecondPreflightKeepsMaintenanceReadinessThroughCopy(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	database.createHook = func(path string) error {
		assert.False(t, database.ready)
		assert.Equal(t, "maintenance", database.reason)
		assert.Contains(t, filepath.Base(path), ".partial")
		return nil
	}
	require.NoError(t, manager.Backup(context.Background()))
	assert.True(t, database.ready)
}

func TestPostGateBackupMarginFailureRestoresNormalReadiness(t *testing.T) {
	probe := &fakeProbe{spaces: []disk.Space{
		{Free: 10 << 30, Total: 50 << 30},
		{Free: 6 << 30, Total: 50 << 30},
	}}
	manager, database := newTestManager(t, probe, 8<<30)
	err := manager.Backup(context.Background())
	require.ErrorIs(t, err, ErrInsufficientBackupSpace)
	assert.True(t, database.ready)
	assert.Empty(t, database.reason)
	assert.Empty(t, database.created)
	assert.Equal(t, 1, database.recordBackupCalls)
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

func TestRecoveryProbeIsFiniteSingleflight(t *testing.T) {
	probe := &blockingProbe{started: make(chan struct{}), release: make(chan struct{})}
	manager, database := newTestManager(t, probe, 64<<20)
	require.NoError(t, database.SetWriteReady(context.Background(), false, "disk_space_low"))
	first := make(chan error, 1)
	go func() { first <- manager.RecoverWrite(context.Background()) }()
	<-probe.started
	assert.ErrorIs(t, manager.RecoverWrite(context.Background()), ErrRecoveryInProgress)
	close(probe.release)
	require.NoError(t, <-first)
	assert.Equal(t, 1, probe.calls)
	assert.True(t, database.ready)
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

func TestBackupPublishesFinalNameOnlyAfterCandidateVerification(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	database.createHook = func(path string) error {
		assert.True(t, strings.HasPrefix(filepath.Base(path), ".wide-events-backup-"))
		assert.True(t, strings.HasSuffix(filepath.Base(path), ".partial"))
		assert.Empty(t, finalBackupNames(t, manager.backupDir))
		return nil
	}
	database.verifyHook = func(path string) error {
		assert.True(t, strings.HasSuffix(path, ".partial"))
		assert.Empty(t, finalBackupNames(t, manager.backupDir))
		return nil
	}
	require.NoError(t, manager.Backup(context.Background()))
	assert.Len(t, finalBackupNames(t, manager.backupDir), 1)
	assert.NotContains(t, manager.LastBackupPath(), ".partial")
}

func TestManagerPublishesARealVerifiedDuckDBBackup(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "wide-events.duckdb")
	database, err := storage.Open(context.Background(), config.Config{
		Database: databasePath, Service: "checkout", Environment: "test", QueryTempBytes: 64 << 20,
	})
	require.NoError(t, err)
	manager := NewManager(Dependencies{
		Config: config.Config{Database: databasePath, RetentionDays: 30}, Database: database,
		Probe: &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}},
		Now:   func() time.Time { return time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC) },
	})
	t.Cleanup(func() {
		require.NoError(t, manager.Close())
		require.NoError(t, database.Close())
	})
	require.NoError(t, manager.Backup(context.Background()))
	assert.FileExists(t, manager.LastBackupPath())
	require.NoError(t, database.VerifyBackup(context.Background(), manager.LastBackupPath()))
}

func TestFailedBackupVerificationRemovesPartialCopy(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	database.verifyErr = errors.New("verification failed")
	database.verifyHook = func(string) error {
		assert.Empty(t, finalBackupNames(t, manager.backupDir))
		return nil
	}
	assert.ErrorContains(t, manager.Backup(context.Background()), "verification failed")
	entries, err := os.ReadDir(manager.backupDir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestCorruptBackupCannotDisplaceThreeVerifiedCopies(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	require.NoError(t, manager.prepareBackupDirectory())
	for index := 1; index <= 4; index++ {
		path := filepath.Join(manager.backupDir, fmt.Sprintf("wide-events-2020010%dT000000.000000000Z.duckdb", index))
		require.NoError(t, os.WriteFile(path, []byte("verified"), 0o600))
		require.NoError(t, os.Chtimes(path, time.Unix(int64(index), 0), time.Unix(int64(index), 0)))
	}
	corrupt := filepath.Join(manager.backupDir, "wide-events-20990101T000000.000000000Z.duckdb")
	require.NoError(t, os.WriteFile(corrupt, []byte("corrupt"), 0o600))
	require.NoError(t, os.Chtimes(corrupt, time.Unix(100, 0), time.Unix(100, 0)))
	database.verifyHook = func(path string) error {
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(content) != "verified" {
			return errors.New("corrupt backup")
		}
		return nil
	}
	require.NoError(t, manager.Backup(context.Background()))
	verified := 0
	for _, path := range finalBackupNames(t, manager.backupDir) {
		if content, _ := os.ReadFile(path); string(content) == "verified" {
			verified++
		}
	}
	assert.Equal(t, 3, verified)
}

func TestBackupRejectsSymlinkAncestorAndUnsafeDirectoryMode(t *testing.T) {
	for name, setup := range map[string]func(*testing.T, *Manager){
		"symlink ancestor": func(t *testing.T, manager *Manager) {
			outside := t.TempDir()
			link := filepath.Join(filepath.Dir(manager.config.Database), "linked")
			require.NoError(t, os.Symlink(outside, link))
			manager.backupDir = filepath.Join(link, "backups")
		},
		"unsafe mode": func(t *testing.T, manager *Manager) {
			require.NoError(t, os.Mkdir(manager.backupDir, 0o755))
		},
	} {
		t.Run(name, func(t *testing.T) {
			manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
			setup(t, manager)
			assert.Error(t, manager.Backup(context.Background()))
			assert.Empty(t, database.created)
		})
	}
}

func TestBackupRejectsCandidateReplacement(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	victim := filepath.Join(t.TempDir(), "victim")
	require.NoError(t, os.WriteFile(victim, []byte("unchanged"), 0o600))
	database.createHook = func(path string) error {
		require.NoError(t, os.Remove(path))
		return os.Symlink(victim, path)
	}
	assert.Error(t, manager.Backup(context.Background()))
	content, err := os.ReadFile(victim)
	require.NoError(t, err)
	assert.Equal(t, "unchanged", string(content))
	assert.Empty(t, finalBackupNames(t, manager.backupDir))
}

func TestGateFailuresRecordStatusExactlyOnce(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	release, err := manager.gate.admitWrite(context.Background())
	require.NoError(t, err)
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, manager.Backup(ctx), context.DeadlineExceeded)
	assert.Equal(t, 1, database.recordBackupCalls)
	assert.ErrorIs(t, database.lastBackupResult, context.DeadlineExceeded)
}

func TestReadinessFailureRecordsBackupStatusExactlyOnce(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	database.setReadyErr = errors.New("readiness unavailable")
	assert.ErrorContains(t, manager.Backup(context.Background()), "readiness unavailable")
	assert.Equal(t, 1, database.recordBackupCalls)
	assert.ErrorContains(t, database.lastBackupResult, "readiness unavailable")
}

func TestScheduledRetentionFailureLogsAndRecordsOnce(t *testing.T) {
	manager, database := newTestManager(t, &fakeProbe{spaces: []disk.Space{{Free: 10 << 30, Total: 20 << 30}}}, 64<<20)
	database.retainErr = errors.New("retention failed")
	logs := &lockedBuffer{}
	manager.logger = slog.New(slog.NewTextHandler(logs, nil))
	manager.now = func() time.Time {
		return time.Now().UTC().Truncate(24 * time.Hour).Add(24*time.Hour - 10*time.Millisecond)
	}
	manager.Start(context.Background())
	require.Eventually(t, func() bool { return strings.Contains(logs.String(), "retention failed") }, time.Second, 5*time.Millisecond)
	require.NoError(t, manager.Close())
	assert.Equal(t, 1, strings.Count(logs.String(), "level=ERROR"))
	assert.Equal(t, 1, database.recordRetentionCalls)
}

type lockedBuffer struct {
	mutex sync.Mutex
	bytes.Buffer
}

func (buffer *lockedBuffer) Write(value []byte) (int, error) {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.Buffer.Write(value)
}

func (buffer *lockedBuffer) String() string {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.Buffer.String()
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

type blockingProbe struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	calls   int
}

func (probe *blockingProbe) Space(string) (disk.Space, error) {
	probe.calls++
	probe.once.Do(func() { close(probe.started) })
	<-probe.release
	return disk.Space{Free: 3 << 30, Total: 10 << 30}, nil
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
	mutex                sync.Mutex
	databaseBytes        int64
	freeBytes            int64
	ready                bool
	reason               string
	retentions           int
	backups              int
	verifications        int
	created              []string
	verifyErr            error
	createHook           func(string) error
	verifyHook           func(string) error
	retainErr            error
	recordBackupCalls    int
	recordRetentionCalls int
	lastBackupResult     error
	lastRetentionResult  error
	setReadyErr          error
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
	if database.setReadyErr != nil {
		return database.setReadyErr
	}
	database.ready = ready
	database.reason = reason
	return nil
}

func (database *fakeDatabase) Retain(context.Context, time.Time, time.Time) error {
	database.retentions++
	return database.retainErr
}

func (*fakeDatabase) Checkpoint(context.Context) error { return nil }
func (database *fakeDatabase) RecordRetention(_ context.Context, _ time.Time, result error) error {
	database.recordRetentionCalls++
	database.lastRetentionResult = result
	return nil
}
func (database *fakeDatabase) RecordBackup(_ context.Context, _ time.Time, result error) error {
	database.recordBackupCalls++
	database.lastBackupResult = result
	return nil
}

func (database *fakeDatabase) CreateBackup(_ context.Context, path string) error {
	database.backups++
	database.created = append(database.created, path)
	if err := os.WriteFile(path, []byte("verified"), 0o600); err != nil {
		return err
	}
	if database.createHook != nil {
		return database.createHook(path)
	}
	return nil
}

func (database *fakeDatabase) VerifyBackup(_ context.Context, path string) error {
	database.verifications++
	if database.verifyHook != nil {
		if err := database.verifyHook(path); err != nil {
			return err
		}
	}
	return database.verifyErr
}

func finalBackupNames(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	var paths []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "wide-events-") && strings.HasSuffix(entry.Name(), ".duckdb") {
			paths = append(paths, filepath.Join(directory, entry.Name()))
		}
	}
	return paths
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
