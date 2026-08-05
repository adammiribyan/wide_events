package maintenance

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/adammiribyan/wide_events/store/internal/config"
	"github.com/adammiribyan/wide_events/store/internal/disk"
)

const maintenanceTimeout = 5 * time.Minute

var (
	ErrDiskSpaceLow        = errors.New("disk space below write floor")
	ErrOperationInProgress = errors.New("maintenance operation already in progress")
	ErrRecoveryInProgress  = errors.New("disk recovery probe already in progress")
)

// DiskSpaceError marks a preflight rejection for the ingest writer's typed
// 507 mapping.
type DiskSpaceError struct {
	RequiredBytes  int64
	AvailableBytes int64
}

func (err *DiskSpaceError) Error() string {
	return fmt.Sprintf("%v: required=%d available=%d", ErrDiskSpaceLow, err.RequiredBytes, err.AvailableBytes)
}

func (err *DiskSpaceError) Unwrap() error             { return ErrDiskSpaceLow }
func (err *DiskSpaceError) InsufficientStorage() bool { return true }

type database interface {
	UpdateCapacity(context.Context, int64, int64) error
	SetWriteReady(context.Context, bool, string) error
	Retain(context.Context, time.Time, time.Time) error
	Checkpoint(context.Context) error
	RecordRetention(context.Context, time.Time, error) error
	RecordBackup(context.Context, time.Time, error) error
	CreateBackup(context.Context, string) error
	VerifyBackup(context.Context, string) error
}

type admissionHookSetter interface {
	SetAdmissionHook(func(context.Context) (func(), error))
}

type noSpaceHookSetter interface {
	SetNoSpaceHook(func(context.Context, error))
}

// Dependencies are process-owned resources used by maintenance. Test-only
// seams are operation-shaped and do not expose a SQL handle.
type Dependencies struct {
	Config          config.Config
	Database        database
	Writer          admissionHookSetter
	Queries         admissionHookSetter
	Probe           disk.Probe
	Logger          *slog.Logger
	BackupDirectory string
	Now             func() time.Time
}

// Manager coordinates capacity checks, retention, backups, and admissions.
type Manager struct {
	config    config.Config
	database  database
	probe     disk.Probe
	logger    *slog.Logger
	now       func() time.Time
	backupDir string
	gate      *gate

	operation chan struct{}
	recovery  chan struct{}
	stop      chan struct{}
	done      chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
	readiness sync.Mutex

	mutex           sync.Mutex
	lastDaily       string
	lastBackupPath  string
	requiredBytes   int64
	availableBytes  int64
	schedulerCancel context.CancelFunc
	started         bool
	closed          bool
	maintenanceOpen bool
	pendingDiskLow  bool
}

// NewManager constructs a manager and installs admission callbacks on the
// owned writer/query executors when supplied.
func NewManager(dependencies Dependencies) *Manager {
	probe := dependencies.Probe
	if probe == nil {
		probe = disk.StatFSProbe{}
	}
	logger := dependencies.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := dependencies.Now
	if now == nil {
		now = time.Now
	}
	backupDir := dependencies.BackupDirectory
	if backupDir == "" {
		backupDir = filepath.Join(filepath.Dir(dependencies.Config.Database), "backups")
	}
	manager := &Manager{
		config: dependencies.Config, database: dependencies.Database, probe: probe,
		logger: logger, now: now, backupDir: backupDir, gate: newGate(),
		operation: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		recovery: make(chan struct{}, 1),
	}
	if dependencies.Writer != nil {
		dependencies.Writer.SetAdmissionHook(manager.AdmitWrite)
		if setter, ok := dependencies.Writer.(noSpaceHookSetter); ok {
			setter.SetNoSpaceHook(manager.HandleNoSpace)
		}
	}
	if dependencies.Queries != nil {
		dependencies.Queries.SetAdmissionHook(manager.AdmitQuery)
	}
	return manager
}

// AdmitWrite enters normal writer admission and performs the binding capacity
// probe before ownership transfers to the writer.
func (manager *Manager) AdmitWrite(ctx context.Context) (func(), error) {
	manager.readiness.Lock()
	defer manager.readiness.Unlock()
	release, err := manager.gate.admitWrite(ctx)
	if err != nil {
		return nil, err
	}
	assessment, err := manager.assessCapacity(ctx)
	if err != nil {
		release()
		return nil, err
	}
	if err := manager.applyWriteCapacity(ctx, assessment); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// RecoverWrite performs one finite, singleflight disk probe for an HTTP
// request that encountered persisted disk degradation. It never waits behind
// another recovery probe and cannot clear an active maintenance gate.
func (manager *Manager) RecoverWrite(parent context.Context) error {
	select {
	case manager.recovery <- struct{}{}:
		defer func() { <-manager.recovery }()
	default:
		return ErrRecoveryInProgress
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	manager.readiness.Lock()
	defer manager.readiness.Unlock()
	release, err := manager.gate.admitWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	assessment, err := manager.assessCapacity(ctx)
	if err != nil {
		return err
	}
	return manager.applyWriteCapacity(ctx, assessment)
}

// AdmitQuery admits reads during disk degradation and retention, but not the
// exclusive backup copy window.
func (manager *Manager) AdmitQuery(ctx context.Context) (func(), error) {
	return manager.gate.admitQuery(ctx)
}

// HandleNoSpace publishes an actual database I/O exhaustion as the same typed
// disk degradation used by the preflight.
func (manager *Manager) HandleNoSpace(ctx context.Context, _ error) {
	manager.readiness.Lock()
	defer manager.readiness.Unlock()
	if manager.maintenanceOpen {
		manager.pendingDiskLow = true
		return
	}
	_ = manager.database.SetWriteReady(ctx, false, "disk_space_low")
}

// Start schedules one daily UTC maintenance attempt. It is idempotent. An
// optional context controls the scheduler lifetime.
func (manager *Manager) Start(contexts ...context.Context) {
	manager.startOnce.Do(func() {
		ctx := context.Background()
		if len(contexts) > 0 && contexts[0] != nil {
			ctx = contexts[0]
		}
		ctx, cancel := context.WithCancel(ctx)
		manager.mutex.Lock()
		if manager.closed {
			manager.mutex.Unlock()
			cancel()
			return
		}
		manager.started = true
		manager.schedulerCancel = cancel
		manager.mutex.Unlock()
		go manager.schedule(ctx)
	})
}

func (manager *Manager) schedule(ctx context.Context) {
	defer close(manager.done)
	for {
		now := manager.now().UTC()
		next := now.Truncate(24 * time.Hour).Add(24 * time.Hour)
		delay := next.Sub(now)
		if delay <= 0 || delay > 24*time.Hour {
			delay = 24 * time.Hour
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
			operationContext, cancel := context.WithTimeout(ctx, maintenanceTimeout)
			_ = manager.RunDaily(operationContext)
			cancel()
		case <-manager.stop:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

// RunDaily performs retention and then one automatic backup at most once per
// UTC calendar day, even when concurrent scheduler calls overlap.
func (manager *Manager) RunDaily(ctx context.Context) error {
	day := manager.now().UTC().Format("2006-01-02")
	manager.mutex.Lock()
	if manager.lastDaily == day {
		manager.mutex.Unlock()
		return nil
	}
	manager.lastDaily = day
	manager.mutex.Unlock()

	if err := manager.runRetention(ctx); err != nil {
		manager.logger.Error("scheduled retention failed", "utc_day", day, "error", err)
		return err
	}
	if err := manager.Backup(ctx); err != nil {
		manager.logger.Error("scheduled backup failed", "utc_day", day, "error", err)
		return err
	}
	return nil
}

func (manager *Manager) runRetention(parent context.Context) (result error) {
	ctx, cancel := boundedContext(parent)
	defer cancel()
	now := manager.now().UTC()
	defer func() { result = manager.recordRetentionResult(now, result) }()
	if !manager.beginOperation() {
		return ErrOperationInProgress
	}
	defer manager.endOperation()
	assessment, err := manager.assessCapacity(ctx)
	if err != nil {
		return err
	}
	if assessment.genericLow() {
		if err := manager.setWriteState(ctx, false, "disk_space_low"); err != nil {
			return err
		}
		return &DiskSpaceError{RequiredBytes: assessment.floor, AvailableBytes: assessment.space.Free}
	}
	if err := manager.beginMaintenance(ctx, false); err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, manager.finishMaintenance(false))
	}()

	retentionDays := manager.config.RetentionDays
	if retentionDays <= 0 {
		retentionDays = 30
	}
	eventCutoff := now.AddDate(0, 0, -retentionDays)
	ledgerCutoff := now.AddDate(0, 0, -7)
	if err := manager.database.Retain(ctx, eventCutoff, ledgerCutoff); err != nil {
		return err
	}
	if err := manager.database.Checkpoint(ctx); err != nil {
		return err
	}
	return nil
}

// Backup creates, verifies, and records one exclusive local database copy.
func (manager *Manager) Backup(parent context.Context) (result error) {
	ctx, cancel := boundedContext(parent)
	defer cancel()
	now := manager.now().UTC()
	defer func() { result = manager.recordBackupResult(now, result) }()
	if !manager.beginOperation() {
		return ErrOperationInProgress
	}
	defer manager.endOperation()
	assessment, err := manager.assessCapacity(ctx)
	if err != nil {
		return err
	}
	if err := assessment.backupError(); err != nil {
		ready, reason := true, ""
		if assessment.genericLow() {
			ready, reason = false, "disk_space_low"
		}
		if readinessErr := manager.setWriteState(ctx, ready, reason); readinessErr != nil {
			return errors.Join(err, readinessErr)
		}
		return err
	}
	if err := manager.beginMaintenance(ctx, true); err != nil {
		return err
	}
	diskLow := false
	defer func() {
		result = errors.Join(result, manager.finishMaintenance(diskLow))
	}()

	if err := manager.database.Checkpoint(ctx); err != nil {
		return err
	}
	assessment, err = manager.assessCapacity(ctx)
	if err != nil {
		return err
	}
	if err := assessment.backupError(); err != nil {
		diskLow = assessment.genericLow()
		return err
	}
	if err := manager.prepareBackupDirectory(); err != nil {
		return err
	}
	finalPath, err := manager.nextBackupPath(now)
	if err != nil {
		return err
	}
	candidatePath, err := manager.nextCandidatePath()
	if err != nil {
		return err
	}
	if err := manager.database.CreateBackup(ctx, candidatePath); err != nil {
		return err
	}
	candidate, err := openCandidateObject(manager.backupDir, candidatePath)
	if err != nil {
		return err
	}
	defer candidate.Close()
	published := false
	defer func() {
		if !published {
			_ = candidate.removeOwned()
		}
	}()
	if err := manager.database.VerifyBackup(ctx, candidatePath); err != nil {
		return err
	}
	if err := candidate.revalidate("after verification"); err != nil {
		return err
	}
	if err := candidate.publish(filepath.Base(finalPath)); err != nil {
		return err
	}
	published = true
	if err := syncDirectory(manager.backupDir); err != nil {
		return err
	}
	if err := manager.pruneBackups(ctx, finalPath); err != nil {
		return err
	}
	manager.mutex.Lock()
	manager.lastBackupPath = finalPath
	manager.mutex.Unlock()
	return nil
}

type capacityAssessment struct {
	space          disk.Space
	floor          int64
	backupRequired int64
}

func (assessment capacityAssessment) genericLow() bool {
	return assessment.space.Free < assessment.floor
}

func (assessment capacityAssessment) backupError() error {
	if assessment.space.Free < assessment.backupRequired {
		return newBackupSpaceError(assessment.backupRequired, assessment.space.Free)
	}
	return nil
}

func (manager *Manager) assessCapacity(ctx context.Context) (capacityAssessment, error) {
	info, err := os.Stat(manager.config.Database)
	if err != nil {
		return capacityAssessment{}, fmt.Errorf("stat database file: %w", err)
	}
	space, err := manager.probe.Space(manager.config.Database)
	if err != nil {
		return capacityAssessment{}, err
	}
	if err := manager.database.UpdateCapacity(ctx, info.Size(), space.Free); err != nil {
		return capacityAssessment{}, err
	}
	floor := disk.WriteFloor(space.Total)
	margin := info.Size() / 10
	if margin < 512<<20 {
		margin = 512 << 20
	}
	required := saturatingAdd(info.Size(), margin)
	if required < floor {
		required = floor
	}
	manager.setBackupSpace(required, space.Free)
	return capacityAssessment{
		space: space, floor: floor, backupRequired: required,
	}, nil
}

func (manager *Manager) applyWriteCapacity(ctx context.Context, assessment capacityAssessment) error {
	if assessment.genericLow() {
		if err := manager.database.SetWriteReady(ctx, false, "disk_space_low"); err != nil {
			return err
		}
		return &DiskSpaceError{RequiredBytes: assessment.floor, AvailableBytes: assessment.space.Free}
	}
	return manager.database.SetWriteReady(ctx, true, "")
}

// Lock order for readiness transitions is always readiness -> gate. Closing
// is marked while both are held, but draining happens after readiness is
// released so admitted writer callbacks can finish without inversion.
func (manager *Manager) beginMaintenance(ctx context.Context, queries bool) error {
	manager.readiness.Lock()
	manager.gate.startClose(queries)
	if err := manager.database.SetWriteReady(ctx, false, "maintenance"); err != nil {
		manager.gate.open()
		manager.readiness.Unlock()
		return err
	}
	manager.maintenanceOpen = true
	manager.readiness.Unlock()
	if err := manager.gate.drain(ctx, queries); err != nil {
		return errors.Join(err, manager.finishMaintenance(false))
	}
	return nil
}

func (manager *Manager) finishMaintenance(diskLow bool) error {
	manager.readiness.Lock()
	defer manager.readiness.Unlock()
	diskLow = diskLow || manager.pendingDiskLow
	ready, reason := true, ""
	if diskLow {
		ready, reason = false, "disk_space_low"
	}
	err := manager.setWriteReady(ready, reason)
	manager.maintenanceOpen = false
	manager.pendingDiskLow = false
	manager.gate.open()
	return err
}

func (manager *Manager) setWriteState(ctx context.Context, ready bool, reason string) error {
	manager.readiness.Lock()
	defer manager.readiness.Unlock()
	return manager.database.SetWriteReady(ctx, ready, reason)
}

func (manager *Manager) prepareBackupDirectory() error {
	databaseParent, err := filepath.Abs(filepath.Dir(manager.config.Database))
	if err != nil {
		return fmt.Errorf("resolve database directory: %w", err)
	}
	directory, err := filepath.Abs(manager.backupDir)
	if err != nil {
		return fmt.Errorf("resolve backup directory: %w", err)
	}
	resolvedParent, err := filepath.EvalSymlinks(databaseParent)
	if err != nil {
		return fmt.Errorf("resolve database directory symlinks: %w", err)
	}
	if filepath.Clean(directory) != directory || filepath.Dir(directory) != databaseParent && filepath.Dir(directory) != resolvedParent {
		return fmt.Errorf("backup directory must be a direct child of the database directory")
	}
	directory = filepath.Join(resolvedParent, filepath.Base(directory))
	manager.backupDir = directory
	if err := validateTrustedAncestors(resolvedParent); err != nil {
		return err
	}
	if err := os.Mkdir(directory, 0o700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("create backup directory: %w", err)
	}
	return manager.validateBackupDirectory()
}

func (manager *Manager) validateBackupDirectory() error {
	info, err := os.Lstat(manager.backupDir)
	if err != nil {
		return fmt.Errorf("inspect backup directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("backup path must be a real directory")
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf("backup directory mode must be 0700, got %04o", info.Mode().Perm())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("backup directory must be owned by uid %d", os.Geteuid())
	}
	return nil
}

func validateTrustedAncestors(path string) error {
	volume := filepath.VolumeName(path)
	remainder := strings.TrimPrefix(path, volume)
	current := volume + string(os.PathSeparator)
	for _, component := range strings.Split(strings.TrimPrefix(remainder, string(os.PathSeparator)), string(os.PathSeparator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect backup ancestor %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("backup ancestor %q must not be a symlink", current)
		}
		if !info.IsDir() {
			return fmt.Errorf("backup ancestor %q must be a directory", current)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("inspect backup ancestor %q ownership", current)
		}
		owner := int(stat.Uid)
		permissions := info.Mode().Perm()
		writableByOthers := permissions&0o022 != 0
		switch {
		case owner == os.Geteuid():
			if writableByOthers {
				return fmt.Errorf("backup ancestor %q is a writable ancestor (group or other)", current)
			}
		case owner == 0:
			rootStickyTemporary := info.Mode()&os.ModeSticky != 0 && permissions == 0o777
			if writableByOthers && !rootStickyTemporary {
				return fmt.Errorf("backup ancestor %q is a writable ancestor without the root-owned sticky exception", current)
			}
		default:
			return fmt.Errorf("backup ancestor %q is owned by untrusted uid %d", current, owner)
		}
	}
	return nil
}

func (manager *Manager) nextBackupPath(now time.Time) (string, error) {
	base := "wide-events-" + now.UTC().Format("20060102T150405.000000000Z")
	for sequence := 0; sequence < 1000; sequence++ {
		name := base + ".duckdb"
		if sequence > 0 {
			name = fmt.Sprintf("%s-%03d.duckdb", base, sequence)
		}
		path := filepath.Join(manager.backupDir, name)
		if filepath.Dir(path) != manager.backupDir {
			return "", fmt.Errorf("generated backup path escaped directory")
		}
		_, err := os.Lstat(path)
		if os.IsNotExist(err) {
			return path, nil
		}
		if err != nil {
			return "", fmt.Errorf("inspect backup path: %w", err)
		}
	}
	return "", fmt.Errorf("no unused backup name available")
}

func (manager *Manager) nextCandidatePath() (string, error) {
	if err := manager.validateBackupDirectory(); err != nil {
		return "", err
	}
	for attempt := 0; attempt < 32; attempt++ {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("generate backup candidate name: %w", err)
		}
		path := filepath.Join(manager.backupDir, ".wide-events-backup-"+hex.EncodeToString(random[:])+".partial")
		if _, err := os.Lstat(path); os.IsNotExist(err) {
			return path, nil
		} else if err != nil {
			return "", fmt.Errorf("inspect backup candidate: %w", err)
		}
	}
	return "", fmt.Errorf("no unused backup candidate name available")
}

func (manager *Manager) pruneBackups(ctx context.Context, current string) error {
	if err := manager.validateBackupDirectory(); err != nil {
		return err
	}
	entries, err := os.ReadDir(manager.backupDir)
	if err != nil {
		return fmt.Errorf("list backups: %w", err)
	}
	type backupFile struct {
		object   *backupObject
		name     string
		modified time.Time
	}
	backups := make([]backupFile, 0, len(entries))
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || !strings.HasPrefix(entry.Name(), "wide-events-") || !strings.HasSuffix(entry.Name(), ".duckdb") {
			continue
		}
		path := filepath.Join(manager.backupDir, entry.Name())
		object, err := openExistingBackupObject(manager.backupDir, path)
		if err != nil {
			continue
		}
		if path != current {
			verifyContext, cancel := context.WithTimeout(ctx, 30*time.Second)
			verifyErr := manager.database.VerifyBackup(verifyContext, path)
			cancel()
			if verifyErr != nil {
				_ = object.Close()
				continue
			}
		}
		if err := object.revalidate("after verification"); err != nil {
			_ = object.Close()
			continue
		}
		info, err := object.file.Stat()
		if err != nil {
			_ = object.Close()
			return fmt.Errorf("inspect verified backup %q: %w", entry.Name(), err)
		}
		backups = append(backups, backupFile{object: object, name: entry.Name(), modified: info.ModTime()})
	}
	defer func() {
		for _, backup := range backups {
			_ = backup.object.Close()
		}
	}()
	sort.Slice(backups, func(left, right int) bool {
		if backups[left].modified.Equal(backups[right].modified) {
			return backups[left].name < backups[right].name
		}
		return backups[left].modified.Before(backups[right].modified)
	})
	for len(backups) > 3 {
		oldest := backups[0]
		backups = backups[1:]
		removeErr := oldest.object.removeOwned()
		closeErr := oldest.object.Close()
		if errors.Is(removeErr, errBackupObjectChanged) {
			continue
		} else if removeErr != nil {
			return fmt.Errorf("remove expired backup: %w", removeErr)
		} else if closeErr != nil {
			return fmt.Errorf("close expired backup: %w", closeErr)
		}
	}
	return syncDirectory(manager.backupDir)
}

// LastBackupPath is returned to the private admin protocol after success.
func (manager *Manager) LastBackupPath() string {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return manager.lastBackupPath
}

// BackupSpace returns the most recent preflight requirement and availability.
func (manager *Manager) BackupSpace() (int64, int64) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return manager.requiredBytes, manager.availableBytes
}

func (manager *Manager) setBackupSpace(required, available int64) {
	manager.mutex.Lock()
	manager.requiredBytes, manager.availableBytes = required, available
	manager.mutex.Unlock()
}

func (manager *Manager) recordRetentionResult(at time.Time, result error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.database.RecordRetention(ctx, at, result); err != nil {
		return errors.Join(result, fmt.Errorf("record retention result: %w", err))
	}
	return result
}

func (manager *Manager) recordBackupResult(at time.Time, result error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.database.RecordBackup(ctx, at, result); err != nil {
		return errors.Join(result, fmt.Errorf("record backup result: %w", err))
	}
	return result
}

func (manager *Manager) setWriteReady(ready bool, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return manager.database.SetWriteReady(ctx, ready, reason)
}

func (manager *Manager) beginOperation() bool {
	select {
	case manager.operation <- struct{}{}:
		return true
	default:
		return false
	}
}

func (manager *Manager) endOperation() { <-manager.operation }

// Close stops the scheduler. It is safe before Start and is idempotent.
func (manager *Manager) Close() error {
	manager.closeOnce.Do(func() {
		manager.mutex.Lock()
		manager.closed = true
		cancel := manager.schedulerCancel
		started := manager.started
		manager.mutex.Unlock()
		close(manager.stop)
		if cancel != nil {
			cancel()
		}
		if started {
			select {
			case <-manager.done:
			case <-time.After(5 * time.Second):
			}
		}
	})
	manager.mutex.Lock()
	started := manager.started
	manager.mutex.Unlock()
	if started {
		select {
		case <-manager.done:
			return nil
		default:
			return fmt.Errorf("stop maintenance scheduler: timeout")
		}
	}
	return nil
}

func boundedContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, maintenanceTimeout)
}

func saturatingAdd(left, right int64) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	if left > maxInt64-right {
		return maxInt64
	}
	return left + right
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open backup directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync backup directory: %w", err)
	}
	return nil
}
