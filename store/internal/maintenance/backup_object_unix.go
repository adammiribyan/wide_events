//go:build darwin || linux

package maintenance

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

var (
	errBackupObjectChanged  = errors.New("backup object identity changed")
	errBackupRollbackFailed = errors.New("backup quarantine rollback failed")
)

type objectIdentity struct {
	device uint64
	inode  uint64
}

type backupObject struct {
	directoryPath string
	directory     *os.File
	directoryID   objectIdentity
	file          *os.File
	identity      objectIdentity
	name          string
}

func openCandidateObject(directoryPath, path string) (*backupObject, error) {
	object, err := reserveCandidateObject(directoryPath, path)
	if err != nil {
		return nil, err
	}
	if err := object.openFile(true); err != nil {
		_ = object.Close()
		return nil, err
	}
	return object, nil
}

func openExistingBackupObject(directoryPath, path string) (*backupObject, error) {
	object, err := reserveCandidateObject(directoryPath, path)
	if err != nil {
		return nil, err
	}
	if err := object.openFile(false); err != nil {
		_ = object.Close()
		return nil, err
	}
	return object, nil
}

func reserveCandidateObject(directoryPath, path string) (*backupObject, error) {
	if filepath.Dir(path) != directoryPath {
		return nil, fmt.Errorf("backup object escaped trusted directory")
	}
	directory, err := openTrustedDirectoryPath(directoryPath)
	if err != nil {
		return nil, fmt.Errorf("open trusted backup directory: %w", err)
	}
	object := &backupObject{directoryPath: directoryPath, directory: directory, name: filepath.Base(path)}
	if err := object.captureDirectoryIdentity(); err != nil {
		object.Close()
		return nil, err
	}
	return object, nil
}

// openTrustedDirectoryPath walks from a validated filesystem root using one
// no-follow directory descriptor per component. The store UID is trusted; the
// traversal prevents other principals, symlinks, and ancestor pathname swaps,
// but does not claim protection from an actively malicious same-UID process.
func openTrustedDirectoryPath(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("trusted directory path must be absolute and clean")
	}
	rootFD, err := unix.Open(string(os.PathSeparator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open filesystem root: %w", err)
	}
	current := os.NewFile(uintptr(rootFD), string(os.PathSeparator))
	rootStat, err := descriptorStat(rootFD)
	if err != nil {
		_ = current.Close()
		return nil, fmt.Errorf("inspect filesystem root: %w", err)
	}
	if err := validateTrustedAncestor(rootStat, string(os.PathSeparator)); err != nil {
		_ = current.Close()
		return nil, err
	}
	components := strings.Split(strings.TrimPrefix(path, string(os.PathSeparator)), string(os.PathSeparator))
	currentPath := string(os.PathSeparator)
	for _, component := range components {
		if component == "" {
			continue
		}
		nextFD, err := unix.Openat(int(current.Fd()), component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			_ = current.Close()
			return nil, fmt.Errorf("open trusted ancestor %q: %w", filepath.Join(currentPath, component), err)
		}
		nextPath := filepath.Join(currentPath, component)
		next := os.NewFile(uintptr(nextFD), nextPath)
		stat, err := descriptorStat(nextFD)
		if err != nil {
			_ = next.Close()
			_ = current.Close()
			return nil, fmt.Errorf("inspect trusted ancestor %q: %w", nextPath, err)
		}
		if err := validateTrustedAncestor(stat, nextPath); err != nil {
			_ = next.Close()
			_ = current.Close()
			return nil, err
		}
		_ = current.Close()
		current = next
		currentPath = nextPath
	}
	return current, nil
}

func validateTrustedAncestor(stat unix.Stat_t, path string) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("backup ancestor %q must be a directory", path)
	}
	owner := int(stat.Uid)
	permissions := stat.Mode & 0o777
	writableByOthers := permissions&0o022 != 0
	switch {
	case owner == os.Geteuid():
		if writableByOthers {
			return fmt.Errorf("backup ancestor %q is a writable ancestor (group or other)", path)
		}
	case owner == 0:
		rootStickyTemporary := stat.Mode&unix.S_ISVTX != 0 && permissions == 0o777
		if writableByOthers && !rootStickyTemporary {
			return fmt.Errorf("backup ancestor %q is a writable ancestor without the root-owned sticky exception", path)
		}
	default:
		return fmt.Errorf("backup ancestor %q is owned by untrusted uid %d", path, owner)
	}
	return nil
}

func ensureOwnedBackupDirectory(parentPath, name string) error {
	if filepath.Base(name) != name || name == "." {
		return fmt.Errorf("invalid backup directory name")
	}
	parent, err := openTrustedDirectoryPath(parentPath)
	if err != nil {
		return err
	}
	defer parent.Close()
	if err := unix.Mkdirat(int(parent.Fd()), name, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
		return fmt.Errorf("create backup directory: %w", err)
	}
	directoryFD, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open backup directory without following links: %w", err)
	}
	directory := os.NewFile(uintptr(directoryFD), filepath.Join(parentPath, name))
	defer directory.Close()
	stat, err := descriptorStat(directoryFD)
	if err != nil {
		return fmt.Errorf("inspect backup directory descriptor: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || int(stat.Uid) != os.Geteuid() || stat.Mode&0o777 != 0o700 {
		return fmt.Errorf("backup directory must be an owner-controlled 0700 directory")
	}
	return nil
}

func (object *backupObject) openFile(secureMode bool) error {
	fileFD, err := unix.Openat(int(object.directory.Fd()), object.name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open backup object without following links: %w", err)
	}
	object.file = os.NewFile(uintptr(fileFD), filepath.Join(object.directoryPath, object.name))
	stat, err := descriptorStat(fileFD)
	if err != nil {
		_ = object.file.Close()
		object.file = nil
		return err
	}
	if err := validateOwnedRegularType(stat); err != nil {
		_ = object.file.Close()
		object.file = nil
		return err
	}
	object.identity = identityOf(stat)
	if secureMode {
		if err := unix.Fchmod(fileFD, 0o600); err != nil {
			return fmt.Errorf("secure backup object mode: %w", err)
		}
	}
	stat, err = descriptorStat(fileFD)
	if err != nil {
		return err
	}
	if err := validateOwnedRegular(stat); err != nil {
		return err
	}
	if identityOf(stat) != object.identity {
		return fmt.Errorf("%w: backup descriptor changed while opening", errBackupObjectChanged)
	}
	if err := object.revalidate("opened"); err != nil {
		return err
	}
	if err := object.file.Sync(); err != nil {
		return fmt.Errorf("sync backup object: %w", err)
	}
	return nil
}

func (object *backupObject) captureDirectoryIdentity() error {
	stat, err := descriptorStat(int(object.directory.Fd()))
	if err != nil {
		return fmt.Errorf("inspect trusted backup directory descriptor: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || int(stat.Uid) != os.Geteuid() || stat.Mode&0o777 != 0o700 {
		return fmt.Errorf("trusted backup directory descriptor changed")
	}
	object.directoryID = identityOf(stat)
	return object.revalidateDirectory()
}

func (object *backupObject) revalidateDirectory() error {
	var pathStat unix.Stat_t
	if err := unix.Lstat(object.directoryPath, &pathStat); err != nil {
		return fmt.Errorf("inspect trusted backup directory path: %w", err)
	}
	descriptor, err := descriptorStat(int(object.directory.Fd()))
	if err != nil {
		return fmt.Errorf("inspect trusted backup directory descriptor: %w", err)
	}
	trustedPath := pathStat.Mode&unix.S_IFMT == unix.S_IFDIR && int(pathStat.Uid) == os.Geteuid() && pathStat.Mode&0o777 == 0o700
	trustedDescriptor := descriptor.Mode&unix.S_IFMT == unix.S_IFDIR && int(descriptor.Uid) == os.Geteuid() && descriptor.Mode&0o777 == 0o700
	if identityOf(pathStat) != object.directoryID || identityOf(descriptor) != object.directoryID || !trustedPath || !trustedDescriptor {
		return fmt.Errorf("%w: trusted backup directory changed", errBackupObjectChanged)
	}
	return nil
}

func (object *backupObject) revalidate(stage string) error {
	return object.revalidateOwned(stage, true)
}

func (object *backupObject) revalidateOwned(stage string, requireSecureMode bool) error {
	if err := object.revalidateDirectory(); err != nil {
		return err
	}
	descriptor, err := descriptorStat(int(object.file.Fd()))
	if err != nil {
		return fmt.Errorf("inspect backup descriptor: %w", err)
	}
	pathStat, err := object.statName(object.name)
	if err != nil {
		return fmt.Errorf("inspect backup object %s: %w", stage, err)
	}
	if identityOf(descriptor) != object.identity || identityOf(pathStat) != object.identity {
		return fmt.Errorf("%w: backup candidate changed during verification", errBackupObjectChanged)
	}
	if requireSecureMode {
		if err := validateOwnedRegular(descriptor); err != nil {
			return err
		}
		if err := validateOwnedRegular(pathStat); err != nil {
			return err
		}
	} else {
		if err := validateOwnedRegularType(descriptor); err != nil {
			return err
		}
		if err := validateOwnedRegularType(pathStat); err != nil {
			return err
		}
	}
	return nil
}

func (object *backupObject) publish(finalName string) error {
	if filepath.Base(finalName) != finalName {
		return fmt.Errorf("invalid final backup name")
	}
	if err := object.revalidate("before publication"); err != nil {
		return err
	}
	if err := renameNoReplace(int(object.directory.Fd()), object.directoryPath, object.name, finalName); err != nil {
		return fmt.Errorf("publish verified backup without replacement: %w", err)
	}
	object.name = finalName
	return object.validatePublished()
}

func (object *backupObject) validatePublished() error {
	validationErr := object.revalidate("after publication")
	if validationErr == nil {
		return nil
	}
	if cleanupErr := object.cleanupReserved(); cleanupErr != nil {
		return errors.Join(validationErr, fmt.Errorf("cleanup invalid published backup: %w", cleanupErr))
	}
	return validationErr
}

func (object *backupObject) removeOwned() error {
	_, err := object.quarantine()
	if err != nil {
		return err
	}
	return object.unlinkQuarantined()
}

func (object *backupObject) quarantine() (string, error) {
	if err := object.revalidateOwned("before quarantine", false); err != nil {
		return "", err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate backup quarantine name: %w", err)
	}
	quarantine := ".wide-events-quarantine-" + hex.EncodeToString(random[:])
	original := object.name
	if err := renameNoReplace(int(object.directory.Fd()), object.directoryPath, object.name, quarantine); err != nil {
		return "", fmt.Errorf("quarantine owned backup: %w", err)
	}
	object.name = quarantine
	stat, err := object.statName(quarantine)
	if err != nil {
		rollbackErr := object.rollbackQuarantine(original)
		return "", errors.Join(fmt.Errorf("inspect quarantined backup: %w", err), rollbackErr)
	}
	if identityOf(stat) != object.identity {
		rollbackErr := object.rollbackQuarantine(original)
		return "", errors.Join(fmt.Errorf("%w: backup object changed before quarantine", errBackupObjectChanged), rollbackErr)
	}
	return original, nil
}

func (object *backupObject) rollbackQuarantine(original string) error {
	if err := renameNoReplace(int(object.directory.Fd()), object.directoryPath, object.name, original); err != nil {
		return fmt.Errorf("%w: restore without replacement: %v", errBackupRollbackFailed, err)
	}
	object.name = original
	return nil
}

func (object *backupObject) unlinkQuarantined() error {
	if err := object.revalidateOwned("after quarantine", false); err != nil {
		return err
	}
	if err := unix.Unlinkat(int(object.directory.Fd()), object.name, 0); err != nil {
		return fmt.Errorf("remove quarantined backup: %w", err)
	}
	return nil
}

func (object *backupObject) cleanupReserved() error {
	if object.file == nil {
		fileFD, err := unix.Openat(int(object.directory.Fd()), object.name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("open partial backup for cleanup: %w", err)
		}
		object.file = os.NewFile(uintptr(fileFD), filepath.Join(object.directoryPath, object.name))
		stat, err := descriptorStat(fileFD)
		if err != nil {
			return fmt.Errorf("inspect partial backup for cleanup: %w", err)
		}
		if err := validateOwnedRegularType(stat); err != nil {
			return err
		}
		object.identity = identityOf(stat)
	}
	err := object.removeOwned()
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}

func (object *backupObject) statName(name string) (unix.Stat_t, error) {
	var stat unix.Stat_t
	err := unix.Fstatat(int(object.directory.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	return stat, err
}

func (object *backupObject) Close() error {
	var result error
	if object.file != nil {
		result = object.file.Close()
	}
	if object.directory != nil {
		if err := object.directory.Close(); result == nil {
			result = err
		}
	}
	return result
}

func descriptorStat(fd int) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return unix.Stat_t{}, err
	}
	return stat, nil
}

func identityOf(stat unix.Stat_t) objectIdentity {
	return objectIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}
}

func validateOwnedRegular(stat unix.Stat_t) error {
	if err := validateOwnedRegularType(stat); err != nil {
		return err
	}
	if stat.Mode&0o777 != 0o600 {
		return fmt.Errorf("backup object mode must be 0600")
	}
	return nil
}

func validateOwnedRegularType(stat unix.Stat_t) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("backup object must be a regular file")
	}
	if int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("backup object must be owned by uid %d", os.Geteuid())
	}
	return nil
}
