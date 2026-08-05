//go:build darwin || linux

package maintenance

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

var errBackupObjectChanged = errors.New("backup object identity changed")

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
	return openBackupObject(directoryPath, path, true)
}

func openExistingBackupObject(directoryPath, path string) (*backupObject, error) {
	return openBackupObject(directoryPath, path, false)
}

func openBackupObject(directoryPath, path string, secureMode bool) (*backupObject, error) {
	if filepath.Dir(path) != directoryPath {
		return nil, fmt.Errorf("backup object escaped trusted directory")
	}
	directoryFD, err := unix.Open(directoryPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open trusted backup directory: %w", err)
	}
	directory := os.NewFile(uintptr(directoryFD), directoryPath)
	object := &backupObject{directoryPath: directoryPath, directory: directory, name: filepath.Base(path)}
	if err := object.captureDirectoryIdentity(); err != nil {
		object.Close()
		return nil, err
	}
	fileFD, err := unix.Openat(directoryFD, object.name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		object.Close()
		return nil, fmt.Errorf("open backup object without following links: %w", err)
	}
	object.file = os.NewFile(uintptr(fileFD), path)
	stat, err := descriptorStat(fileFD)
	if err != nil {
		object.Close()
		return nil, err
	}
	if err := validateOwnedRegularType(stat); err != nil {
		object.Close()
		return nil, err
	}
	if secureMode {
		if err := unix.Fchmod(fileFD, 0o600); err != nil {
			object.Close()
			return nil, fmt.Errorf("secure backup object mode: %w", err)
		}
	}
	stat, err = descriptorStat(fileFD)
	if err != nil {
		object.Close()
		return nil, err
	}
	if err := validateOwnedRegular(stat); err != nil {
		object.Close()
		return nil, err
	}
	object.identity = identityOf(stat)
	if err := object.revalidate("opened"); err != nil {
		object.Close()
		return nil, err
	}
	if err := object.file.Sync(); err != nil {
		object.Close()
		return nil, fmt.Errorf("sync backup object: %w", err)
	}
	return object, nil
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
	if err := validateOwnedRegular(descriptor); err != nil {
		return err
	}
	if err := validateOwnedRegular(pathStat); err != nil {
		return err
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
	oldName := object.name
	object.name = finalName
	if err := object.revalidate("after publication"); err != nil {
		_ = object.removeOwned()
		object.name = oldName
		return err
	}
	return nil
}

func (object *backupObject) removeOwned() error {
	if err := object.revalidate("before quarantine"); err != nil {
		return err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Errorf("generate backup quarantine name: %w", err)
	}
	quarantine := ".wide-events-quarantine-" + hex.EncodeToString(random[:])
	if err := renameNoReplace(int(object.directory.Fd()), object.directoryPath, object.name, quarantine); err != nil {
		return fmt.Errorf("quarantine owned backup: %w", err)
	}
	stat, err := object.statName(quarantine)
	if err != nil {
		return fmt.Errorf("inspect quarantined backup: %w", err)
	}
	if identityOf(stat) != object.identity {
		_ = renameNoReplace(int(object.directory.Fd()), object.directoryPath, quarantine, object.name)
		return fmt.Errorf("%w: backup object changed before quarantine", errBackupObjectChanged)
	}
	if err := unix.Unlinkat(int(object.directory.Fd()), quarantine, 0); err != nil {
		return fmt.Errorf("remove quarantined backup: %w", err)
	}
	return nil
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
