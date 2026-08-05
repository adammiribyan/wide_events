package maintenance

import (
	"errors"
	"fmt"
)

var ErrInsufficientBackupSpace = errors.New("insufficient space for backup")

// BackupSpaceError reports the exact capacity rejected by a backup preflight.
type BackupSpaceError struct {
	RequiredBytes  int64 `json:"required_bytes"`
	AvailableBytes int64 `json:"available_bytes"`
}

func newBackupSpaceError(required, available int64) error {
	return &BackupSpaceError{RequiredBytes: required, AvailableBytes: available}
}

func (err *BackupSpaceError) Error() string {
	return fmt.Sprintf("%v: required=%d available=%d", ErrInsufficientBackupSpace, err.RequiredBytes, err.AvailableBytes)
}

func (err *BackupSpaceError) Unwrap() error { return ErrInsufficientBackupSpace }

// BackupCapacity exposes structured bytes to the package-independent admin
// protocol without coupling that helper to maintenance internals.
func (err *BackupSpaceError) BackupCapacity() (required, available int64) {
	return err.RequiredBytes, err.AvailableBytes
}
