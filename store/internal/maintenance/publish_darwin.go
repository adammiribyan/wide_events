//go:build darwin

package maintenance

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

func renameNoReplace(_ int, directory, oldName, newName string) error {
	return unix.RenamexNp(filepath.Join(directory, oldName), filepath.Join(directory, newName), unix.RENAME_EXCL)
}
