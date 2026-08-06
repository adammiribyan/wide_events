//go:build darwin

package maintenance

import "golang.org/x/sys/unix"

func renameNoReplace(directoryFD int, _ string, oldName, newName string) error {
	return unix.RenameatxNp(directoryFD, oldName, directoryFD, newName, unix.RENAME_EXCL)
}
