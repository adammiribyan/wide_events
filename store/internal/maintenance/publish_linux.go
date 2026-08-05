//go:build linux

package maintenance

import "golang.org/x/sys/unix"

func renameNoReplace(directoryFD int, _ string, oldName, newName string) error {
	return unix.Renameat2(directoryFD, oldName, directoryFD, newName, unix.RENAME_NOREPLACE)
}
