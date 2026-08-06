//go:build linux

package storage

import (
	"fmt"
)

func backupDescriptorPath(_ string, descriptor uintptr) string {
	return fmt.Sprintf("/proc/self/fd/%d", descriptor)
}
