//go:build darwin

package storage

func backupDescriptorPath(validatedPath string, _ uintptr) string {
	return validatedPath
}
