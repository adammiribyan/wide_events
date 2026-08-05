//go:build unix

package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// StatFSProbe reads filesystem capacity with statfs(2).
type StatFSProbe struct{}

// Space probes path, or its nearest existing parent when the final path does
// not exist yet.
func (StatFSProbe) Space(path string) (Space, error) {
	if path == "" {
		return Space{}, fmt.Errorf("disk probe path must not be empty")
	}
	resolved, err := filepath.Abs(path)
	if err != nil {
		return Space{}, fmt.Errorf("resolve disk probe path: %w", err)
	}
	for {
		_, err = os.Stat(resolved)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return Space{}, fmt.Errorf("stat disk probe path: %w", err)
		}
		parent := filepath.Dir(resolved)
		if parent == resolved {
			return Space{}, fmt.Errorf("find existing disk probe parent for %q", path)
		}
		resolved = parent
	}

	var stats syscall.Statfs_t
	if err := syscall.Statfs(resolved, &stats); err != nil {
		return Space{}, fmt.Errorf("probe filesystem capacity: %w", err)
	}
	blockSize := int64(stats.Bsize)
	return validate(Space{
		Total: saturatingProduct(int64(stats.Blocks), blockSize),
		Free:  saturatingProduct(int64(stats.Bavail), blockSize),
	})
}

func saturatingProduct(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if left > maxInt64/right {
		return maxInt64
	}
	return left * right
}
