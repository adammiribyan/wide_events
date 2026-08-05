// Package disk provides the binding disk-space limits used by the store.
package disk

import "fmt"

const (
	minimumWriteFloor = int64(2 << 30)
	maximumQuerySpill = int64(4 << 30)
)

// Space is a filesystem capacity snapshot in bytes.
type Space struct {
	Total int64
	Free  int64
}

// Probe reads capacity for the filesystem containing path.
type Probe interface {
	Space(path string) (Space, error)
}

// WriteFloor reserves the greater of 2 GiB and ten percent of a volume.
func WriteFloor(total int64) int64 {
	floor := total / 10
	if floor < minimumWriteFloor {
		return minimumWriteFloor
	}
	return floor
}

// QuerySpillLimit caps DuckDB spill at the lesser of 4 GiB and 25% of a
// volume. It returns zero for an invalid volume size.
func QuerySpillLimit(total int64) int64 {
	if total <= 0 {
		return 0
	}
	limit := total / 4
	if limit > maximumQuerySpill {
		return maximumQuerySpill
	}
	return limit
}

func validate(space Space) (Space, error) {
	if space.Total <= 0 || space.Free < 0 || space.Free > space.Total {
		return Space{}, fmt.Errorf("invalid filesystem capacity total=%d free=%d", space.Total, space.Free)
	}
	return space, nil
}
