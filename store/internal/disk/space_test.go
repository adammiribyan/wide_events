package disk

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindingLimitsUseVolumeSize(t *testing.T) {
	assert.Equal(t, int64(2<<30), WriteFloor(10<<30))
	assert.Equal(t, int64(5<<30), WriteFloor(50<<30))
	assert.Equal(t, int64(2<<30), QuerySpillLimit(8<<30))
	assert.Equal(t, int64(4<<30), QuerySpillLimit(100<<30))
}

func TestStatFSProbeAcceptsAPathThatDoesNotExistYet(t *testing.T) {
	space, err := (StatFSProbe{}).Space(filepath.Join(t.TempDir(), "new.duckdb"))
	require.NoError(t, err)
	assert.Positive(t, space.Total)
	assert.Positive(t, space.Free)
	assert.LessOrEqual(t, space.Free, space.Total)
}
