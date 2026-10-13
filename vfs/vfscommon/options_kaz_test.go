package vfscommon

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKazLookupByPathOption checks the kaz lookup option is registered as an
// opt-in VFS flag that is clearly marked as a kaz fork extension.
func TestKazLookupByPathOption(t *testing.T) {
	var found bool
	for _, o := range OptionsInfo {
		if o.Name != "kaz_vfs_lookup_by_path" {
			continue
		}
		found = true
		assert.Equal(t, false, o.Default)
		assert.True(t, strings.HasPrefix(o.Help, "[kaz] "), "help must start with [kaz]")
		assert.Contains(t, strings.Split(o.Groups, ","), "Kaz")
		assert.Contains(t, strings.Split(o.Groups, ","), "VFS")
	}
	require.True(t, found, "kaz_vfs_lookup_by_path must be in OptionsInfo")
	assert.False(t, Opt.KazLookupByPath, "default must be off")
}
