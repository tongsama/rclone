package flags

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestKazGroupRegistered checks the Kaz flag group exists so kaz fork flags
// can be listed together.
func TestKazGroupRegistered(t *testing.T) {
	g, ok := All.ByName["Kaz"]
	assert.True(t, ok)
	if ok {
		assert.Contains(t, g.Help, "kaz")
	}
}
