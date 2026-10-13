//go:build unix

package nfs

import (
	"io"
	"os"
	"strings"
	"testing"

	_ "github.com/rclone/rclone/backend/memory"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKazPathTraversalLookup is TestPathTraversal with --kaz-vfs-lookup-by-path
// and both cache modes: the lookup and O_TRUNC create shortcuts must not let a
// ".." name reach outside the directory served.
func TestKazPathTraversalLookup(t *testing.T) {
	for _, mode := range []vfscommon.CacheMode{vfscommon.CacheModeOff, vfscommon.CacheModeWrites} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx := t.Context()
			modTime := fstest.Time("2001-02-03T04:05:06.499999999Z")
			remote := ":memory:nfs-kaz-traversal-" + mode.String()

			outside, err := fs.NewFs(ctx, remote)
			require.NoError(t, err)
			_, err = operations.Rcat(ctx, outside, "outside-secret.txt", io.NopCloser(strings.NewReader("SECRET")), modTime, nil)
			require.NoError(t, err)
			f, err := fs.NewFs(ctx, remote+"/served-root")
			require.NoError(t, err)
			_, err = operations.Rcat(ctx, f, "inside.txt", io.NopCloser(strings.NewReader("INSIDE")), modTime, nil)
			require.NoError(t, err)

			opt := vfscommon.Opt
			opt.CacheMode = mode
			opt.KazLookupByPath = true
			v := vfs.New(ctx, f, &opt)
			defer v.Shutdown()
			bfs := &FS{vfs: v}

			_, err = bfs.OpenFile(bfs.Join("", ".."), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
			assert.Error(t, err)
			_, err = bfs.OpenFile(bfs.Join("", "../outside-write.txt"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
			assert.Error(t, err)
			_, err = bfs.Create(bfs.Join("", "../outside-write.txt"))
			assert.Error(t, err)
			_, err = bfs.Open(bfs.Join("", "../outside-secret.txt"))
			assert.Error(t, err)
			_, err = bfs.Stat(bfs.Join("", ".."))
			assert.Error(t, err)
			assert.Error(t, bfs.Remove(bfs.Join("", "../outside-secret.txt")))

			fstest.CheckListingWithPrecision(t, outside, []fstest.Item{
				fstest.NewItem("outside-secret.txt", "SECRET", modTime),
				fstest.NewItem("served-root/inside.txt", "INSIDE", modTime),
			}, []string{"served-root"}, fs.GetModifyWindow(ctx, outside))
		})
	}
}
