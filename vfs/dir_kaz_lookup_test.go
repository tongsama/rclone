package vfs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingFs wraps an fs.Fs to count directory listings and, when
// newObjectErr is set, to make NewObject fail like a remote error would.
type countingFs struct {
	fs.Fs
	lists        atomic.Int64
	newObjectErr error // set before the VFS is used; read only afterwards
}

// List counts the listing and delegates to the wrapped Fs.
func (c *countingFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	c.lists.Add(1)
	return c.Fs.List(ctx, dir)
}

// NewObject returns newObjectErr if set, otherwise delegates.
func (c *countingFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if c.newObjectErr != nil {
		return nil, c.newObjectErr
	}
	return c.Fs.NewObject(ctx, remote)
}

// newLookupVFS makes a VFS over a counting wrapper of the test remote with
// the kaz lookup mode set to lookup and the given dir cache time. Writes made
// with r.WriteObject act as another client sharing the remote.
func newLookupVFS(t *testing.T, lookup bool, dirCacheTime time.Duration) (*fstest.Run, *countingFs, *VFS) {
	r := fstest.NewRun(t)
	cf := &countingFs{Fs: r.Fremote}
	opt := vfscommon.Opt
	opt.KazLookupByPath = lookup
	opt.DirCacheTime = fs.Duration(dirCacheTime)
	opt.PollInterval = 0
	v := New(context.Background(), cf, &opt)
	t.Cleanup(func() { cleanupVFS(t, v) })
	return r, cf, v
}

var errKazBoom = errors.New("kaz boom")

// TestKazLookupSeesExternalCreate checks an object created by another client
// after the directory was cached is found, without listing any directory.
func TestKazLookupSeesExternalCreate(t *testing.T) {
	r, cf, v := newLookupVFS(t, true, time.Hour)
	ctx := context.Background()
	r.WriteObject(ctx, "dir/a", "aaa", t1)
	_, err := v.Stat("dir/a")
	require.NoError(t, err)

	r.WriteObject(ctx, "dir/b", "bbbb", t1)
	node, err := v.Stat("dir/b")
	require.NoError(t, err)
	assert.True(t, node.IsFile())
	assert.Equal(t, int64(4), node.Size())
	assert.Equal(t, int64(0), cf.lists.Load(), "lookup mode must not list directories")
}

// TestKazLookupDoesNotCacheMiss checks a miss is not remembered, so an object
// created after the miss becomes visible on the next Stat.
func TestKazLookupDoesNotCacheMiss(t *testing.T) {
	r, _, v := newLookupVFS(t, true, time.Hour)
	ctx := context.Background()
	r.WriteObject(ctx, "dir/a", "aaa", t1)
	_, err := v.Stat("dir/c")
	assert.True(t, errors.Is(err, ENOENT))

	r.WriteObject(ctx, "dir/c", "c", t1)
	_, err = v.Stat("dir/c")
	assert.NoError(t, err)
}

// TestKazLookupDirectory checks intermediate directories are resolved through
// ErrorIsDir and cached as directories.
func TestKazLookupDirectory(t *testing.T) {
	r, cf, v := newLookupVFS(t, true, time.Hour)
	r.WriteObject(context.Background(), "a/b/c/file", "x", t1)
	node, err := v.Stat("a/b")
	require.NoError(t, err)
	assert.True(t, node.IsDir())
	node, err = v.Stat("a/b/c/file")
	require.NoError(t, err)
	assert.True(t, node.IsFile())
	assert.Equal(t, int64(0), cf.lists.Load())
}

// TestKazLookupError checks a remote error is returned as is instead of
// being turned into "not found".
func TestKazLookupError(t *testing.T) {
	r, cf, v := newLookupVFS(t, true, time.Hour)
	r.WriteObject(context.Background(), "dir/a", "aaa", t1)
	cf.newObjectErr = errKazBoom
	_, err := v.Stat("dir/a")
	assert.True(t, errors.Is(err, errKazBoom), "got %v", err)
	assert.False(t, errors.Is(err, ENOENT))
}

// TestKazLookupOffUnchanged checks the upstream behaviour is kept when the
// option is off: within the dir cache time an external create is not seen.
func TestKazLookupOffUnchanged(t *testing.T) {
	r, cf, v := newLookupVFS(t, false, time.Hour)
	ctx := context.Background()
	r.WriteObject(ctx, "dir/a", "aaa", t1)
	_, err := v.Stat("dir/a")
	require.NoError(t, err)
	r.WriteObject(ctx, "dir/b", "b", t1)
	_, err = v.Stat("dir/b")
	assert.True(t, errors.Is(err, ENOENT))
	assert.Greater(t, cf.lists.Load(), int64(0))
}

// TestKazLookupListingStillWorks checks a directory listing after lookups
// returns every object once, including ones created by another client.
func TestKazLookupListingStillWorks(t *testing.T) {
	r, cf, v := newLookupVFS(t, true, time.Hour)
	ctx := context.Background()
	r.WriteObject(ctx, "dir/a", "aaa", t1)
	_, err := v.Stat("dir/a")
	require.NoError(t, err)
	r.WriteObject(ctx, "dir/b", "bb", t1)

	node, err := v.Stat("dir")
	require.NoError(t, err)
	nodes, err := node.(*Dir).ReadDirAll()
	require.NoError(t, err)
	var names []string
	for _, n := range nodes {
		names = append(names, n.Name())
	}
	assert.Equal(t, []string{"a", "b"}, names)
	assert.Greater(t, cf.lists.Load(), int64(0), "ReadDirAll must still list")
}

// TestKazLookupConcurrent checks concurrent lookups of one name end up with a
// single cached node.
func TestKazLookupConcurrent(t *testing.T) {
	r, _, v := newLookupVFS(t, true, time.Hour)
	r.WriteObject(context.Background(), "dir/a", "aaa", t1)
	const n = 20
	nodes := make([]Node, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node, err := v.Stat("dir/a")
			assert.NoError(t, err)
			nodes[i] = node
		}()
	}
	wg.Wait()
	again, err := v.Stat("dir/a")
	require.NoError(t, err)
	for i := range n {
		assert.Same(t, again, nodes[i])
	}
}

// dirItemCount returns how many entries dir currently caches.
func dirItemCount(d *Dir) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.items)
}

// TestKazLookupEntriesExpire checks entries added by lookup are dropped after
// about twice the dir cache time, and that this repeats on the same Dir after
// a cleanup, so a deletion by another client is eventually seen.
//
// It uses the root directory because the root Dir is never replaced. Both
// rounds are armed by the lookup; the timer set when the Dir is created only
// explains why round 0 would also pass without arming, so round 1 is the one
// that proves the timer is armed again.
func TestKazLookupEntriesExpire(t *testing.T) {
	r, _, v := newLookupVFS(t, true, 200*time.Millisecond)
	ctx := context.Background()
	obj := r.WriteObject(ctx, "a", "aaa", t1)
	root, err := v.Root()
	require.NoError(t, err)

	for round := range 2 {
		_, err = v.Stat("a")
		require.NoError(t, err, "round %d", round)
		require.Equal(t, 1, dirItemCount(root), "round %d", round)
		require.Eventually(t, func() bool { return dirItemCount(root) == 0 },
			3*time.Second, 20*time.Millisecond, "round %d: lookup entry must expire", round)
	}

	// Removed by another client: once expired the lookup reports not found.
	o, err := r.Fremote.NewObject(ctx, obj.Path)
	require.NoError(t, err)
	require.NoError(t, o.Remove(ctx))
	_, err = v.Stat("a")
	assert.True(t, errors.Is(err, ENOENT))
}

// TestKazRemoveAlreadyGone checks removing a cached file that another client
// already deleted succeeds and drops the stale entry.
func TestKazRemoveAlreadyGone(t *testing.T) {
	r, _, v := newLookupVFS(t, true, time.Hour)
	ctx := context.Background()
	obj := r.WriteObject(ctx, "dir/a", "aaa", t1)
	_, err := v.Stat("dir/a")
	require.NoError(t, err)

	o, err := r.Fremote.NewObject(ctx, obj.Path)
	require.NoError(t, err)
	require.NoError(t, o.Remove(ctx))

	require.NoError(t, v.Remove("dir/a"))
	_, err = v.Stat("dir/a")
	assert.True(t, errors.Is(err, ENOENT))
}

// TestKazLookupErrorAtLeaf checks a remote error while resolving the last path
// element is returned as is and leaves nothing cached under that name.
func TestKazLookupErrorAtLeaf(t *testing.T) {
	r, cf, v := newLookupVFS(t, true, time.Hour)
	r.WriteObject(context.Background(), "dir/a", "aaa", t1)
	node, err := v.Stat("dir/a")
	require.NoError(t, err)
	require.True(t, node.IsFile())
	dirNode, err := v.Stat("dir")
	require.NoError(t, err)
	dir := dirNode.(*Dir)

	cf.newObjectErr = errKazBoom
	_, err = v.Stat("dir/x")
	cf.newObjectErr = nil
	assert.True(t, errors.Is(err, errKazBoom), "got %v", err)
	assert.False(t, errors.Is(err, ENOENT))
	assert.Equal(t, 1, dirItemCount(dir), "failed lookup must not be cached")
}

// TestKazLookupBusyDirExpires checks lookups arriving more often than the
// expiry do not postpone the cleanup: entries still get dropped while the
// directory stays busy.
func TestKazLookupBusyDirExpires(t *testing.T) {
	r, _, v := newLookupVFS(t, true, 200*time.Millisecond)
	ctx := context.Background()
	const n = 15
	for i := range n {
		r.WriteObject(ctx, fmt.Sprintf("n%02d", i), "x", t1)
	}
	root, err := v.Root()
	require.NoError(t, err)

	dropped := false
	for i := range n {
		_, err := v.Stat(fmt.Sprintf("n%02d", i))
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		if dirItemCount(root) < i+1 {
			dropped = true
		}
	}
	assert.True(t, dropped, "a cleanup must run although lookups keep arriving")
}

// TestKazRemoveKeepsErrorWhenUnsure checks Remove keeps the backend error when
// the follow-up check cannot confirm the object is gone.
func TestKazRemoveKeepsErrorWhenUnsure(t *testing.T) {
	r, cf, v := newLookupVFS(t, true, time.Hour)
	ctx := context.Background()
	obj := r.WriteObject(ctx, "dir/a", "aaa", t1)
	_, err := v.Stat("dir/a")
	require.NoError(t, err)

	o, err := r.Fremote.NewObject(ctx, obj.Path)
	require.NoError(t, err)
	require.NoError(t, o.Remove(ctx))

	cf.newObjectErr = errKazBoom
	err = v.Remove("dir/a")
	cf.newObjectErr = nil
	assert.Error(t, err)
}
