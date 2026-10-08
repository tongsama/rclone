package s3

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/gofakes3"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/cmd/serve/proxy"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingFs wraps an fs.Fs and, while fail is set, makes List and NewObject
// return errKaz like a remote rate limit would.
type failingFs struct {
	fs.Fs
	fail atomic.Bool
}

var errKaz = errors.New("kaz: simulated remote failure")

// List fails while fail is set, otherwise delegates.
func (f *failingFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if f.fail.Load() {
		return nil, errKaz
	}
	return f.Fs.List(ctx, dir)
}

// NewObject fails while fail is set, otherwise delegates.
func (f *failingFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if f.fail.Load() {
		return nil, errKaz
	}
	return f.Fs.NewObject(ctx, remote)
}

// newKazBackend serves a fresh local directory containing "bucket" through a
// failingFs. lookup sets --kaz-vfs-lookup-by-path; tweak may change the serve
// options. Files written straight to root/bucket act as another host sharing
// the remote.
func newKazBackend(t *testing.T, lookup bool, tweak func(*Options)) (*s3Backend, *failingFs, string) {
	fstest.Initialise()
	ctx := context.Background()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket"), 0777))
	base, err := fs.NewFs(ctx, root)
	require.NoError(t, err)
	ff := &failingFs{Fs: base}

	vfsOpt := vfscommon.Opt
	vfsOpt.KazLookupByPath = lookup
	vfsOpt.DirCacheTime = fs.Duration(time.Hour)
	vfsOpt.PollInterval = 0
	opt := Opt
	opt.HTTP.ListenAddr = []string{endpoint}
	if tweak != nil {
		tweak(&opt)
	}
	w, err := newServer(ctx, ff, &opt, &vfsOpt, &proxy.Opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Shutdown() })
	return newBackend(w), ff, root
}

// TestKazStatErrorsAreNot404 checks only "not found" becomes NoSuchKey or
// NoSuchBucket and other remote errors are passed through, with and without
// lookup mode.
func TestKazStatErrorsAreNot404(t *testing.T) {
	ctx := context.Background()
	for _, lookup := range []bool{false, true} {
		t.Run(map[bool]string{false: "list", true: "lookup"}[lookup], func(t *testing.T) {
			b, ff, root := newKazBackend(t, lookup, nil)
			require.NoError(t, os.WriteFile(filepath.Join(root, "bucket", "obj"), []byte("x"), 0666))

			_, err := b.HeadObject(ctx, "bucket", "missing")
			assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey), "missing key: %v", err)
			_, err = b.HeadObject(ctx, "nobucket", "obj")
			assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket), "missing bucket: %v", err)
			exists, err := b.BucketExists(ctx, "nobucket")
			assert.NoError(t, err)
			assert.False(t, exists)

			// Make every remote call fail and forget what the VFS cached.
			ff.fail.Store(true)
			v, err := b.s.getVFS(ctx)
			require.NoError(t, err)
			root2, err := v.Root()
			require.NoError(t, err)
			root2.ForgetAll()

			_, err = b.HeadObject(ctx, "bucket", "obj")
			assert.ErrorIs(t, err, errKaz, "HEAD")
			_, err = b.GetObject(ctx, "bucket", "obj", nil)
			assert.ErrorIs(t, err, errKaz, "GET")
			err = b.deleteObject(ctx, "bucket", "obj")
			assert.ErrorIs(t, err, errKaz, "DELETE")
			_, err = b.BucketExists(ctx, "bucket")
			assert.ErrorIs(t, err, errKaz, "BucketExists")
		})
	}
}
