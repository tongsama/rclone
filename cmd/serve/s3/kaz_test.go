package s3

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
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
// return errKaz like a remote rate limit would. It also counts the List calls
// and, when onList is set, calls it before each List and returns its error.
// onList must be set before the backend is used.
type failingFs struct {
	fs.Fs
	fail   atomic.Bool
	lists  atomic.Int64
	onList func(dir string) error
}

var errKaz = errors.New("kaz: simulated remote failure")

// List fails while fail is set or onList fails, otherwise delegates.
func (f *failingFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	f.lists.Add(1)
	if f.fail.Load() {
		return nil, errKaz
	}
	if f.onList != nil {
		if err := f.onList(dir); err != nil {
			return nil, err
		}
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

// TestKazDeleteRemovesMeta checks DELETE drops the in-memory user metadata so
// it does not grow without bound.
func TestKazDeleteRemovesMeta(t *testing.T) {
	ctx := context.Background()
	b, _, _ := newKazBackend(t, true, nil)
	_, err := b.PutObject(ctx, "bucket", "d/obj", map[string]string{"X-Amz-Meta-Crc32c": "AAAAAA=="}, strings.NewReader("abc"), 3)
	require.NoError(t, err)
	fp, err := bucketObjectPath("bucket", "d/obj")
	require.NoError(t, err)
	_, ok := b.meta.Load(fp)
	require.True(t, ok)

	require.NoError(t, b.deleteObject(ctx, "bucket", "d/obj"))
	_, ok = b.meta.Load(fp)
	assert.False(t, ok, "meta must be dropped on delete")
}

// TestNoCleanupKeepsParents checks --no-cleanup keeps the emptied parent
// directories and that the default still removes them.
func TestNoCleanupKeepsParents(t *testing.T) {
	ctx := context.Background()
	for _, noCleanup := range []bool{false, true} {
		b, _, root := newKazBackend(t, true, func(o *Options) { o.NoCleanup = noCleanup })
		_, err := b.PutObject(ctx, "bucket", "d1/d2/obj", map[string]string{}, strings.NewReader("abc"), 3)
		require.NoError(t, err)
		require.NoError(t, b.deleteObject(ctx, "bucket", "d1/d2/obj"))
		_, statErr := os.Stat(filepath.Join(root, "bucket", "d1", "d2"))
		if noCleanup {
			assert.NoError(t, statErr, "--no-cleanup must keep the parent")
		} else {
			assert.True(t, os.IsNotExist(statErr), "default must remove the empty parent")
		}
	}
}

// TestKazOtherHostVisible checks objects written and deleted directly on the
// shared remote (another host) are seen by HEAD, GET and DELETE in lookup
// mode, even though the directory is already cached.
func TestKazOtherHostVisible(t *testing.T) {
	ctx := context.Background()
	b, _, root := newKazBackend(t, true, nil)
	dir := filepath.Join(root, "bucket", "chunks", "0", "4")
	require.NoError(t, os.MkdirAll(dir, 0777))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "4000_0_3"), []byte("abc"), 0666))
	_, err := b.HeadObject(ctx, "bucket", "chunks/0/4/4000_0_3")
	require.NoError(t, err)

	// Another host writes a new object into the cached directory.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "4096_0_5"), []byte("hello"), 0666))
	obj, err := b.HeadObject(ctx, "bucket", "chunks/0/4/4096_0_5")
	require.NoError(t, err)
	assert.Equal(t, int64(5), obj.Size)
	obj, err = b.GetObject(ctx, "bucket", "chunks/0/4/4096_0_5", nil)
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Contents)
	require.NoError(t, err)
	_ = obj.Contents.Close()
	assert.Equal(t, "hello", string(data))

	// DELETE removes it from the remote, then it is gone.
	require.NoError(t, b.deleteObject(ctx, "bucket", "chunks/0/4/4096_0_5"))
	_, statErr := os.Stat(filepath.Join(dir, "4096_0_5"))
	assert.True(t, os.IsNotExist(statErr), "DELETE must remove the object on the remote")
	_, err = b.HeadObject(ctx, "bucket", "chunks/0/4/4096_0_5")
	assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey))

	// Deleted by another host first: DELETE still succeeds.
	require.NoError(t, os.Remove(filepath.Join(dir, "4000_0_3")))
	assert.NoError(t, b.deleteObject(ctx, "bucket", "chunks/0/4/4000_0_3"))
}

// TestKazGetObjectDeletedElsewhere checks a GET of an object still cached
// here but deleted by another host fails instead of returning data or 404.
func TestKazGetObjectDeletedElsewhere(t *testing.T) {
	ctx := context.Background()
	b, _, root := newKazBackend(t, true, nil)
	p := filepath.Join(root, "bucket", "obj")
	require.NoError(t, os.WriteFile(p, []byte("abc"), 0666))
	_, err := b.HeadObject(ctx, "bucket", "obj")
	require.NoError(t, err)
	require.NoError(t, os.Remove(p))

	obj, err := b.GetObject(ctx, "bucket", "obj", nil)
	if err == nil {
		_, err = io.ReadAll(obj.Contents)
		_ = obj.Contents.Close()
	}
	assert.Error(t, err)
	assert.False(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey), "must not look like a clean 404: %v", err)
}

// TestKazPutCreatesParents checks a PUT into directories that do not exist
// yet works in lookup mode and the object is visible right away.
func TestKazPutCreatesParents(t *testing.T) {
	ctx := context.Background()
	b, _, root := newKazBackend(t, true, nil)
	_, err := b.PutObject(ctx, "bucket", "chunks/0/9/9000_0_4", map[string]string{}, strings.NewReader("data"), 4)
	require.NoError(t, err)
	obj, err := b.HeadObject(ctx, "bucket", "chunks/0/9/9000_0_4")
	require.NoError(t, err)
	assert.Equal(t, int64(4), obj.Size)
	got, err := os.ReadFile(filepath.Join(root, "bucket", "chunks", "0", "9", "9000_0_4"))
	require.NoError(t, err)
	assert.Equal(t, "data", string(got))
}

// TestKazBucketStatErrorsElsewhereAreNot404 checks LIST, PUT and bucket
// DELETE also report NoSuchBucket only when the bucket does not exist and
// pass other remote errors through.
func TestKazBucketStatErrorsElsewhereAreNot404(t *testing.T) {
	ctx := context.Background()
	for _, lookup := range []bool{false, true} {
		t.Run(map[bool]string{false: "list", true: "lookup"}[lookup], func(t *testing.T) {
			b, ff, _ := newKazBackend(t, lookup, nil)

			_, err := b.ListBucket(ctx, "nobucket", nil, gofakes3.ListBucketPage{})
			assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket), "LIST missing: %v", err)
			_, err = b.PutObject(ctx, "nobucket", "k", map[string]string{}, strings.NewReader("x"), 1)
			assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket), "PUT missing: %v", err)
			err = b.DeleteBucket(ctx, "nobucket")
			assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchBucket), "DELETE bucket missing: %v", err)

			ff.fail.Store(true)
			v, err := b.s.getVFS(ctx)
			require.NoError(t, err)
			root, err := v.Root()
			require.NoError(t, err)
			root.ForgetAll()

			_, err = b.ListBucket(ctx, "bucket", nil, gofakes3.ListBucketPage{})
			assert.ErrorIs(t, err, errKaz, "LIST")
			_, err = b.PutObject(ctx, "bucket", "k", map[string]string{}, strings.NewReader("x"), 1)
			assert.ErrorIs(t, err, errKaz, "PUT")
			err = b.DeleteBucket(ctx, "bucket")
			assert.ErrorIs(t, err, errKaz, "DELETE bucket")
		})
	}
}

// TestKazKeyStatErrorIsNot404 checks that in lookup mode a remote failure while
// resolving a key is not reported as NoSuchKey when the bucket itself is cached.
func TestKazKeyStatErrorIsNot404(t *testing.T) {
	ctx := context.Background()
	b, ff, _ := newKazBackend(t, true, nil)

	_, err := b.HeadObject(ctx, "bucket", "obj-missing")
	require.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey), "missing key: %v", err)

	// The bucket is served from cache; only the key lookup reaches the remote.
	ff.fail.Store(true)
	defer ff.fail.Store(false)
	_, err = b.HeadObject(ctx, "bucket", "other")
	assert.ErrorIs(t, err, errKaz, "HEAD")
	_, err = b.GetObject(ctx, "bucket", "other", nil)
	assert.ErrorIs(t, err, errKaz, "GET")
}
