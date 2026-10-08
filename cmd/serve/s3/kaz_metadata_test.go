package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/pkg/xattr"
	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/lib/random"

	"github.com/rclone/rclone/cmd/serve/proxy"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKazToProperties checks only X-Amz-Meta-* headers are kept, prefixed
// and lower cased.
func TestKazToProperties(t *testing.T) {
	got := kazToProperties(map[string]string{
		"X-Amz-Meta-Crc32c":    "123",
		"x-amz-meta-Mixed-Key": "v",
		"X-Amz-Date":           "20261008T000000Z",
		"Content-Type":         "text/plain",
		"Last-Modified":        "Thu, 08 Oct 2026 00:00:00 GMT",
		"mtime":                "1.5",
	})
	assert.Equal(t, fs.Metadata{"s3m-crc32c": "123", "s3m-mixed-key": "v"}, got)
	assert.Nil(t, kazToProperties(map[string]string{"X-Amz-Date": "x"}), "no user metadata → nil")
}

// TestKazFromProperties checks only s3m-* entries come back as canonical
// X-Amz-Meta-* headers.
func TestKazFromProperties(t *testing.T) {
	got := kazFromProperties(fs.Metadata{"s3m-crc32c": "123", "s3m-mixed-key": "v", "mtime": "x", "owner": "y"})
	assert.Equal(t, map[string]string{"X-Amz-Meta-Crc32c": "123", "X-Amz-Meta-Mixed-Key": "v"}, got)
}

// TestKazPersistNeedsCacheModeOff checks the server refuses to start with
// --kaz-s3-persist-metadata unless the VFS cache mode is off.
func TestKazPersistNeedsCacheModeOff(t *testing.T) {
	ctx := context.Background()
	f, err := fs.NewFs(ctx, t.TempDir())
	require.NoError(t, err)
	opt := Opt
	opt.HTTP.ListenAddr = []string{endpoint}
	opt.KazPersistMetadata = true
	vfsOpt := vfscommon.Opt
	vfsOpt.CacheMode = vfscommon.CacheModeWrites
	_, err = newServer(ctx, f, &opt, &vfsOpt, &proxy.Opt)
	assert.Error(t, err)

	vfsOpt.CacheMode = vfscommon.CacheModeOff
	w, err := newServer(ctx, f, &opt, &vfsOpt, &proxy.Opt)
	require.NoError(t, err)
	_ = w.Shutdown()
}

var errKazMeta = errors.New("kaz: metadata read failed")

// kazFakeNode is a vfs.Node whose DirEntry is a fixed object; no other method
// is used by kazObjectUserMetadata.
type kazFakeNode struct {
	vfs.Node
	o fs.Object
}

// DirEntry returns the fixed object.
func (n kazFakeNode) DirEntry() fs.DirEntry {
	if n.o == nil {
		return nil
	}
	return n.o
}

// kazFailingMetaObject is an object whose metadata cannot be read.
type kazFailingMetaObject struct{ fs.Object }

// Metadata always fails.
func (kazFailingMetaObject) Metadata(context.Context) (fs.Metadata, error) { return nil, errKazMeta }

// TestKazObjectUserMetadataErrors checks a failure to read metadata is
// returned (HEAD/GET then answer 500) and a node without an object gives none.
func TestKazObjectUserMetadataErrors(t *testing.T) {
	ctx := context.Background()
	_, err := kazObjectUserMetadata(ctx, kazFakeNode{o: kazFailingMetaObject{}})
	assert.True(t, errors.Is(err, errKazMeta), "got %v", err)
	m, err := kazObjectUserMetadata(ctx, kazFakeNode{})
	require.NoError(t, err)
	assert.Empty(t, m)
}

// newKazPersistBackend serves root (a local directory containing "bucket")
// with --kaz-s3-persist-metadata. Each call makes a VFS of its own (distinct
// dirCacheTime), so two backends on one root act as two hosts, or as one
// host before and after a restart.
func newKazPersistBackend(t *testing.T, root string, dirCacheTime time.Duration) *s3Backend {
	t.Helper()
	fstest.Initialise()
	ctx := context.Background()
	f, err := fs.NewFs(ctx, root)
	require.NoError(t, err)
	vfsOpt := vfscommon.Opt
	vfsOpt.CacheMode = vfscommon.CacheModeOff
	vfsOpt.KazLookupByPath = true
	vfsOpt.DirCacheTime = fs.Duration(dirCacheTime)
	vfsOpt.PollInterval = 0
	opt := Opt
	opt.HTTP.ListenAddr = []string{endpoint}
	opt.KazPersistMetadata = true
	w, err := newServer(ctx, f, &opt, &vfsOpt, &proxy.Opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Shutdown() })
	return newBackend(w)
}

// kazRequireXattrs skips when the temp filesystem cannot hold user xattrs.
func kazRequireXattrs(t *testing.T, dir string) {
	t.Helper()
	p := filepath.Join(dir, "kaz-probe")
	require.NoError(t, os.WriteFile(p, []byte("x"), 0666))
	defer func() { _ = os.Remove(p) }()
	if err := kazSetXattr(p); err != nil {
		t.Skipf("user xattrs not supported: %v", err)
	}
}

// TestKazPersistAcrossHosts checks metadata PUT on host A is returned by
// HEAD and GET on A, on host B, and on A after a restart, and that the
// in-memory store stays empty.
func TestKazPersistAcrossHosts(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket"), 0777))
	a := newKazPersistBackend(t, root, time.Hour)
	b := newKazPersistBackend(t, root, time.Hour+time.Second)

	_, err := a.PutObject(ctx, "bucket", "chunks/0/1/1_0_3", map[string]string{"X-Amz-Meta-Crc32c": "777", "X-Amz-Date": "d"}, strings.NewReader("abc"), 3)
	require.NoError(t, err)

	for name, be := range map[string]*s3Backend{"A": a, "B": b, "A-restarted": newKazPersistBackend(t, root, time.Hour+2*time.Second)} {
		obj, err := be.HeadObject(ctx, "bucket", "chunks/0/1/1_0_3")
		require.NoError(t, err, name)
		assert.Equal(t, "777", obj.Metadata["X-Amz-Meta-Crc32c"], name)
		_, hasDate := obj.Metadata["X-Amz-Date"]
		assert.False(t, hasDate, name+": request headers must not be stored")
		obj, err = be.GetObject(ctx, "bucket", "chunks/0/1/1_0_3", nil)
		require.NoError(t, err, name)
		_ = obj.Contents.Close()
		assert.Equal(t, "777", obj.Metadata["X-Amz-Meta-Crc32c"], name)
	}
	n := 0
	a.meta.Range(func(any, any) bool { n++; return true })
	assert.Zero(t, n, "b.meta must stay empty")
}

// TestKazPersistOverwriteReplaces checks a new PUT replaces the metadata:
// keys it does not send are gone.
func TestKazPersistOverwriteReplaces(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket"), 0777))
	a := newKazPersistBackend(t, root, time.Hour)
	_, err := a.PutObject(ctx, "bucket", "k", map[string]string{"X-Amz-Meta-Old": "1", "X-Amz-Meta-Crc32c": "1"}, strings.NewReader("one"), 3)
	require.NoError(t, err)
	_, err = a.PutObject(ctx, "bucket", "k", map[string]string{"X-Amz-Meta-Crc32c": "2"}, strings.NewReader("two"), 3)
	require.NoError(t, err)
	obj, err := newKazPersistBackend(t, root, time.Hour+time.Second).HeadObject(ctx, "bucket", "k")
	require.NoError(t, err)
	assert.Equal(t, "2", obj.Metadata["X-Amz-Meta-Crc32c"])
	_, hasOld := obj.Metadata["X-Amz-Meta-Old"]
	assert.False(t, hasOld)

	// A PUT without user metadata leaves none behind.
	_, err = a.PutObject(ctx, "bucket", "k", map[string]string{}, strings.NewReader("three"), 5)
	require.NoError(t, err)
	obj, err = newKazPersistBackend(t, root, time.Hour+2*time.Second).HeadObject(ctx, "bucket", "k")
	require.NoError(t, err)
	_, hasCrc := obj.Metadata["X-Amz-Meta-Crc32c"]
	assert.False(t, hasCrc)
}

// TestKazPersistLegacyObject checks an object stored without metadata (as by
// an older rclone) can be read, overwritten with metadata, deleted and is
// then gone.
func TestKazPersistLegacyObject(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket", "d"), 0777))
	require.NoError(t, os.WriteFile(filepath.Join(root, "bucket", "d", "old"), []byte("legacy"), 0666))
	a := newKazPersistBackend(t, root, time.Hour)

	obj, err := a.HeadObject(ctx, "bucket", "d/old")
	require.NoError(t, err)
	assert.Equal(t, int64(6), obj.Size)
	_, has := obj.Metadata["X-Amz-Meta-Crc32c"]
	assert.False(t, has)
	obj, err = a.GetObject(ctx, "bucket", "d/old", nil)
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Contents)
	_ = obj.Contents.Close()
	require.NoError(t, err)
	assert.Equal(t, "legacy", string(data))

	_, err = a.PutObject(ctx, "bucket", "d/old", map[string]string{"X-Amz-Meta-Crc32c": "9"}, strings.NewReader("new!"), 4)
	require.NoError(t, err)
	obj, err = a.GetObject(ctx, "bucket", "d/old", nil)
	require.NoError(t, err)
	data, err = io.ReadAll(obj.Contents)
	_ = obj.Contents.Close()
	require.NoError(t, err)
	assert.Equal(t, "new!", string(data))
	assert.Equal(t, "9", obj.Metadata["X-Amz-Meta-Crc32c"])

	require.NoError(t, a.deleteObject(ctx, "bucket", "d/old"))
	_, err = a.HeadObject(ctx, "bucket", "d/old")
	assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey))
	_, err = a.GetObject(ctx, "bucket", "d/old", nil)
	assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey))
}

// TestKazPersistFailedPutKeepsOld checks a PUT whose body fails part way
// leaves the previous object and its metadata in place.
func TestKazPersistFailedPutKeepsOld(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket"), 0777))
	a := newKazPersistBackend(t, root, time.Hour)
	_, err := a.PutObject(ctx, "bucket", "k", map[string]string{"X-Amz-Meta-Crc32c": "1"}, strings.NewReader("one"), 3)
	require.NoError(t, err)
	_, err = a.PutObject(ctx, "bucket", "k", map[string]string{"X-Amz-Meta-Crc32c": "2"}, &errorReader{data: []byte("tw"), err: errBoom}, 3)
	require.Error(t, err)
	obj, err := newKazPersistBackend(t, root, time.Hour+time.Second).GetObject(ctx, "bucket", "k", nil)
	require.NoError(t, err)
	data, _ := io.ReadAll(obj.Contents)
	_ = obj.Contents.Close()
	assert.Equal(t, "one", string(data))
	assert.Equal(t, "1", obj.Metadata["X-Amz-Meta-Crc32c"])
}

// TestKazPersistMultipart checks metadata given when a multipart upload is
// created is stored with the completed object, also after it is moved from
// its temporary name.
func TestKazPersistMultipart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	vfsOpt := vfscommon.Opt
	vfsOpt.CacheMode = vfscommon.CacheModeOff
	core, _, bucket := newMultipartTestServerVFS(t, root, false, func(o *Options) { o.KazPersistMetadata = true }, &vfsOpt)
	uploadID, err := core.NewMultipartUpload(ctx, bucket, "mp", minio.PutObjectOptions{UserMetadata: map[string]string{"crc32c": "55"}})
	require.NoError(t, err)
	data := []byte(random.String(1024))
	p, err := core.PutObjectPart(ctx, bucket, "mp", uploadID, 1, bytes.NewReader(data), int64(len(data)), minio.PutObjectPartOptions{})
	require.NoError(t, err)
	_, err = core.CompleteMultipartUpload(ctx, bucket, "mp", uploadID, []minio.CompletePart{{PartNumber: 1, ETag: p.ETag}}, minio.PutObjectOptions{})
	require.NoError(t, err)

	obj, err := newKazPersistBackend(t, root, time.Hour+3*time.Second).HeadObject(ctx, bucket, "mp")
	require.NoError(t, err)
	assert.Equal(t, "55", obj.Metadata["X-Amz-Meta-Crc32c"])
}

// TestKazPersistCopySameKeyNotImplemented checks a metadata-only copy onto
// the same key is refused instead of being silently ignored.
func TestKazPersistCopySameKeyNotImplemented(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket"), 0777))
	a := newKazPersistBackend(t, root, time.Hour)
	_, err := a.PutObject(ctx, "bucket", "k", map[string]string{}, strings.NewReader("one"), 3)
	require.NoError(t, err)
	_, err = a.CopyObject(ctx, "bucket", "k", "bucket", "k", map[string]string{"X-Amz-Meta-A": "b"})
	assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNotImplemented), "got %v", err)
}

// kazSetXattr writes a user xattr on path, to probe xattr support.
func kazSetXattr(path string) error {
	return xattr.Set(path, "user.kaz-probe", []byte("1"))
}
