package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
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
	return w.backend
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
	assert.Zero(t, a.tenant(ctx).meta.Len(), "tenant metadata must stay empty")
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
	data, err := io.ReadAll(obj.Contents)
	require.NoError(t, err)
	_ = obj.Contents.Close()
	assert.Equal(t, "one", string(data))
	assert.Equal(t, "1", obj.Metadata["X-Amz-Meta-Crc32c"])
}

// TestKazPersistMtime checks X-Amz-Meta-Mtime is stored as user metadata
// like any other key and does not change the object's modification time
// seen through another host.
func TestKazPersistMtime(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket"), 0777))
	a := newKazPersistBackend(t, root, time.Hour)
	_, err := a.PutObject(ctx, "bucket", "k", map[string]string{"X-Amz-Meta-Mtime": "1700000000.5", "X-Amz-Meta-Crc32c": "3"}, strings.NewReader("abc"), 3)
	require.NoError(t, err)
	obj, err := newKazPersistBackend(t, root, time.Hour+time.Second).HeadObject(ctx, "bucket", "k")
	require.NoError(t, err)
	assert.Equal(t, "3", obj.Metadata["X-Amz-Meta-Crc32c"])
	assert.Equal(t, "1700000000.5", obj.Metadata["X-Amz-Meta-Mtime"])
	mt, err := http.ParseTime(obj.Metadata["Last-Modified"])
	require.NoError(t, err)
	assert.True(t, time.Unix(1700000000, 0).Equal(mt), "modtime %v", mt)
	assert.Zero(t, a.tenant(ctx).meta.Len(), "tenant metadata must stay empty")
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

// kazPersistMultipartServer starts a server with --kaz-s3-persist-metadata
// and --vfs-cache-mode off on root, returning the client and bucket name.
func kazPersistMultipartServer(t *testing.T, root string) (*minio.Core, string) {
	t.Helper()
	vfsOpt := vfscommon.Opt
	vfsOpt.CacheMode = vfscommon.CacheModeOff
	core, _, bucket := newMultipartTestServerVFS(t, root, false, func(o *Options) { o.KazPersistMetadata = true }, &vfsOpt)
	return core, bucket
}

// TestKazPersistMultipartOutOfOrder checks the metadata reaches the object
// when a later part arrives first and is buffered before the sink exists.
func TestKazPersistMultipartOutOfOrder(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	core, bucket := kazPersistMultipartServer(t, root)
	uploadID, err := core.NewMultipartUpload(ctx, bucket, "mp", minio.PutObjectOptions{UserMetadata: map[string]string{"crc32c": "42"}})
	require.NoError(t, err)
	part1 := []byte(random.String(5 * 1024 * 1024))
	part2 := []byte("tail")
	p2, err := core.PutObjectPart(ctx, bucket, "mp", uploadID, 2, bytes.NewReader(part2), int64(len(part2)), minio.PutObjectPartOptions{})
	require.NoError(t, err)
	p1, err := core.PutObjectPart(ctx, bucket, "mp", uploadID, 1, bytes.NewReader(part1), int64(len(part1)), minio.PutObjectPartOptions{})
	require.NoError(t, err)
	_, err = core.CompleteMultipartUpload(ctx, bucket, "mp", uploadID, []minio.CompletePart{{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag}}, minio.PutObjectOptions{})
	require.NoError(t, err)

	obj, err := newKazPersistBackend(t, root, time.Hour+3*time.Second).HeadObject(ctx, bucket, "mp")
	require.NoError(t, err)
	assert.Equal(t, "42", obj.Metadata["X-Amz-Meta-Crc32c"])
	assert.Equal(t, int64(len(part1)+len(part2)), obj.Size)
}

// TestKazPersistMultipartNoParts checks completing an upload with no parts
// creates an empty object which still carries the metadata.
func TestKazPersistMultipartNoParts(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	core, bucket := kazPersistMultipartServer(t, root)
	uploadID, err := core.NewMultipartUpload(ctx, bucket, "empty", minio.PutObjectOptions{UserMetadata: map[string]string{"crc32c": "0"}})
	require.NoError(t, err)
	_, err = core.CompleteMultipartUpload(ctx, bucket, "empty", uploadID, nil, minio.PutObjectOptions{})
	require.NoError(t, err)

	obj, err := newKazPersistBackend(t, root, time.Hour+3*time.Second).HeadObject(ctx, bucket, "empty")
	require.NoError(t, err)
	assert.Equal(t, "0", obj.Metadata["X-Amz-Meta-Crc32c"])
	assert.Zero(t, obj.Size)
}

// TestKazPersistMultipartCreateMakesNothing checks that, as upstream
// requires, creating an upload with --kaz-s3-persist-metadata makes nothing
// on the remote until the first part arrives.
func TestKazPersistMultipartCreateMakesNothing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	core, bucket := kazPersistMultipartServer(t, root)
	_, err := core.NewMultipartUpload(ctx, bucket, "d/mp", minio.PutObjectOptions{UserMetadata: map[string]string{"crc32c": "1"}})
	require.NoError(t, err)
	var files []string
	require.NoError(t, filepath.WalkDir(filepath.Join(root, bucket), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			files = append(files, p)
		}
		return err
	}))
	assert.Empty(t, files, "no object may exist before the first part")
}

// TestKazPersistCopySameKey checks a metadata-only copy is refused with 501
// and a copy of a missing object is NoSuchKey, and nothing is kept in the
// tenant's metadata store.
func TestKazPersistCopySameKey(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	kazRequireXattrs(t, root)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket"), 0777))
	a := newKazPersistBackend(t, root, time.Hour)
	_, err := a.PutObject(ctx, "bucket", "k", map[string]string{"X-Amz-Meta-Crc32c": "3"}, strings.NewReader("abc"), 3)
	require.NoError(t, err)
	_, err = a.CopyObject(ctx, "bucket", "k", "bucket", "k", map[string]string{"X-Amz-Meta-Crc32c": "4"})
	assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNotImplemented), "same key copy: %v", err)
	_, err = a.CopyObject(ctx, "bucket", "missing", "bucket", "missing", map[string]string{})
	assert.True(t, gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchKey), "missing source: %v", err)
	assert.Zero(t, a.tenant(ctx).meta.Len(), "tenant metadata must stay empty")
}
