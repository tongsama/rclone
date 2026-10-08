package s3

import (
	"context"
	"errors"
	"testing"

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
