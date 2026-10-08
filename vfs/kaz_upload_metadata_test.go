package vfs

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireUserXattrs skips the test when the test remote is not local or the
// filesystem behind it cannot store user xattrs, which the local backend uses
// for user metadata.
func requireUserXattrs(t *testing.T, r *fstest.Run) {
	t.Helper()
	if r.Fremote.Features().UserMetadata == false {
		t.Skip("remote has no user metadata support")
	}
	ctx := context.Background()
	obj := r.WriteObject(ctx, "kaz-xattr-probe", "x", t1)
	o, err := r.Fremote.NewObject(ctx, obj.Path)
	require.NoError(t, err)
	setter, ok := o.(fs.SetMetadataer)
	if !ok {
		t.Skip("object cannot set metadata")
	}
	if err := setter.SetMetadata(ctx, fs.Metadata{"kaz-probe": "1"}); err != nil {
		t.Skipf("user xattrs not supported here: %v", err)
	}
	m, err := fs.GetMetadata(ctx, o)
	require.NoError(t, err)
	if m["kaz-probe"] != "1" {
		t.Skip("user xattrs not read back here")
	}
	require.NoError(t, o.Remove(ctx))
}

// TestKazUploadMetadataReachesBackend checks metadata set on a write handle
// before the first write is stored with the uploaded object.
func TestKazUploadMetadataReachesBackend(t *testing.T) {
	r, v := newTestVFS(t)
	requireUserXattrs(t, r)
	ctx := context.Background()

	fd, err := v.OpenFile("meta.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	require.NoError(t, err)
	setter, ok := fd.(KazUploadMetadataSetter)
	require.True(t, ok, "cache-mode off write handle must accept upload metadata")
	require.NoError(t, setter.SetKazUploadMetadata(fs.Metadata{"s3m-crc32c": "12345"}))
	_, err = fd.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	o, err := r.Fremote.NewObject(ctx, "meta.txt")
	require.NoError(t, err)
	m, err := fs.GetMetadata(ctx, o)
	require.NoError(t, err)
	assert.Equal(t, "12345", m["s3m-crc32c"])
}

// TestKazUploadMetadataUnsetUnchanged checks an upload without metadata does
// not store any kaz user metadata.
func TestKazUploadMetadataUnsetUnchanged(t *testing.T) {
	r, v := newTestVFS(t)
	requireUserXattrs(t, r)
	ctx := context.Background()

	fd, err := v.OpenFile("plain.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	require.NoError(t, err)
	_, err = fd.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, fd.Close())

	o, err := r.Fremote.NewObject(ctx, "plain.txt")
	require.NoError(t, err)
	m, err := fs.GetMetadata(ctx, o)
	require.NoError(t, err)
	_, found := m["s3m-crc32c"]
	assert.False(t, found)
}

// TestKazUploadMetadataTooLate checks setting metadata after the upload has
// started is refused rather than silently ignored.
func TestKazUploadMetadataTooLate(t *testing.T) {
	_, v := newTestVFS(t)
	fd, err := v.OpenFile("late.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	require.NoError(t, err)
	_, err = fd.Write([]byte("x"))
	require.NoError(t, err)
	err = fd.(KazUploadMetadataSetter).SetKazUploadMetadata(fs.Metadata{"s3m-a": "b"})
	assert.True(t, errors.Is(err, ErrKazUploadStarted), "got %v", err)
	require.NoError(t, fd.Close())
}
