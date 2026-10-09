package s3

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/cmd/serve/proxy"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatedFs wraps an fs.Fs so that opening any object waits until release is
// closed or the context of the Open call ends, like a remote that is slow to
// answer a download.
type gatedFs struct {
	fs.Fs
	opened  chan context.Context
	release chan struct{}
}

// gatedObject is an fs.Object of a gatedFs.
type gatedObject struct {
	fs.Object
	f *gatedFs
}

// Open reports its context on opened, then waits for release or ctx.
func (o *gatedObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	o.f.opened <- ctx
	select {
	case <-o.f.release:
		return o.Object.Open(ctx, options...)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// wrap returns entries with every object wrapped in a gatedObject.
func (f *gatedFs) wrap(entries fs.DirEntries) fs.DirEntries {
	for i, e := range entries {
		if o, ok := e.(fs.Object); ok {
			entries[i] = &gatedObject{Object: o, f: f}
		}
	}
	return entries
}

// List lists dir with gated objects.
func (f *gatedFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.Fs.List(ctx, dir)
	return f.wrap(entries), err
}

// NewObject finds a gated object.
func (f *gatedFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	o, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return &gatedObject{Object: o, f: f}, nil
}

// newGatedBackend serves a local directory holding bucket/obj through a
// gatedFs with --vfs-cache-mode off and the given --kaz-s3-cancel-get-on-disconnect.
func newGatedBackend(t *testing.T, cancelOnDisconnect bool) (*s3Backend, *gatedFs) {
	fstest.Initialise()
	ctx := context.Background()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bucket"), 0777))
	require.NoError(t, os.WriteFile(filepath.Join(root, "bucket", "obj"), []byte("0123456789"), 0666))
	base, err := fs.NewFs(ctx, root)
	require.NoError(t, err)
	gf := &gatedFs{Fs: base, opened: make(chan context.Context, 8), release: make(chan struct{})}

	vfsOpt := vfscommon.Opt
	vfsOpt.CacheMode = vfscommon.CacheModeOff
	opt := Opt
	opt.HTTP.ListenAddr = []string{endpoint}
	opt.KazCancelGetOnDisconnect = cancelOnDisconnect
	w, err := newServer(ctx, gf, &opt, &vfsOpt, &proxy.Opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Shutdown() })
	return newBackend(w), gf
}

// waitOpened waits for the next Open of a gated object and returns its context.
func waitOpened(t *testing.T, gf *gatedFs) context.Context {
	select {
	case ctx := <-gf.opened:
		return ctx
	case <-time.After(5 * time.Second):
		t.Fatal("object was not opened")
		return nil
	}
}

// TestKazCancelGetOnDisconnect checks that, with the option, a client that goes
// away while the remote has not answered yet also cancels the download.
func TestKazCancelGetOnDisconnect(t *testing.T) {
	b, gf := newGatedBackend(t, true)
	reqCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		obj, err := b.GetObject(reqCtx, "bucket", "obj", nil)
		if err == nil {
			_, err = io.ReadAll(obj.Contents)
			_ = obj.Contents.Close()
		}
		done <- err
	}()
	openCtx := waitOpened(t, gf)

	cancel()
	select {
	case err := <-done:
		assert.True(t, errors.Is(err, context.Canceled), "unexpected error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("GET did not return after the client went away")
	}
	select {
	case <-openCtx.Done():
	default:
		t.Fatal("the download from the remote was not canceled")
	}
}

// TestKazCancelGetOnDisconnectRange checks ranges are served correctly.
func TestKazCancelGetOnDisconnectRange(t *testing.T) {
	b, gf := newGatedBackend(t, true)
	close(gf.release)
	ctx := context.Background()

	obj, err := b.GetObject(ctx, "bucket", "obj", nil)
	require.NoError(t, err)
	data, err := io.ReadAll(obj.Contents)
	require.NoError(t, err)
	require.NoError(t, obj.Contents.Close())
	assert.Equal(t, "0123456789", string(data))
	assert.Equal(t, int64(10), obj.Size)

	obj, err = b.GetObject(ctx, "bucket", "obj", &gofakes3.ObjectRangeRequest{Start: 3, End: 6})
	require.NoError(t, err)
	data, err = io.ReadAll(obj.Contents)
	require.NoError(t, err)
	require.NoError(t, obj.Contents.Close())
	assert.Equal(t, "3456", string(data))
}

// TestKazCancelGetOnDisconnectOff checks that without the option the download
// is not tied to the request, as before.
func TestKazCancelGetOnDisconnectOff(t *testing.T) {
	b, gf := newGatedBackend(t, false)
	reqCtx, cancel := context.WithCancel(context.Background())
	obj, err := b.GetObject(reqCtx, "bucket", "obj", nil)
	require.NoError(t, err)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(obj.Contents)
		readDone <- err
	}()
	openCtx := waitOpened(t, gf)
	cancel()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-openCtx.Done():
		t.Fatal("download must not follow the request context without the option")
	default:
	}
	close(gf.release)
	require.NoError(t, <-readDone)
	require.NoError(t, obj.Contents.Close())
}

// TestKazCancelGetOnDisconnectNeedsCacheModeOff checks the option is refused
// with a VFS cache mode other than off.
func TestKazCancelGetOnDisconnectNeedsCacheModeOff(t *testing.T) {
	fstest.Initialise()
	ctx := context.Background()
	base, err := fs.NewFs(ctx, t.TempDir())
	require.NoError(t, err)
	vfsOpt := vfscommon.Opt
	vfsOpt.CacheMode = vfscommon.CacheModeWrites
	opt := Opt
	opt.HTTP.ListenAddr = []string{endpoint}
	opt.KazCancelGetOnDisconnect = true
	_, err = newServer(ctx, base, &opt, &vfsOpt, &proxy.Opt)
	assert.Error(t, err)
}
