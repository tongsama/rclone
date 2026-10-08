package vfs

import (
	"errors"
	"path"
	"time"

	"github.com/rclone/rclone/fs"
)

// lookup resolves leaf with a single Fs.NewObject call instead of listing
// the directory. It is used when --kaz-vfs-lookup-by-path is set.
//
// A found object or directory is cached as a normal (non virtual) entry so
// a later listing or ForgetAll can replace or drop it. A miss is not cached,
// so objects created by other clients sharing the remote are seen at once.
// Any error other than "not found" is returned as is so callers do not
// mistake a remote failure for a missing object.
//
// It must be called without d.mu held; the remote call runs unlocked so
// lookups of other names in the same directory are not serialised.
func (d *Dir) lookup(leaf string) (Node, error) {
	d.mu.RLock()
	dirPath := d.path
	d.mu.RUnlock()
	remote := path.Join(dirPath, leaf)

	o, err := d.f.NewObject(d.vfs.ctx, remote)
	var entry fs.DirEntry
	switch {
	case err == nil:
		entry = o
	case errors.Is(err, fs.ErrorIsDir):
		// The remote does not give a directory's modtime here; use now as
		// VFS.New does for the root directory.
		entry = fs.NewDir(remote, time.Now())
	case errors.Is(err, fs.ErrorObjectNotFound), errors.Is(err, fs.ErrorDirNotFound):
		return nil, ENOENT
	default:
		return nil, err
	}

	d.mu.Lock()
	if d.path != dirPath {
		// The directory was renamed while the remote call was in flight, so
		// the result belongs to the old path. Look up again under the new
		// one, with no lock held.
		d.mu.Unlock()
		return d.lookup(leaf)
	}
	defer d.mu.Unlock()
	if node, ok := d.items[leaf]; ok {
		// Another lookup, a listing or a create got there first.
		return node, nil
	}
	var node Node
	if obj, ok := entry.(fs.Object); ok {
		node = newFile(d, d.path, obj, leaf)
	} else {
		node = newDir(d.vfs, d.f, d, entry.(fs.Directory))
	}
	d.items[leaf] = node
	d._armLookupCleanup()
	return node, nil
}

// _armLookupCleanup schedules the directory cache cleanup after the first
// lookup hit since the last cleanup, so entries added by lookup expire after
// at most DirCacheTime*2. Arming only once per cleanup keeps a busy
// directory from postponing its cleanup forever, and a Dir dropped from its
// parent is not re-armed. Must be called with d.mu held.
func (d *Dir) _armLookupCleanup() {
	if d.lookupArmed {
		return
	}
	d.lookupArmed = true
	d.cleanupTimer.Reset(time.Duration(d.vfs.Opt.DirCacheTime * 2))
}
