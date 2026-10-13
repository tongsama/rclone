package vfs

import (
	"errors"

	"github.com/rclone/rclone/fs"
)

// ErrKazUploadStarted is returned by SetKazUploadMetadata once the upload of
// the handle has started, as the metadata can then no longer be sent with it.
var ErrKazUploadStarted = errors.New("vfs: upload already started, metadata must be set before the first write")

// KazUploadMetadataSetter is implemented by write handles which can send
// metadata with the upload of the file (cache mode off only).
//
// The metadata is passed to the backend as if --metadata were set for this
// upload only. It must be set before the first write.
type KazUploadMetadataSetter interface {
	SetKazUploadMetadata(meta fs.Metadata) error
}

// check interface
var _ KazUploadMetadataSetter = (*WriteFileHandle)(nil)

// SetKazUploadMetadata sets the metadata sent with the upload of this handle.
//
// It returns ErrKazUploadStarted if the upload has already started.
func (fh *WriteFileHandle) SetKazUploadMetadata(meta fs.Metadata) error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.opened {
		return ErrKazUploadStarted
	}
	fh.uploadMeta = meta
	return nil
}
