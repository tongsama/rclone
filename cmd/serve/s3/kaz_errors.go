package s3

import (
	"errors"

	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/vfs"
)

// bucketStatError maps the error from a VFS Stat of a bucket. Only "does not
// exist" becomes NoSuchBucket; any other error (for example a remote rate
// limit or network failure) is returned as is, which gofakes3 reports as a
// 500 InternalError instead of hiding the failure behind a 404.
func bucketStatError(bucket string, err error) error {
	if errors.Is(err, vfs.ENOENT) {
		return gofakes3.BucketNotFound(bucket)
	}
	return err
}

// keyStatError maps the error from a VFS Stat of an object key in the same
// way as bucketStatError, using NoSuchKey for "does not exist".
func keyStatError(key string, err error) error {
	if errors.Is(err, vfs.ENOENT) {
		return gofakes3.KeyNotFound(key)
	}
	return err
}
