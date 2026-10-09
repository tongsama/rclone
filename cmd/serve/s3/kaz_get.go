package s3

import (
	"context"
	"io"

	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/chunkedreader"
	"github.com/rclone/rclone/vfs"
)

// kazOpenObject opens the body of a GET of o with the request context ctx,
// for --kaz-s3-cancel-get-on-disconnect.
//
// The VFS opens downloads with the context of the file, so a client that
// goes away before the backend answers leaves the download running. Opening
// with ctx makes the backend request end with the client request. rnge may
// be nil for the whole object.
func kazOpenObject(ctx context.Context, v *vfs.VFS, o fs.Object, rnge *gofakes3.ObjectRange) (io.ReadCloser, error) {
	opt := &v.Opt
	cr := chunkedreader.New(ctx, o, int64(opt.ChunkSize), int64(opt.ChunkSizeLimit), opt.ChunkStreams)
	if rnge != nil {
		// Seeking before Open makes the first backend request start at the range.
		if _, err := cr.RangeSeek(ctx, rnge.Start, io.SeekStart, rnge.Length); err != nil {
			_ = cr.Close()
			return nil, err
		}
	}
	r, err := cr.Open()
	if err != nil {
		_ = cr.Close()
		return nil, err
	}
	tr := accounting.GlobalStats().NewTransfer(o, nil)
	in := tr.Account(ctx, r).WithBuffer()
	var rdr io.Reader = in
	if rnge != nil {
		rdr = io.LimitReader(in, rnge.Length)
	}
	return &readerWithCloser{
		Reader: rdr,
		closer: func() error {
			err := in.Close()
			tr.Done(ctx, err)
			return err
		},
	}, nil
}
