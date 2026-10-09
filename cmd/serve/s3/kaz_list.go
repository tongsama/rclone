package s3

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
)

// errKazPageFull stops the walk when an entry is found after the page is
// already full, which proves the listing is truncated.
var errKazPageFull = errors.New("kaz: list page full")

// kazLister collects one page of a bucket listing by walking its directories
// depth first in S3 key order (UTF-8 byte order), so that only the
// directories needed for the page are read.
type kazLister struct {
	ctx       context.Context
	vfs       *vfs.VFS
	b         *s3Backend
	bucket    string
	delimiter bool   // return sub directories as CommonPrefixes
	hasMarker bool   // only entries after marker are returned
	marker    string // last key or CommonPrefix of the previous page
	maxKeys   int    // entries per page
	count     int    // entries added to resp
	dirs      int    // directories read, for the debug log
	last      string // last key or CommonPrefix added to resp
	resp      *gofakes3.ObjectList
}

// kazListEntry is a directory entry with its object key and the key it sorts
// by in S3: the object key, or for a directory the key followed by "/".
type kazListEntry struct {
	node    vfs.Node
	key     string
	sortKey string
}

// kazListBucket returns the page of the listing of bucket under prefix that
// follows page.Marker. It replaces entryListR and pager with
// --kaz-s3-list-by-key-order so that a page does not need everything under
// the prefix to be read and sorted first.
func (b *s3Backend) kazListBucket(ctx context.Context, _vfs *vfs.VFS, bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	start := time.Now()
	maxKeys := int(page.MaxKeys)
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	l := &kazLister{
		ctx:       ctx,
		vfs:       _vfs,
		b:         b,
		bucket:    bucket,
		delimiter: prefix.HasDelimiter,
		hasMarker: page.HasMarker,
		marker:    page.Marker,
		maxKeys:   maxKeys,
		resp:      gofakes3.NewObjectList(),
	}
	dir, name := prefixParser(prefix)
	err := l.walk(dir, name)
	switch {
	case errors.Is(err, errKazPageFull):
		l.resp.IsTruncated = true
		l.resp.NextMarker = l.last
	case errors.Is(err, gofakes3.ErrNoSuchKey):
		// The prefix directory does not exist: AWS returns an empty list.
		l.resp = gofakes3.NewObjectList()
	case err != nil:
		return nil, err
	}
	fs.Debugf(bucket, "kaz list: prefix=%q marker=%q entries=%d truncated=%v dirs=%d took=%v",
		prefix.Prefix, page.Marker, l.count, l.resp.IsTruncated, l.dirs, time.Since(start))
	return l.resp, nil
}

// walk reads dir (a path inside the bucket, "" for its root) and adds the
// entries whose name starts with name in key order, descending into sub
// directories unless they are returned as CommonPrefixes. It returns
// errKazPageFull once the page is full and one more entry exists.
func (l *kazLister) walk(dir, name string) error {
	fp, err := bucketDirPath(l.bucket, dir)
	if err != nil {
		// A listing prefix that can't be represented as a path matches nothing.
		return gofakes3.ErrNoSuchKey
	}
	nodes, err := getDirEntries(fp, l.vfs)
	if err != nil {
		return err
	}
	l.dirs++

	entries := make([]kazListEntry, 0, len(nodes))
	for _, node := range nodes {
		object := node.Name()
		// Hide the temporary objects of in-progress uploads
		if strings.HasPrefix(object, tempObjectPrefix) || strings.HasPrefix(object, legacyMultipartUploadPrefix) {
			continue
		}
		if !strings.HasPrefix(object, name) {
			continue
		}
		key := path.Join(dir, object)
		sortKey := key
		if node.IsDir() {
			sortKey += "/"
		}
		entries = append(entries, kazListEntry{node: node, key: key, sortKey: sortKey})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].sortKey < entries[j].sortKey })

	for _, e := range entries {
		if !e.node.IsDir() || l.delimiter {
			if l.hasMarker && e.sortKey <= l.marker {
				continue
			}
			if err := l.add(e.sortKey, e.node); err != nil {
				return err
			}
			continue
		}
		if err := l.walk(e.key, ""); err != nil {
			return err
		}
	}
	return nil
}

// add appends an object, or a directory as a CommonPrefix (key ending in
// "/"), to the page. It returns errKazPageFull instead when the page already
// holds maxKeys entries.
func (l *kazLister) add(key string, node vfs.Node) error {
	if l.count >= l.maxKeys {
		return errKazPageFull
	}
	if node.IsDir() {
		l.resp.AddPrefix(key)
	} else {
		l.resp.Add(&gofakes3.Content{
			Key:          key,
			LastModified: gofakes3.NewContentTime(node.ModTime()),
			ETag:         getFileHash(node, l.b.s.etagHashType),
			Size:         node.Size(),
			StorageClass: gofakes3.StorageStandard,
		})
	}
	l.count++
	l.last = key
	return nil
}
