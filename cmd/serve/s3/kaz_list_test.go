package s3

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/cmd/serve/proxy"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// kazNames mixes names that sort before "/" ("a b", "a-", "a.") and after it
// ("a_") so that a directory "a" among them checks directories sort as "a/".
var kazNames = []string{"a", "a b", "a-", "a.", "a_", "b", "0", "1", "10", "1-x"}

// makeKazTree fills dir with a random tree of files and directories (some of
// them empty) up to three levels deep, reusing entries that already exist.
func makeKazTree(t *testing.T, dir string, rng *rand.Rand, depth int) {
	for _, name := range kazNames {
		p := filepath.Join(dir, name)
		if st, err := os.Stat(p); err == nil {
			if st.IsDir() && depth < 3 {
				makeKazTree(t, p, rng, depth+1)
			}
			continue
		}
		switch r := rng.IntN(10); {
		case r < 4:
			require.NoError(t, os.WriteFile(p, []byte(name), 0666))
		case r < 7 && depth < 3:
			require.NoError(t, os.Mkdir(p, 0777))
			makeKazTree(t, p, rng, depth+1)
		}
	}
}

// kazExpected returns what an S3 listing of prefix should hold, worked out
// straight from the tree under bucketDir: the keys and, with delimiter, the
// CommonPrefixes, in key order. Directories count as CommonPrefixes even when
// empty, as serve s3 shows them.
func kazExpected(t *testing.T, bucketDir, prefix string, delimiter bool) []string {
	set := map[string]bool{}
	err := filepath.Walk(bucketDir, func(p string, info os.FileInfo, err error) error {
		require.NoError(t, err)
		rel, err := filepath.Rel(bucketDir, p)
		require.NoError(t, err)
		if rel == "." || strings.HasPrefix(info.Name(), tempObjectPrefix) {
			return nil
		}
		key := filepath.ToSlash(rel)
		if info.IsDir() {
			if !delimiter {
				return nil
			}
			key += "/"
		}
		// The directory named by the prefix itself is no entry of it.
		if !strings.HasPrefix(key, prefix) || key == prefix {
			return nil
		}
		if delimiter {
			if i := strings.IndexByte(key[len(prefix):], '/'); i >= 0 {
				key = key[:len(prefix)+i+1]
			}
		}
		set[key] = true
		return nil
	})
	require.NoError(t, err)
	var keys []string // nil when empty, like the listings it is compared with
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// kazListPage requests one page of the listing of "bucket" from b.
func kazListPage(t *testing.T, b *s3Backend, ctx context.Context, prefix string, delimiter bool, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	p := &gofakes3.Prefix{HasPrefix: prefix != "", Prefix: prefix}
	if delimiter {
		p.HasDelimiter, p.Delimiter = true, "/"
	}
	return b.ListBucket(ctx, "bucket", p, page)
}

// kazListAll pages through the whole listing of prefix, maxKeys entries at a
// time, and returns the keys and CommonPrefixes in the order the pages gave
// them. Every page but the last must be full and carry a NextMarker.
func kazListAll(t *testing.T, b *s3Backend, prefix string, delimiter bool, maxKeys int64) []string {
	full := int(maxKeys)
	if full == 0 {
		full = 1000
	}
	var got []string
	page := gofakes3.ListBucketPage{MaxKeys: maxKeys}
	for i := 0; ; i++ {
		require.Less(t, i, 10000, "listing does not end")
		resp, err := kazListPage(t, b, context.Background(), prefix, delimiter, page)
		require.NoError(t, err)
		var items []string
		for _, c := range resp.Contents {
			items = append(items, c.Key)
		}
		for _, cp := range resp.CommonPrefixes {
			items = append(items, cp.Prefix)
		}
		// A page is one contiguous range of the key order, so sorting
		// inside it only merges Contents and CommonPrefixes.
		sort.Strings(items)
		got = append(got, items...)
		if !resp.IsTruncated {
			return got
		}
		require.Len(t, items, full, "a truncated page must be full")
		require.NotEmpty(t, resp.NextMarker)
		page.HasMarker, page.Marker = true, resp.NextMarker
	}
}

// kazByKeyOrder turns --kaz-s3-list-by-key-order on for newKazBackend.
func kazByKeyOrder(o *Options) { o.KazListByKeyOrder = true }

// TestKazListMatchesSortedWalk pages through a random tree with many page
// sizes, prefixes and with and without a delimiter, and checks the pages
// joined together equal the sorted listing of the whole tree.
func TestKazListMatchesSortedWalk(t *testing.T) {
	for _, lookup := range []bool{false, true} {
		b, _, root := newKazBackend(t, lookup, kazByKeyOrder)
		bucketDir := filepath.Join(root, "bucket")
		require.NoError(t, os.MkdirAll(filepath.Join(bucketDir, "a", "a"), 0777))
		require.NoError(t, os.WriteFile(filepath.Join(bucketDir, "a", "a-"), []byte("x"), 0666))
		makeKazTree(t, bucketDir, rand.New(rand.NewPCG(1, 2)), 0)

		prefixes := []string{"", "a/", "a", "a/a", "a/a/", "1", "a/a-/", "zz", "nope/deep/"}
		for _, prefix := range prefixes {
			for _, delimiter := range []bool{false, true} {
				want := kazExpected(t, bucketDir, prefix, delimiter)
				for _, maxKeys := range []int64{1, 2, 3, 7, 0, 1000} {
					got := kazListAll(t, b, prefix, delimiter, maxKeys)
					assert.Equal(t, want, got, "lookup=%v prefix=%q delimiter=%v maxKeys=%d", lookup, prefix, delimiter, maxKeys)
				}
			}
		}
	}
}

// TestKazListMarkerNotAKey checks a start-after that is not a key resumes
// right after it, inside a directory and between directories.
func TestKazListMarkerNotAKey(t *testing.T) {
	b, _, root := newKazBackend(t, false, kazByKeyOrder)
	bucketDir := filepath.Join(root, "bucket")
	makeKazTree(t, bucketDir, rand.New(rand.NewPCG(5, 6)), 0)
	all := kazExpected(t, bucketDir, "", false)
	require.NotEmpty(t, all)

	for _, marker := range []string{"", "0", "a/", "a/zzz", "a0", "a~", "zzz", all[len(all)/2] + "x"} {
		resp, err := kazListPage(t, b, context.Background(), "", false, gofakes3.ListBucketPage{MaxKeys: 1000, HasMarker: true, Marker: marker})
		require.NoError(t, err)
		var got []string
		for _, c := range resp.Contents {
			got = append(got, c.Key)
		}
		var want []string
		for _, key := range all {
			if key > marker {
				want = append(want, key)
			}
		}
		assert.Equal(t, want, got, "marker=%q", marker)
	}
}

// TestKazListHidesTempObjects checks the temporary objects of uploads in
// progress are neither listed nor counted towards MaxKeys.
func TestKazListHidesTempObjects(t *testing.T) {
	b, _, root := newKazBackend(t, false, kazByKeyOrder)
	dir := filepath.Join(root, "bucket", "d")
	require.NoError(t, os.MkdirAll(dir, 0777))
	for _, name := range []string{tempObjectPrefix + "x", legacyMultipartUploadPrefix + "y", "f1", "f2"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0666))
	}

	resp, err := kazListPage(t, b, context.Background(), "", false, gofakes3.ListBucketPage{MaxKeys: 2})
	require.NoError(t, err)
	require.Len(t, resp.Contents, 2)
	assert.Equal(t, "d/f1", resp.Contents[0].Key)
	assert.Equal(t, "d/f2", resp.Contents[1].Key)
	assert.False(t, resp.IsTruncated)
}

// makeKazFlatTree creates n directories d00, d01, ... in the bucket, each
// with the files f0, f1 and f2.
func makeKazFlatTree(t *testing.T, root string, n int) {
	for i := range n {
		dir := filepath.Join(root, "bucket", fmt.Sprintf("d%02d", i))
		require.NoError(t, os.MkdirAll(dir, 0777))
		for _, name := range []string{"f0", "f1", "f2"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0666))
		}
	}
}

// TestKazListReadsOnlyNeededDirs checks the first page reads only the
// directories it needs, and that a page after a marker deep in the bucket
// does not read the directories before the marker.
func TestKazListReadsOnlyNeededDirs(t *testing.T) {
	ctx := context.Background()

	b, ff, root := newKazBackend(t, false, kazByKeyOrder)
	makeKazFlatTree(t, root, 50)
	resp, err := kazListPage(t, b, ctx, "", false, gofakes3.ListBucketPage{MaxKeys: 2})
	require.NoError(t, err)
	assert.True(t, resp.IsTruncated)
	assert.Equal(t, "d00/f1", resp.NextMarker)
	assert.LessOrEqual(t, ff.lists.Load(), int64(5), "first page must not read all 50 directories")

	// A fresh backend has nothing cached, as after a restart.
	b, ff, root = newKazBackend(t, false, kazByKeyOrder)
	makeKazFlatTree(t, root, 50)
	resp, err = kazListPage(t, b, ctx, "", false, gofakes3.ListBucketPage{MaxKeys: 2, HasMarker: true, Marker: "d40/f2"})
	require.NoError(t, err)
	require.Len(t, resp.Contents, 2)
	assert.Equal(t, "d41/f0", resp.Contents[0].Key)
	assert.LessOrEqual(t, ff.lists.Load(), int64(6), "directories before the marker must not be read")
}

// TestKazListStopsOnCancel checks the walk stops between directories with
// context.Canceled once the request is canceled.
func TestKazListStopsOnCancel(t *testing.T) {
	b, ff, root := newKazBackend(t, false, kazByKeyOrder)
	makeKazFlatTree(t, root, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var listed []string
	ff.onList = func(dir string) error {
		listed = append(listed, dir)
		if dir == "bucket/d01" {
			cancel()
		}
		return nil
	}

	_, err := kazListPage(t, b, ctx, "", false, gofakes3.ListBucketPage{MaxKeys: 1000})
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, slices.Contains(listed, "bucket/d02"), "walk must stop after the cancel: %v", listed)
}

// TestKazListDirErrorIsNotEmpty checks a directory that can't be read fails
// the listing instead of being left out of it.
func TestKazListDirErrorIsNotEmpty(t *testing.T) {
	b, ff, root := newKazBackend(t, false, kazByKeyOrder)
	makeKazFlatTree(t, root, 5)
	ff.onList = func(dir string) error {
		if dir == "bucket/d03" {
			return errKaz
		}
		return nil
	}

	_, err := kazListPage(t, b, context.Background(), "", false, gofakes3.ListBucketPage{MaxKeys: 1000})
	assert.ErrorIs(t, err, errKaz)
}

// TestKazListContinuesAfterDeletes deletes objects through serve s3 between
// pages, as juicefs gc --delete does, so that emptied directories and the
// marker key itself disappear, and checks the listing still returns every
// remaining key exactly once.
func TestKazListContinuesAfterDeletes(t *testing.T) {
	ctx := context.Background()
	b, _, root := newKazBackend(t, false, kazByKeyOrder)
	makeKazFlatTree(t, root, 5)

	var got []string
	page := gofakes3.ListBucketPage{MaxKeys: 2}
	for i := 0; ; i++ {
		require.Less(t, i, 100)
		resp, err := kazListPage(t, b, ctx, "", false, page)
		require.NoError(t, err)
		for _, c := range resp.Contents {
			got = append(got, c.Key)
			// Delete every key of d01 and d02 as soon as it is listed.
			if strings.HasPrefix(c.Key, "d01/") || strings.HasPrefix(c.Key, "d02/") {
				require.NoError(t, b.deleteObject(ctx, "bucket", c.Key))
			}
		}
		if !resp.IsTruncated {
			break
		}
		page.HasMarker, page.Marker = true, resp.NextMarker
	}
	assert.Equal(t, kazExpectedFlat(5), got)
	_, err := os.Stat(filepath.Join(root, "bucket", "d01"))
	assert.True(t, os.IsNotExist(err), "the emptied directory must have been cleaned up")
}

// TestKazListDirRemovedElsewhere removes a directory on the remote after its
// parent was listed but before the walk reaches it, as another host sharing
// the remote can, and checks it is treated as empty.
func TestKazListDirRemovedElsewhere(t *testing.T) {
	ctx := context.Background()
	b, _, root := newKazBackend(t, false, kazByKeyOrder)
	makeKazFlatTree(t, root, 5)

	resp, err := kazListPage(t, b, ctx, "", false, gofakes3.ListBucketPage{MaxKeys: 2})
	require.NoError(t, err)
	require.True(t, resp.IsTruncated)
	require.NoError(t, os.RemoveAll(filepath.Join(root, "bucket", "d02")))

	got := []string{resp.Contents[0].Key, resp.Contents[1].Key}
	page := gofakes3.ListBucketPage{MaxKeys: 2, HasMarker: true, Marker: resp.NextMarker}
	for i := 0; ; i++ {
		require.Less(t, i, 100)
		resp, err = kazListPage(t, b, ctx, "", false, page)
		require.NoError(t, err)
		for _, c := range resp.Contents {
			got = append(got, c.Key)
		}
		if !resp.IsTruncated {
			break
		}
		page.HasMarker, page.Marker = true, resp.NextMarker
	}
	var want []string
	for _, key := range kazExpectedFlat(5) {
		if !strings.HasPrefix(key, "d02/") {
			want = append(want, key)
		}
	}
	assert.Equal(t, want, got)
}

// kazExpectedFlat returns the keys makeKazFlatTree creates for n directories.
func kazExpectedFlat(n int) []string {
	var keys []string
	for i := range n {
		for _, name := range []string{"f0", "f1", "f2"} {
			keys = append(keys, fmt.Sprintf("d%02d/%s", i, name))
		}
	}
	return keys
}

// TestKazListOffKeepsOldListing checks that without the option the old
// listing still returns the whole tree when the result fits in one page, and
// pages correctly without a delimiter now that the marker is compared by
// order.
func TestKazListOffKeepsOldListing(t *testing.T) {
	b, _, root := newKazBackend(t, false, nil)
	bucketDir := filepath.Join(root, "bucket")
	makeKazTree(t, bucketDir, rand.New(rand.NewPCG(1, 2)), 0)
	for _, delimiter := range []bool{false, true} {
		assert.Equal(t, kazExpected(t, bucketDir, "", delimiter), kazListAll(t, b, "", delimiter, 1000), "delimiter=%v", delimiter)
	}
	for _, maxKeys := range []int64{1, 3} {
		assert.Equal(t, kazExpected(t, bucketDir, "", false), kazListAll(t, b, "", false, maxKeys), "maxKeys=%d", maxKeys)
	}
}

// TestKazListMinioPaging pages through a listing over HTTP with a real S3
// client, using ListObjects V2 (continuation-token) and V1 (the last key, as
// no NextMarker is sent without a delimiter), and checks every entry comes
// back exactly once in key order.
func TestKazListMinioPaging(t *testing.T) {
	fstest.Initialise()
	ctx := context.Background()
	root := t.TempDir()
	bucketDir := filepath.Join(root, "bucket")
	require.NoError(t, os.MkdirAll(bucketDir, 0777))
	makeKazTree(t, bucketDir, rand.New(rand.NewPCG(3, 4)), 0)
	f, err := fs.NewFs(ctx, root)
	require.NoError(t, err)

	keyid, keysec := random.String(16), random.String(16)
	opt := Opt
	opt.AuthKey = []string{keyid + "," + keysec}
	opt.HTTP.ListenAddr = []string{endpoint}
	opt.KazListByKeyOrder = true
	w, err := newServer(ctx, f, &opt, &vfscommon.Opt, &proxy.Opt)
	require.NoError(t, err)
	go func() { _ = w.Serve() }()
	t.Cleanup(func() { _ = w.Shutdown() })
	u, err := url.Parse(w.server.URLs()[0])
	require.NoError(t, err)
	client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(keyid, keysec, "")})
	require.NoError(t, err)

	for _, v1 := range []bool{false, true} {
		for _, delimiter := range []bool{false, true} {
			for _, maxKeys := range []int{1, 3} {
				var got []string
				for obj := range client.ListObjects(ctx, "bucket", minio.ListObjectsOptions{Recursive: !delimiter, MaxKeys: maxKeys, UseV1: v1}) {
					require.NoError(t, obj.Err)
					got = append(got, obj.Key)
				}
				want := kazExpected(t, bucketDir, "", delimiter)
				if delimiter {
					// The client sends a page's Contents before its
					// CommonPrefixes, so only the set can be compared.
					sort.Strings(got)
				}
				assert.Equal(t, want, got, "v1=%v delimiter=%v maxKeys=%d", v1, delimiter, maxKeys)
			}
		}
	}
}
