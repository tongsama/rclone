package s3

import (
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/rclone/gofakes3"
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
