// Package s3 implements a fake s3 server for rclone
package s3

import (
	"sort"

	"github.com/rclone/gofakes3"
)

// pager sorts the whole object list and returns the page of it after
// page.Marker. It is the listing used without --kaz-s3-list-by-key-order.
func (db *s3Backend) pager(list *gofakes3.ObjectList, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	// sort by alphabet
	sort.Slice(list.CommonPrefixes, func(i, j int) bool {
		return list.CommonPrefixes[i].Prefix < list.CommonPrefixes[j].Prefix
	})
	// sort by key name
	sort.Slice(list.Contents, func(i, j int) bool {
		return list.Contents[i].Key < list.Contents[j].Key
	})
	tokens := page.MaxKeys
	if tokens == 0 {
		tokens = 1000
	}
	if page.HasMarker {
		// S3 returns the keys after the marker, which need not be a key
		// itself (start-after may be any string), so search by order
		// rather than for an exact match.
		i := sort.Search(len(list.Contents), func(i int) bool {
			return list.Contents[i].Key > page.Marker
		})
		list.Contents = list.Contents[i:]
		j := sort.Search(len(list.CommonPrefixes), func(j int) bool {
			return list.CommonPrefixes[j].Prefix > page.Marker
		})
		list.CommonPrefixes = list.CommonPrefixes[j:]
	}

	response := gofakes3.NewObjectList()
	for _, obj := range list.CommonPrefixes {
		if tokens <= 0 {
			break
		}
		response.AddPrefix(obj.Prefix)
		tokens--
	}

	for _, obj := range list.Contents {
		if tokens <= 0 {
			break
		}
		response.Add(obj)
		tokens--
	}

	if len(list.CommonPrefixes)+len(list.Contents) > int(page.MaxKeys) {
		response.IsTruncated = true
		if len(response.Contents) > 0 {
			response.NextMarker = response.Contents[len(response.Contents)-1].Key
		} else {
			response.NextMarker = response.CommonPrefixes[len(response.CommonPrefixes)-1].Prefix
		}
	}

	return response, nil
}
