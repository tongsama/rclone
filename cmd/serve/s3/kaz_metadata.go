package s3

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/ncw/swift/v2"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs"
)

const (
	// kazAmzMetaPrefix is the canonical prefix of S3 user metadata headers.
	kazAmzMetaPrefix = "X-Amz-Meta-"
	// kazPropertyPrefix prefixes user metadata stored in the backend so it
	// never collides with metadata names the backend interprets itself.
	kazPropertyPrefix = "s3m-"
)

// kazToProperties returns the X-Amz-Meta-* entries of meta as backend
// metadata named s3m-<lower case name>, or nil if there are none.
func kazToProperties(meta map[string]string) fs.Metadata {
	var out fs.Metadata
	for k, v := range meta {
		if len(k) <= len(kazAmzMetaPrefix) || !strings.EqualFold(k[:len(kazAmzMetaPrefix)], kazAmzMetaPrefix) {
			continue
		}
		if out == nil {
			out = fs.Metadata{}
		}
		out[kazPropertyPrefix+strings.ToLower(k[len(kazAmzMetaPrefix):])] = v
	}
	return out
}

// kazFromProperties returns the s3m-* entries of m as X-Amz-Meta-* headers.
func kazFromProperties(m fs.Metadata) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		name, ok := strings.CutPrefix(k, kazPropertyPrefix)
		if !ok || name == "" {
			continue
		}
		out[http.CanonicalHeaderKey(kazAmzMetaPrefix+name)] = v
	}
	return out
}

// kazSetUploadMetadata sends the user metadata of meta with the upload made
// through h. It fails if the handle cannot carry metadata, so metadata is
// never dropped silently.
func kazSetUploadMetadata(h vfs.Handle, meta map[string]string) error {
	setter, ok := h.(vfs.KazUploadMetadataSetter)
	if !ok {
		return errors.New("serve s3: this upload cannot store metadata (needs --vfs-cache-mode off)")
	}
	props := kazToProperties(meta)
	if props == nil {
		// Still mark the upload as carrying metadata so a backend which
		// replaces properties on update drops stale ones.
		props = fs.Metadata{}
	}
	return setter.SetKazUploadMetadata(props)
}

// kazObjectUserMetadata returns the X-Amz-Meta-* headers stored with the
// object behind node. A node still uploading or a backend without metadata
// support gives none; a failure to read the metadata is returned.
func kazObjectUserMetadata(ctx context.Context, node vfs.Node) (map[string]string, error) {
	o, ok := node.DirEntry().(fs.Object)
	if !ok || o == nil {
		return map[string]string{}, nil
	}
	// Ask with --metadata set so a backend fetching it on demand does not
	// cache an empty result.
	ctx, ci := fs.AddConfig(ctx)
	ci.Metadata = true
	m, err := fs.GetMetadata(ctx, o)
	if err != nil {
		return nil, err
	}
	return kazFromProperties(m), nil
}

// kazStoreMeta stores meta for the object at fp like tenant.storeMeta, except
// that with --kaz-s3-persist-metadata the metadata already travelled with the
// upload, so only the modification time is applied and nothing is kept in
// memory.
func (b *s3Backend) kazStoreMeta(t *tenant, v *vfs.VFS, fp string, meta map[string]string) error {
	if !b.s.opt.KazPersistMetadata {
		return t.storeMeta(v, fp, meta)
	}
	return kazApplyModtime(v, fp, meta)
}

// kazApplyModtime sets the modification time of the object at fp from
// "X-Amz-Meta-Mtime" or "mtime" in meta, as tenant.storeMeta does.
func kazApplyModtime(v *vfs.VFS, fp string, meta map[string]string) error {
	for _, key := range []string{"X-Amz-Meta-Mtime", "mtime"} {
		val, ok := meta[key]
		if !ok {
			continue
		}
		ti, err := swift.FloatStringToTime(val)
		if err != nil {
			continue
		}
		return v.Chtimes(fp, ti, ti)
	}
	return nil
}

// kazUserMeta returns the user metadata of node, the object at fp: from the
// object itself with --kaz-s3-persist-metadata, otherwise what this user
// stored in memory (tenant.getMeta).
func (b *s3Backend) kazUserMeta(ctx context.Context, fp string, node vfs.Node) (map[string]string, bool, error) {
	if b.s.opt.KazPersistMetadata {
		user, err := kazObjectUserMetadata(ctx, node)
		return user, err == nil, err
	}
	meta, ok := b.tenant(ctx).getMeta(ctx, fp, node)
	return meta, ok, nil
}
