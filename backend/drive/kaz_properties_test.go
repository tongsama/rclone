package drive

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	drive "google.golang.org/api/drive/v3"
)

// TestKazFieldsIncludeProperties checks properties are requested for listings
// and uploads only when kaz_properties is set.
func TestKazFieldsIncludeProperties(t *testing.T) {
	ctx := context.Background()
	f := &Fs{}
	assert.NotContains(t, string(f.getFileFields(ctx)), "properties")
	assert.Equal(t, partialFields, f.kazUploadFields())

	f.opt.KazProperties = true
	assert.Contains(t, strings.Split(string(f.getFileFields(ctx)), ","), "properties")
	assert.Contains(t, strings.Split(f.kazUploadFields(), ","), "properties")
}

// TestKazNewBaseObjectKeepsProperties checks an object made from a listing
// with properties answers Metadata from them without an API call.
func TestKazNewBaseObjectKeepsProperties(t *testing.T) {
	ctx := context.Background()
	f := &Fs{}
	f.opt.KazProperties = true
	info := &drive.File{Id: "id1", Name: "a", Properties: map[string]string{"s3m-crc32c": "42"}}
	o, err := f.newBaseObject(ctx, "a", info)
	require.NoError(t, err)
	m, err := o.Metadata(ctx) // must not call the API (f.svc is nil)
	require.NoError(t, err)
	assert.Equal(t, "42", m["s3m-crc32c"])
	assert.Equal(t, map[string]string{"s3m-crc32c": "42"}, o.kazProperties)

	// No properties: empty metadata, still no API call.
	o, err = f.newBaseObject(ctx, "b", &drive.File{Id: "id2", Name: "b"})
	require.NoError(t, err)
	m, err = o.Metadata(ctx)
	require.NoError(t, err)
	assert.Empty(t, m)
}

// TestKazNullStaleProperties checks an update sends null for existing
// properties which the new metadata no longer has, and keeps the rest.
func TestKazNullStaleProperties(t *testing.T) {
	updateInfo := &drive.File{Properties: map[string]string{"s3m-new": "1"}}
	kazNullStaleProperties(updateInfo, map[string]string{"s3m-old": "x", "s3m-new": "0"})
	body, err := json.Marshal(updateInfo)
	require.NoError(t, err)
	var got struct {
		Properties map[string]*string `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(body, &got))
	require.Contains(t, got.Properties, "s3m-old")
	assert.Nil(t, got.Properties["s3m-old"], "stale key must be sent as null")
	require.NotNil(t, got.Properties["s3m-new"])
	assert.Equal(t, "1", *got.Properties["s3m-new"])

	// Nothing new at all: every existing key is nulled and properties is sent.
	updateInfo = &drive.File{}
	kazNullStaleProperties(updateInfo, map[string]string{"s3m-old": "x"})
	body, err = json.Marshal(updateInfo)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"properties":{"s3m-old":null}`)
}
