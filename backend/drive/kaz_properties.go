package drive

import (
	"maps"

	"github.com/rclone/rclone/fs"
	drive "google.golang.org/api/drive/v3"
)

// kazUploadFields returns the fields requested from upload responses, adding
// properties when kaz_properties is set so a new object knows its metadata.
func (f *Fs) kazUploadFields() string {
	if f.opt.KazProperties {
		return partialFields + ",properties"
	}
	return partialFields
}

// kazUserMetadata returns the user properties of info as metadata, empty but
// non-nil when there are none so Metadata does not fetch them again.
func kazUserMetadata(info *drive.File) *fs.Metadata {
	m := make(fs.Metadata, len(info.Properties))
	maps.Copy(m, info.Properties)
	return &m
}

// kazNullStaleProperties makes updateInfo delete the existing properties
// which it does not set, so the properties match the new upload.
func kazNullStaleProperties(updateInfo *drive.File, existing map[string]string) {
	for k := range existing {
		if _, ok := updateInfo.Properties[k]; ok {
			continue
		}
		updateInfo.NullFields = append(updateInfo.NullFields, "Properties."+k)
	}
	if len(updateInfo.NullFields) > 0 {
		// An empty map is otherwise left out of the request with its nulls.
		updateInfo.ForceSendFields = append(updateInfo.ForceSendFields, "Properties")
	}
}
