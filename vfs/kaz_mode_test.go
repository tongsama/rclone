//go:build kazlookup

package vfs

import "github.com/rclone/rclone/vfs/vfscommon"

// init turns on --kaz-vfs-lookup-by-path for every VFS the upstream tests of
// this package create, so `go test -tags kazlookup` checks that the kaz
// lookup paths keep the behaviour (and the security fixes) upstream tests.
func init() { vfscommon.Opt.KazLookupByPath = true }
