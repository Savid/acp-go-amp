//go:build windows

package ampacp

import (
	"path/filepath"
	"strings"
)

// handoffURIPath maps a file URI's path component onto a host path. RFC 8089
// spells a Windows path with an empty authority and a leading slash in front of
// the drive letter — file:///C:/dir/x.png — so that slash separates the
// authority from the path and is not part of the path itself. Used verbatim it
// yields "\C:\dir\x.png", which is not absolute, and the whole handoff surface
// is refused on this platform.
//
// A component that names no volume is returned unchanged, and so stays the
// drive-relative spelling the absolute-path gate then refuses.
func handoffURIPath(path string) string {
	trimmed := strings.TrimPrefix(path, "/")
	if filepath.VolumeName(filepath.FromSlash(trimmed)) == "" {
		return path
	}

	return trimmed
}
