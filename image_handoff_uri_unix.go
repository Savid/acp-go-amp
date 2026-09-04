//go:build !windows

package ampacp

// handoffURIPath maps a file URI's path component onto a host path. A URI path
// and a Unix path are the same spelling, so the component is already the path
// and this is the identity.
func handoffURIPath(path string) string { return path }
