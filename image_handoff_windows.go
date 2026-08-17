//go:build windows

package ampacp

// handoffOpenFlags adds no flags on Windows: the platform exposes no
// non-blocking open mode here. Containment is still the read root's, and the
// descriptor's regular-file check still rejects a name under the root that does
// not lead to a regular file.
const handoffOpenFlags = 0
