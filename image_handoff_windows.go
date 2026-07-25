//go:build windows

package ampacp

// handoffOpenFlags adds no flags on Windows: the platform exposes no
// symlink-refusing or non-blocking open mode here. The descriptor mode re-check
// still rejects a handoff path that changed identity after it was resolved.
const handoffOpenFlags = 0
