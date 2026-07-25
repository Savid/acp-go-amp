//go:build unix

package ampacp

import "syscall"

// handoffOpenFlags hardens the open of an already-resolved handoff path.
// O_NOFOLLOW refuses a final component that became a symlink after resolution,
// and O_NONBLOCK means a component that became a FIFO fails the descriptor mode
// re-check instead of blocking the turn inside open(2). Neither flag changes how
// a regular file reads.
const handoffOpenFlags = syscall.O_NOFOLLOW | syscall.O_NONBLOCK
