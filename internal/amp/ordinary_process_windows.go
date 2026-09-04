//go:build windows

package amp

import (
	"errors"
	"os"
	"syscall"
)

// processAlreadyGone reports whether a Kill refusal means the child had already
// exited, which is revocation satisfied rather than revocation refused.
//
// Windows releases the process handle when the child is waited for, and a Kill
// against that released handle is answered with EINVAL — not os.ErrProcessDone,
// which the platform can no longer tell. Every amp process is short-lived and
// is waited for as its turn settles, so without this spelling a session close
// or delete over a finished turn reports "invalid argument" as a containment
// refusal for a child that has plainly gone.
func processAlreadyGone(err error) bool {
	return errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.EINVAL)
}
