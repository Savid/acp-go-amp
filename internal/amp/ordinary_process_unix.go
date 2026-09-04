//go:build !windows

package amp

import (
	"errors"
	"os"
)

// processAlreadyGone reports whether a Kill refusal means the child had already
// exited, which is revocation satisfied rather than revocation refused. On Unix
// the kernel keeps the child reapable until it is waited for, so the one
// spelling is os.ErrProcessDone.
func processAlreadyGone(err error) bool {
	return errors.Is(err, os.ErrProcessDone)
}
