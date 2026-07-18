//go:build freebsd || openbsd

package amp

import "time"

func (t *processTree) terminateAndWait(time.Duration) error {
	return t.finish(ErrProcessContainmentIncomplete)
}

func validateBestEffortLaunch(*processTreeCommand, *processTree, func()) error { return nil }
