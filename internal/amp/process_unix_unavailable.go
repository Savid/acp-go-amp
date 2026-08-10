//go:build unix && !linux && !darwin

package amp

import "time"

func (t *processTree) terminateAndWait(timeout time.Duration) error {
	return t.terminateOrdinary(timeout)
}

func validateBestEffortLaunch(*processTreeCommand, *processTree, func()) error { return nil }
