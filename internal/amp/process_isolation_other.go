//go:build !unix

package amp

import (
	"errors"
	"os/exec"
)

func validateProcessIsolationPlatform(*ProcessIsolation) error {
	return errors.New("process isolation is unsupported on this platform")
}
func sharedProcessIdentity(*ProcessIsolation) bool { return false }
func applyProcessIsolation(_ *exec.Cmd, isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
func verifyProcessIsolation(isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
