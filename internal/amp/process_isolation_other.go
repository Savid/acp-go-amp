//go:build !unix

package amp

import (
	"errors"
	"os/exec"
)

func validateProcessIsolationPlatform(isolation *ProcessIsolation) error {
	return errors.New("process isolation is unsupported on this platform")
}
func applyProcessIsolation(_ *exec.Cmd, isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
func verifyProcessIsolation(isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
