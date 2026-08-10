//go:build !unix

package amp

import (
	"errors"
	"os/exec"
)

func validateProcessIsolationPlatform(isolation *ProcessIsolation) error {
	// The implicit current-identity policy asks for no isolation at all, so the
	// platform having none to offer is not an error; the containment backend
	// still decides whether a launch is possible here.
	if isolation.Implicit {
		return nil
	}

	return errors.New("process isolation is unsupported on this platform")
}
func sharedProcessIdentity(*ProcessIsolation) bool { return false }
func applyProcessIsolation(_ *exec.Cmd, isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
func verifyProcessIsolation(isolation *ProcessIsolation) error {
	return validateProcessIsolation(isolation)
}
