//go:build !linux && !darwin && !freebsd && !openbsd

package amp

import "os/exec"

func prepareProcessTreeCommand(native *exec.Cmd) (*processTreeCommand, error) {
	configureCommand(native)

	return &processTreeCommand{cmd: native}, nil
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
