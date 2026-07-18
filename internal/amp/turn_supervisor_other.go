//go:build !linux && !darwin && !freebsd && !openbsd

package amp

import "os/exec"

func prepareProcessTreeCommand(native *exec.Cmd, _ processLaunchOptions) (*processTreeCommand, error) {
	configureCommand(native)

	return &processTreeCommand{cmd: native}, nil
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
