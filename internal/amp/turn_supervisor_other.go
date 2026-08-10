//go:build !unix

package amp

import (
	"errors"
	"fmt"
	"os/exec"
)

func prepareProcessTreeCommand(native *exec.Cmd, options processLaunchOptions) (*processTreeCommand, error) {
	if options.Isolation != nil {
		return nil, errors.New("process isolation is unsupported on this platform")
	}
	if options.DarwinBestEffort {
		return nil, fmt.Errorf("%w: Darwin best-effort containment is unavailable", ErrProcessContainmentIncomplete)
	}

	configureCommand(native)

	return &processTreeCommand{cmd: native}, nil
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
