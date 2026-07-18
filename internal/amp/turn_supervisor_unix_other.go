//go:build freebsd || openbsd

package amp

import (
	"fmt"
	"os/exec"
)

func prepareProcessTreeCommand(_ *exec.Cmd, _ processLaunchOptions) (*processTreeCommand, error) {
	return nil, fmt.Errorf(
		"%w: platform containment backend unavailable",
		ErrProcessContainmentIncomplete,
	)
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
