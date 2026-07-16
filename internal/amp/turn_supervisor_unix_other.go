//go:build darwin || freebsd || openbsd

package amp

import (
	"fmt"
	"os/exec"
)

func prepareProcessTreeCommand(*exec.Cmd) (*processTreeCommand, error) {
	return nil, fmt.Errorf(
		"%w: platform cannot prove Amp turn descendants that escape a process group",
		ErrProcessTreeNotQuiescent,
	)
}

func awaitProcessTreeReady(*processTreeCommand) error { return nil }
