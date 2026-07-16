//go:build !linux && !darwin && !freebsd && !openbsd && !windows

package amp

import (
	"fmt"
	"os/exec"
	"time"
)

type processTree struct{}

func (*processTree) descendantCount() (int, bool) { return 0, false }

func configureCommand(*exec.Cmd) {}

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	launch.close()

	return nil, fmt.Errorf("%w: platform containment backend unavailable", ErrProcessTreeNotQuiescent)
}

func (*processTree) interrupt() error { return ErrProcessTreeNotQuiescent }
func (*processTree) kill() error      { return ErrProcessTreeNotQuiescent }
func (*processTree) terminateAndWait(time.Duration) error {
	return ErrProcessTreeNotQuiescent
}

func interruptProcess(*exec.Cmd) error { return ErrProcessTreeNotQuiescent }
func killProcess(*exec.Cmd) error      { return ErrProcessTreeNotQuiescent }
