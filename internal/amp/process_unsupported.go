//go:build !linux && !darwin && !freebsd && !openbsd && !windows

package amp

import (
	"errors"
	"fmt"
	"os/exec"
	"time"
)

type processTree struct{}

func (*processTree) descendantCount() (int, bool) { return 0, false }

func configureCommand(*exec.Cmd) {}

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	return nil, errors.Join(
		fmt.Errorf("%w: platform containment backend unavailable", ErrProcessContainmentIncomplete),
		launch.close(),
	)
}

func (*processTree) commandWait() *commandWait { return nil }

func (*processTree) interrupt() error { return ErrProcessContainmentIncomplete }
func (*processTree) kill() error      { return ErrProcessContainmentIncomplete }
func (*processTree) terminateAndWait(time.Duration) error {
	return ErrProcessContainmentIncomplete
}

func interruptProcess(*exec.Cmd) error { return ErrProcessContainmentIncomplete }
func killProcess(*exec.Cmd) error      { return ErrProcessContainmentIncomplete }
