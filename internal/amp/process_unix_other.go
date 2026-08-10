//go:build unix && !linux && !darwin && !freebsd

package amp

import (
	"os/exec"
	"syscall"
)

// The remaining Unix platforms have no portable Pdeathsig equivalent;
// parent-death cleanup is best-effort via process-group signalling.
func configureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
