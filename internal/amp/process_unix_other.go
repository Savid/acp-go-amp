//go:build unix && !linux && !freebsd && !darwin

package amp

import (
	"os/exec"
	"syscall"
)

// The remaining Unix platforms have no Pdeathsig equivalent; parent-death
// cleanup is best-effort via process-group signalling.
func configureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
