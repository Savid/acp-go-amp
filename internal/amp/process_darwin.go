//go:build darwin

package amp

import (
	"os/exec"
	"syscall"
)

// Darwin has no Pdeathsig equivalent; parent-death cleanup is best-effort via
// process-group signalling.
func configureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
