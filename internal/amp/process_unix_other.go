//go:build unix && !linux && !freebsd

package amp

import (
	"os/exec"
	"syscall"
)

// Darwin and the remaining BSDs have no Pdeathsig equivalent; parent-death
// cleanup is best-effort via process-group signalling.
func configureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}
