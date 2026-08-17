//go:build linux

package amp

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestConfigureCommandLinux(t *testing.T) {
	cmd := exec.Command("true")
	configureCommand(cmd)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid || cmd.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Fatalf("SysProcAttr = %#v, want Setpgid and Pdeathsig SIGKILL", cmd.SysProcAttr)
	}
}
