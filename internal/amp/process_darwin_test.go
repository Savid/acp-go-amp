//go:build darwin

package amp

import (
	"os/exec"
	"testing"
)

func TestConfigureCommandDarwin(t *testing.T) {
	cmd := exec.Command("true")
	configureCommand(cmd)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatalf("SysProcAttr = %#v, want Setpgid", cmd.SysProcAttr)
	}
}
