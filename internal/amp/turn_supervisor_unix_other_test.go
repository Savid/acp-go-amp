//go:build darwin || freebsd || openbsd

package amp

import (
	"errors"
	"os/exec"
	"testing"
)

func TestTurnLaunchFailsClosedWithoutUnescapableContainment(t *testing.T) {
	launch, err := prepareProcessTreeCommand(exec.Command("amp"))
	if launch != nil || !errors.Is(err, ErrProcessTreeNotQuiescent) {
		t.Fatalf("turn launch = %#v, %v; want process-tree proof failure", launch, err)
	}
}
