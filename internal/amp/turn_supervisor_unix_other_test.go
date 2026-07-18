//go:build darwin || freebsd || openbsd

package amp

import (
	"errors"
	"os/exec"
	"runtime"
	"testing"
)

func TestTurnLaunchFailsClosedWithoutUnescapableContainment(t *testing.T) {
	launch, err := prepareProcessTreeCommand(exec.Command("amp"), processLaunchOptions{})
	if launch != nil || !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("turn launch = %#v, %v; want containment failure", launch, err)
	}

	launch, err = prepareProcessTreeCommand(exec.Command("amp"), processLaunchOptions{
		DarwinBestEffort: true,
		Generation:       &DarwinGeneration{RuntimeID: "00000000000000000000000000000000", ScratchRoot: t.TempDir()},
	})
	if runtime.GOOS == "darwin" {
		if err != nil || launch == nil {
			t.Fatalf("opted-in Darwin turn launch = %#v, %v", launch, err)
		}

		return
	}
	if launch != nil || !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("unsupported opted-in launch = %#v, %v; want containment failure", launch, err)
	}
}
