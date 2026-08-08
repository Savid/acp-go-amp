//go:build linux

package amp

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

// TestTurnSupervisorRefusalReasonReachesTheCallersError proves the whole point
// of the terminal readiness frame: a supervisor that refuses to start writes
// its reason where the parent already reads, so the caller's error names the
// refusal instead of reporting a bare readiness EOF. The refusal reason used
// here is a real one — an identity-lock adoption the guardian cannot perform is
// exactly the class of failure that has been costing triage time.
func TestTurnSupervisorRefusalReasonReachesTheCallersError(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	t.Setenv(turnSupervisorModeEnv, turnSupervisorMode)

	const reason = "adopt Amp agent identity lock: operation not permitted"

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()

	exitCode := -1
	turnSupervisorExit = func(code int) { exitCode = code }
	turnSupervisorInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return io.NopCloser(strings.NewReader("")), io.NopCloser(strings.NewReader("")), write, nil
	}
	turnSupervisorRun = func(io.Reader, io.Reader, io.Writer) error { return errors.New(reason) }

	turnSupervisorBootstrap()

	if exitCode != 1 {
		t.Fatalf("refused bootstrap exit = %d, want 1", exitCode)
	}

	err = awaitProcessTreeReady(&processTreeCommand{ready: read})
	if err == nil {
		t.Fatal("refused readiness succeeded")
	}
	if !strings.Contains(err.Error(), "amp native supervisor failed before readiness") {
		t.Fatalf("refusal error = %v", err)
	}
	if !strings.Contains(err.Error(), reason) {
		t.Fatalf("refusal error dropped the reason: %v", err)
	}
	if strings.Contains(err.Error(), "EOF") {
		t.Fatalf("named refusal reported as an EOF: %v", err)
	}
}

// TestTurnSupervisorWordlessDeathRemainsAnEOF pins the other half of the
// contract. The frame names a refusal that had something to say; a child that
// dies without writing one has not said anything, and EOF stays the honest
// verdict rather than being dressed up as a named refusal.
func TestTurnSupervisorWordlessDeathRemainsAnEOF(t *testing.T) {
	restoreTurnSupervisorSeams(t)

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()

	if err = write.Close(); err != nil {
		t.Fatal(err)
	}

	err = awaitProcessTreeReady(&processTreeCommand{ready: read})
	if err == nil {
		t.Fatal("wordless readiness succeeded")
	}
	if !strings.Contains(err.Error(), "await Amp native supervisor readiness") ||
		!strings.Contains(err.Error(), "EOF") {
		t.Fatalf("wordless death error = %v", err)
	}
	if strings.Contains(err.Error(), "failed before readiness") {
		t.Fatalf("wordless death claimed a named refusal: %v", err)
	}
}

// TestTurnSupervisorLivenessRefusalReasonReachesTheGuardian pins the inner hop.
// The liveness supervisor publishes readiness to the guardian rather than to
// the parent, so the same frame has to survive that read for the guardian's own
// refusal frame to carry anything but "EOF" onwards to the caller.
func TestTurnSupervisorLivenessRefusalReasonReachesTheGuardian(t *testing.T) {
	const reason = "start supervised Amp native root: fork/exec /usr/bin/amp: no such file or directory"

	pid, err := parseTurnSupervisorLivenessReady(turnSupervisorFailure + reason + "\n")
	if err == nil {
		t.Fatal("refused liveness readiness succeeded")
	}
	if pid != 0 {
		t.Fatalf("refused liveness readiness returned pid %d", pid)
	}
	if !strings.Contains(err.Error(), "amp liveness failed before readiness") ||
		!strings.Contains(err.Error(), reason) {
		t.Fatalf("liveness refusal error = %v", err)
	}
}
