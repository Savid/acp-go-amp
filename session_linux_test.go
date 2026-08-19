//go:build linux

package ampacp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

var timeoutContainmentIdentitySequence atomic.Uint32

// timeoutContainmentIdentity gives each standalone-authority fixture its own
// vacant Linux identity. The authority record permanently binds a UID to its
// state root, so reusing the family-wide 65534 fixture across -count runs or
// concurrent packages would turn shared-fleet authority contention into a
// false process-lifecycle failure.
func timeoutContainmentIdentity() (uint32, uint32) {
	uid := uint32(1_000_000_000) + uint32(os.Getpid())*256 + timeoutContainmentIdentitySequence.Add(1)

	return uid, uid
}

// testNativeIsolation opts direct-client fixtures into escaped-descendant
// containment.
func testNativeIsolation() *nativeamp.ProcessIsolation {
	uid, gid := timeoutContainmentIdentity()

	return &nativeamp.ProcessIsolation{
		UID: uid, GID: gid,
		BaseEnvironment:      map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")},
		TestOnlyNoCredential: true,
	}
}

// containmentReadyWait bounds how long an isolated fixture may take to publish
// its readiness. These two launches claim the standalone agent identity, and
// that claim proves the identity vacant across every task in the PID namespace
// — the whole host under the privileged suite, walked by a supervisor that is
// the instrumented test binary itself. The generic one-second helper is sized
// for launches that claim nothing, and it reports that walk as a fixture which
// never started, so these cases carry a supervised bound instead.
const containmentReadyWait = 30 * time.Second

// awaitContainmentReady waits out the supervised bound for a fixture's
// readiness file. A prompt that ended first reports its own failure: a launch
// that never reached the fixture is not a missing file, and saying so names the
// cause instead of the symptom. A nil channel belongs to a case with no prompt
// of its own.
func awaitContainmentReady(t *testing.T, path string, promptEnded <-chan error) {
	t.Helper()

	deadline := time.Now().Add(containmentReadyWait)

	for {
		if _, err := os.Stat(path); err == nil {
			return
		}

		select {
		case err := <-promptEnded:
			t.Fatalf("the prompt ended before %s was created: %v", path, err)
		default:
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s was not created within %s", path, containmentReadyWait)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func TestCancelContainsDescendantBeforeReturn(t *testing.T) {
	path, stateDir := fakeAgentAmpPath(t, "sigint-descendant")
	client := nativeamp.NewClient(nil, nativeamp.Options{
		CLIPath: path, Cwd: t.TempDir(), Isolation: testNativeIsolation(),
	})
	turn, err := client.Continue(t.Context(), "T-agent-thread", map[string]any{"type": "user"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	t.Cleanup(func() { _ = turn.Close() })

	state := newPromptTurnState()
	state.setTurn(turn)
	agent := newTestAgent()
	// The short cancel timeout is the SIGINT grace, so the ladder escalates at
	// once. The close-turn wait is the ceiling on proving the isolated tree
	// contained, which an isolated descendant reaches through the supervisor, so
	// it gets room rather than the tightest value that has ever worked.
	agent.options.runtime.nativeCancelTimeout = 100 * time.Millisecond
	agent.options.runtime.nativeCloseTurnWait = 10 * time.Second
	session := &agentSession{agent: agent, activePrompt: state}

	awaitContainmentReady(t, filepath.Join(stateDir, "continue-ready"), nil)
	descendantPID := readHelperJSON[int](t, filepath.Join(stateDir, "descendant-pid.jsonl"))[0]
	requireProcessAlive(t, descendantPID, "before cancel")

	if err := session.Cancel(t.Context()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	requireProcessExited(t, descendantPID, "Cancel returned")
}

func TestTurnTimeoutContainsDescendantBeforeReturn(t *testing.T) {
	path, stateDir := fakeAgentAmpPath(t, "sigint-descendant")
	uid, gid := timeoutContainmentIdentity()
	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		// The deadline is configured only so the prompt arms one; it never
		// elapses. What fires it is the seam below, after this test has proven
		// the descendant alive.
		WithTurnTimeout(time.Hour),
		WithProcessIsolation(ProcessIsolation{
			UID: uid, GID: gid,
			BaseEnvironment: map[string]string{
				"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME"), "AMP_API_KEY": "fake",
			},
			StandaloneOwnerID:   fmt.Sprintf("turn-timeout-descendant-%d", uid),
			StandaloneStateRoot: testStandaloneStateRoot(t, uid, gid),
		}),
	)
	agent.options.runtime.nativeCancelTimeout = 100 * time.Millisecond
	agent.options.runtime.nativeCloseTurnWait = 10 * time.Second
	// The fixture becomes the subject of this case only once it has started its
	// escaped descendant, waited for that descendant to leave the process group,
	// and recorded its own readiness. A wall-clock deadline raced that handshake:
	// when the timer won, the ladder contained the fake amp before it wrote
	// continue-ready, and the case failed on a missing readiness file rather than
	// on the containment it exists to prove. Drive the deadline through the
	// newTurnTimer seam instead and fire it below, so the order is stated rather
	// than hoped for.
	turnDeadline := make(chan time.Time, 1)
	agent.options.runtime.newTurnTimer = func(time.Duration) (<-chan time.Time, func()) {
		return turnDeadline, func() {}
	}
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	resp, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(
			context.Background(),
			TextPromptRequest(resp.SessionId, "timeout-descendant", "run until timeout"),
		)
		resultCh <- result
		errCh <- promptErr
	}()

	awaitContainmentReady(t, filepath.Join(stateDir, "continue-ready"), errCh)
	descendantPID := readHelperJSON[int](t, filepath.Join(stateDir, "descendant-pid.jsonl"))[0]
	requireProcessAlive(t, descendantPID, "before timeout")

	turnDeadline <- time.Now()

	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if result.StopReason == acp.StopReasonCancelled {
			t.Fatalf("timeout reported as cancelled: %#v", result)
		}
		requireTurnFailure(t, promptErr, causeTimeout, "WithTurnTimeout")
		requireProcessExited(t, descendantPID, "timeout Prompt returned")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout prompt did not return")
	}
}

func requireProcessAlive(t *testing.T, pid int, stage string) {
	t.Helper()
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("process %d is not alive %s: %v", pid, stage, err)
	}
}

func requireProcessExited(t *testing.T, pid int, stage string) {
	t.Helper()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process %d remained alive after %s: %v", pid, stage, err)
	}
}
