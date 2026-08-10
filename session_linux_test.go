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

	waitForPath(t, filepath.Join(stateDir, "continue-ready"))
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
		WithTurnTimeout(100*time.Millisecond),
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

	waitForPath(t, filepath.Join(stateDir, "continue-ready"))
	descendantPID := readHelperJSON[int](t, filepath.Join(stateDir, "descendant-pid.jsonl"))[0]
	requireProcessAlive(t, descendantPID, "before timeout")

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
