//go:build linux

package ampacp

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func TestCancelContainsDescendantBeforeReturn(t *testing.T) {
	path, stateDir := fakeAgentAmpPath(t, "sigint-descendant")
	client := nativeamp.NewClient(nil, nativeamp.Options{CLIPath: path, Cwd: t.TempDir()})
	turn, err := client.Continue(t.Context(), "T-agent-thread", map[string]any{"type": "user"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	t.Cleanup(func() { _ = turn.Close() })

	state := newPromptTurnState()
	state.setTurn(turn)
	agent := NewAgent()
	agent.options.runtime.nativeCancelTimeout = 100 * time.Millisecond
	agent.options.runtime.nativeCloseTurnWait = time.Second
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
	agent := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithTurnTimeout(100*time.Millisecond),
	)
	agent.options.runtime.nativeCancelTimeout = 100 * time.Millisecond
	agent.options.runtime.nativeCloseTurnWait = time.Second
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
