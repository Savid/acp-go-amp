//go:build !windows

package ampacp

import (
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestIteration6PromptContextCancelUsesInterruptLadder(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "sigint-ignore")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	agent.options.runtime.nativeCloseTurnWait = time.Second

	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	promptCtx, cancelPrompt := context.WithCancel(context.Background())
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(promptCtx, TextPromptRequest(resp.SessionId, "cancel by context"))
		resultCh <- result
		errCh <- promptErr
	}()

	waitForPath(t, filepath.Join(state, "continue-ready"))
	pids := readHelperJSON[int](t, filepath.Join(state, "pid.jsonl"))
	cancelPrompt()

	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if promptErr != nil || result.StopReason != acp.StopReasonCancelled {
			t.Fatalf("prompt after caller context cancel = %#v, %v", result, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("caller context cancel did not return from prompt")
	}

	waitForPath(t, filepath.Join(state, "signal"))
	waitForProcessExit(t, pids[len(pids)-1])
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d still exists after interrupt ladder", pid)
}
