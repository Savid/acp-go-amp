//go:build !windows

package ampacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestCloseSessionCancelsActiveTurnAfterProcessExit(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "sigint-ignore")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	agent.options.runtime.nativeCancelTimeout = 100 * time.Millisecond
	agent.options.runtime.nativeCloseTurnWait = time.Second

	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	session, err := agent.session(resp.SessionId)
	if err != nil {
		t.Fatalf("session lookup: %v", err)
	}
	settingsDir := session.settingsDir
	if _, err := os.Stat(settingsDir); err != nil {
		t.Fatalf("settings dir before close: %v", err)
	}

	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(context.Background(), TextPromptRequest(resp.SessionId, "close me"))
		resultCh <- result
		errCh <- promptErr
	}()

	waitForPath(t, filepath.Join(state, "continue-ready"))
	pids := readHelperJSON[int](t, filepath.Join(state, "pid.jsonl"))
	pid := pids[len(pids)-1]

	closeDone := make(chan error, 1)
	go func() {
		_, closeErr := agent.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: resp.SessionId})
		closeDone <- closeErr
	}()

	waitForPath(t, filepath.Join(state, "signal"))
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		t.Fatal("fake process exited before kill fallback")
	}
	if _, err := os.Stat(settingsDir); err != nil {
		t.Fatalf("settings dir removed while process was still alive: %v", err)
	}

	select {
	case closeErr := <-closeDone:
		if closeErr != nil {
			t.Fatalf("CloseSession: %v", closeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CloseSession did not finish")
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("CloseSession returned before process exit: %v", err)
	}
	if _, err := os.Stat(settingsDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("settings dir after close = %v, want removed", err)
	}

	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if promptErr != nil || result.StopReason != acp.StopReasonCancelled {
			t.Fatalf("prompt after close = %#v, %v", result, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prompt did not return cancelled result")
	}
	select {
	case extra := <-resultCh:
		t.Fatalf("extra prompt result: %#v", extra)
	default:
	}
}

func TestPromptContextCancelUsesInterruptLadder(t *testing.T) {
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
