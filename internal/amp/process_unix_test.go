//go:build linux || darwin || freebsd || openbsd

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSignalProcessGroupErrors(t *testing.T) {
	originalGetpgid := syscallGetpgid
	originalKill := syscallKill
	t.Cleanup(func() {
		syscallGetpgid = originalGetpgid
		syscallKill = originalKill
	})

	cmd := &exec.Cmd{Process: &os.Process{Pid: 12345}}

	syscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
	if err := killProcess(cmd); err != nil {
		t.Fatalf("getpgid ESRCH should map to nil, got %v", err)
	}

	syscallGetpgid = func(int) (int, error) { return 0, syscall.EPERM }
	if err := killProcess(cmd); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("getpgid EPERM should propagate, got %v", err)
	}

	syscallGetpgid = func(pid int) (int, error) { return pid, nil }
	syscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	if err := killProcess(cmd); err != nil {
		t.Fatalf("kill ESRCH should map to nil, got %v", err)
	}

	syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }
	if err := interruptProcess(cmd); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("kill EPERM should propagate, got %v", err)
	}
}

func TestOutputWaitsForDescendantTreeQuiescence(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "amp")
	err := os.WriteFile(script, []byte("#!/bin/sh\nsetsid sh -c 'trap \"\" INT TERM HUP; echo $$ > \"$AMP_CHILD_PID_FILE\"; while :; do sleep 1; done' &\nwhile [ ! -s \"$AMP_CHILD_PID_FILE\" ]; do sleep 0.01; done\nexit 0\n"), 0o700)
	if err != nil {
		t.Fatal(err)
	}

	client := NewClient(nil, Options{
		CLIPath: script,
		Cwd:     dir,
		Env:     map[string]string{"AMP_CHILD_PID_FILE": pidFile},
	})
	if _, outputErr := client.outputRaw(t.Context(), "descendant"); outputErr != nil {
		t.Fatalf("contained output: %v", outputErr)
	}

	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatal(err)
	}
	if processPIDAlive(pid) {
		t.Fatalf("descendant pid %d survived successful command return", pid)
	}
}

func TestOutputCancellationTerminatesContainedTree(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "amp")
	err := os.WriteFile(script, []byte("#!/bin/sh\nsetsid sh -c 'trap \"\" INT TERM HUP; echo $$ > \"$AMP_CHILD_PID_FILE\"; while :; do sleep 1; done' &\nwhile [ ! -s \"$AMP_CHILD_PID_FILE\" ]; do sleep 0.01; done\ntrap '' INT TERM HUP\nwhile :; do sleep 1; done\n"), 0o700)
	if err != nil {
		t.Fatal(err)
	}

	client := NewClient(nil, Options{
		CLIPath: script,
		Cwd:     dir,
		Env:     map[string]string{"AMP_CHILD_PID_FILE": pidFile},
	})
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	if _, outputErr := client.outputRaw(cancelled, "cancelled-before-start"); !errors.Is(outputErr, context.Canceled) {
		t.Fatalf("pre-start cancellation = %v, want context.Canceled", outputErr)
	}

	running, stop := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer stop()

	if _, outputErr := client.outputRaw(running, "cancel-running"); outputErr == nil {
		t.Fatal("running cancellation unexpectedly succeeded")
	}

	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatal(err)
	}
	if processPIDAlive(pid) {
		t.Fatalf("setsid descendant pid %d survived cancelled command return", pid)
	}
}

func TestProcessTreeQuiescenceFailureBranches(t *testing.T) {
	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })

	if err := (*processTree)(nil).terminateAndWait(time.Millisecond); err != nil {
		t.Fatalf("nil tree: %v", err)
	}
	if err := signalProcessGroupID(0, syscall.SIGKILL); err != nil {
		t.Fatalf("zero process group: %v", err)
	}
	if !ProcessTreeQuiescent(nil) || ProcessTreeQuiescent(ErrProcessTreeNotQuiescent) {
		t.Fatal("process-tree quiescence classification mismatch")
	}

	tree := &processTree{pgid: 12345}
	syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }
	if err := tree.terminateAndWait(time.Millisecond); !errors.Is(err, ErrProcessTreeNotQuiescent) {
		t.Fatalf("terminate failure = %v", err)
	}

	calls := 0
	syscallKill = func(_ int, signal syscall.Signal) error {
		calls++
		if signal == syscall.Signal(0) {
			return syscall.ESRCH
		}

		return nil
	}
	if err := tree.terminateAndWait(time.Second); err != nil {
		t.Fatalf("quiescent group: %v", err)
	}
	if calls != 2 {
		t.Fatalf("syscall calls = %d, want kill plus probe", calls)
	}

	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == syscall.Signal(0) {
			return syscall.EINVAL
		}

		return nil
	}
	if err := tree.terminateAndWait(time.Second); !errors.Is(err, ErrProcessTreeNotQuiescent) {
		t.Fatalf("probe failure = %v", err)
	}

	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == syscall.Signal(0) {
			return syscall.EPERM
		}

		return nil
	}
	if err := tree.terminateAndWait(time.Millisecond); !errors.Is(err, ErrProcessTreeNotQuiescent) {
		t.Fatalf("live group timeout = %v", err)
	}
}

func processPIDAlive(pid int) bool {
	err := syscall.Kill(pid, 0)

	return err == nil || errors.Is(err, syscall.EPERM)
}

func TestInterruptReturnsSignalError(t *testing.T) {
	originalGetpgid := syscallGetpgid
	t.Cleanup(func() { syscallGetpgid = originalGetpgid })

	syscallGetpgid = func(int) (int, error) { return 0, syscall.EPERM }

	turn := &Turn{cmd: &exec.Cmd{Process: &os.Process{Pid: 12345}}}
	if err := turn.Interrupt(context.Background(), time.Second); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Interrupt should propagate signal error, got %v", err)
	}
}
