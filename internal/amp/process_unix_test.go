//go:build unix

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
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
	if runtime.GOOS != "linux" {
		t.Skip("authoritative escaped-descendant containment is Linux-only")
	}
	if os.Geteuid() != 0 {
		t.Skip("authoritative escaped-descendant containment requires a trusted root supervisor")
	}
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "amp")
	err := os.WriteFile(script, []byte("#!/bin/sh\nsetsid sh -c 'trap \"\" INT TERM HUP; echo $$ > \"$AMP_CHILD_PID_FILE\"; while :; do sleep 1; done' &\nwhile [ ! -s \"$AMP_CHILD_PID_FILE\" ]; do sleep 0.01; done\nexit 0\n"), 0o700)
	if err != nil {
		t.Fatal(err)
	}

	client := newTestClient(t, nil, Options{
		CLIPath:   script,
		Cwd:       dir,
		Env:       map[string]string{"AMP_CHILD_PID_FILE": pidFile},
		Isolation: testProcessIsolation(),
	})
	if _, outputErr := client.outputWithArgs(t.Context(), "descendant"); outputErr != nil {
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
	if runtime.GOOS != "linux" {
		t.Skip("authoritative escaped-descendant containment is Linux-only")
	}
	if os.Geteuid() != 0 {
		t.Skip("authoritative escaped-descendant containment requires a trusted root supervisor")
	}
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "amp")
	err := os.WriteFile(script, []byte("#!/bin/sh\nsetsid sh -c 'trap \"\" INT TERM HUP; echo $$ > \"$AMP_CHILD_PID_FILE\"; while :; do sleep 1; done' &\nwhile [ ! -s \"$AMP_CHILD_PID_FILE\" ]; do sleep 0.01; done\ntrap '' INT TERM HUP\nwhile :; do sleep 1; done\n"), 0o700)
	if err != nil {
		t.Fatal(err)
	}

	client := newTestClient(t, nil, Options{
		CLIPath:   script,
		Cwd:       dir,
		Env:       map[string]string{"AMP_CHILD_PID_FILE": pidFile},
		Isolation: testProcessIsolation(),
	})
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	if _, outputErr := client.outputWithArgs(cancelled, "cancelled-before-start"); !errors.Is(outputErr, context.Canceled) {
		t.Fatalf("pre-start cancellation = %v, want context.Canceled", outputErr)
	}

	running, stop := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer stop()

	if _, outputErr := client.outputWithArgs(running, "cancel-running"); outputErr == nil {
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

func TestAuthoritativeProcessTreeFailureBranches(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("authoritative process-tree branch coverage is Linux-only")
	}
	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })

	if err := (*processTree)(nil).terminateAndWait(time.Millisecond); err != nil {
		t.Fatalf("nil tree: %v", err)
	}
	if err := signalProcessGroupID(0, syscall.SIGKILL); err != nil {
		t.Fatalf("zero process group: %v", err)
	}
	if !ProcessContainmentComplete(nil) || ProcessContainmentComplete(ErrProcessContainmentIncomplete) {
		t.Fatal("process containment classification mismatch")
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	tree := &processTree{pgid: 12345, supervised: true, control: writer}
	syscallKill = func(int, syscall.Signal) error { return nil }
	if err := tree.terminateAndWait(time.Millisecond); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("terminate failure = %v", err)
	}

	calls := 0
	tree = &processTree{pgid: 12345, supervised: true}
	syscallKill = func(_ int, signal syscall.Signal) error {
		calls++

		return syscall.ESRCH
	}
	if err := tree.terminateAndWait(time.Second); err != nil {
		t.Fatalf("complete group: %v", err)
	}
	if calls != 1 {
		t.Fatalf("syscall calls = %d, want one quiescence probe", calls)
	}

	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == syscall.Signal(0) {
			return syscall.EINVAL
		}

		return nil
	}
	tree = &processTree{pgid: 12345, supervised: true}
	if err := tree.terminateAndWait(time.Second); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("probe failure = %v", err)
	}

	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == syscall.Signal(0) {
			return syscall.EPERM
		}

		return nil
	}
	tree = &processTree{pgid: 12345, supervised: true}
	if err := tree.terminateAndWait(time.Millisecond); !errors.Is(err, ErrProcessContainmentIncomplete) {
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

func TestOrdinaryProcessTerminationBranches(t *testing.T) {
	if err := (*processTree)(nil).terminateOrdinary(time.Second); err != nil {
		t.Fatal(err)
	}

	completed := make(chan struct{})
	close(completed)
	released := 0
	tree := &processTree{
		waiter:        &commandWait{done: completed},
		releaseNative: func() { released++ },
	}
	if err := tree.terminateOrdinary(time.Second); err != nil || released != 1 {
		t.Fatalf("completed ordinary process = %v, releases=%d", err, released)
	}

	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })

	termDone := make(chan struct{})
	termReleased := 0
	syscallKill = func(int, syscall.Signal) error {
		select {
		case <-termDone:
		default:
			close(termDone)
		}

		return nil
	}
	tree = &processTree{
		pgid:          10,
		waiter:        &commandWait{done: termDone},
		releaseWaiter: func() { termReleased++ },
	}
	if err := tree.terminateOrdinary(20 * time.Millisecond); err != nil || termReleased != 1 {
		t.Fatalf("TERM ordinary process = %v, waiter releases=%d", err, termReleased)
	}

	killDone := make(chan struct{})
	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == syscall.SIGKILL {
			close(killDone)
		}

		return nil
	}
	tree = &processTree{pgid: 11, waiter: &commandWait{done: killDone}}
	if err := tree.terminateOrdinary(20 * time.Millisecond); err != nil {
		t.Fatalf("KILL ordinary process = %v", err)
	}

	syscallKill = func(int, syscall.Signal) error { return nil }
	tree = &processTree{pgid: 12, waiter: &commandWait{done: make(chan struct{})}}
	if err := tree.terminateOrdinary(0); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("unsettled ordinary process = %v", err)
	}
}

// TestOrdinarySignalsStopAfterDirectChildSettlement pins the settlement gate.
// An ordinary tree only knows the numeric process-group ID captured at launch;
// once the direct child is reaped the kernel may hand that number to a
// stranger, so no ordinary entry point may signal it again.
func TestOrdinarySignalsStopAfterDirectChildSettlement(t *testing.T) {
	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })

	var signals atomic.Int64

	syscallKill = func(int, syscall.Signal) error {
		signals.Add(1)

		return nil
	}

	reaped := make(chan struct{})
	close(reaped)

	settled := &processTree{pgid: 424242, waiter: &commandWait{done: reaped}}
	if err := settled.interrupt(); err != nil || signals.Load() != 0 {
		t.Fatalf("settled interrupt = %v, signals = %d", err, signals.Load())
	}
	if err := settled.kill(); err != nil || signals.Load() != 0 {
		t.Fatalf("settled kill = %v, signals = %d", err, signals.Load())
	}
	if err := settled.terminateOrdinary(time.Second); err != nil || signals.Load() != 0 {
		t.Fatalf("settled terminate = %v, signals = %d", err, signals.Load())
	}

	// A live child is still reachable: the gate suppresses stale signals, not
	// cancellation itself.
	live := &processTree{pgid: 424243, waiter: &commandWait{done: make(chan struct{})}}
	if err := live.interrupt(); err != nil || signals.Load() != 1 {
		t.Fatalf("live interrupt = %v, signals = %d", err, signals.Load())
	}
	if err := live.kill(); err != nil || signals.Load() != 2 {
		t.Fatalf("live kill = %v, signals = %d", err, signals.Load())
	}

	// A tree whose waiter has not been published yet has certainly not been
	// reaped, so it is never treated as settled.
	var nilTree *processTree
	if nilTree.settled() {
		t.Fatal("nil ordinary tree reported settlement")
	}
	if (&processTree{}).settled() {
		t.Fatal("waiterless ordinary tree reported settlement")
	}
}

// startOrdinaryTestTree launches a real ordinary child through the platform
// selector, which returns a direct command for a nil policy.
func startOrdinaryTestTree(t *testing.T, body string) (*processTree, *exec.Cmd) {
	t.Helper()

	dir := t.TempDir()
	script := filepath.Join(dir, "amp")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(script)
	cmd.Dir = dir

	launch, err := prepareProcessTreeCommand(cmd, processLaunchOptions{})
	if err != nil {
		t.Fatalf("prepare ordinary launch: %v", err)
	}

	tree, err := startProcessTree(launch)
	if err != nil {
		t.Fatalf("start ordinary tree: %v", err)
	}

	return tree, launch.cmd
}

// TestOrdinaryTurnCloseAfterSettlementIsSilent runs a real ordinary child to
// completion and then closes the turn. Turn.Close interrupts unconditionally,
// so this is the case that used to signal a reaped process group: the close
// must be clean and send nothing at all.
func TestOrdinaryTurnCloseAfterSettlementIsSilent(t *testing.T) {
	tree, cmd := startOrdinaryTestTree(t, "exit 0\n")
	if _, completed := tree.commandWait().await(t.Context()); !completed {
		t.Fatal("ordinary child did not settle")
	}

	var signals atomic.Int64

	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })
	syscallKill = func(pgid int, signal syscall.Signal) error {
		signals.Add(1)

		return originalKill(pgid, signal)
	}

	turn := &Turn{cmd: cmd, tree: tree}
	if err := turn.Close(); err != nil {
		t.Fatalf("close of an already-settled ordinary turn = %v", err)
	}
	if got := signals.Load(); got != 0 {
		t.Fatalf("settled ordinary close sent %d signals, want 0", got)
	}
}

// TestOrdinaryTurnCancelTerminatesLiveChild is the live half: cancellation
// still reaches a running ordinary child, reaps it, and reports success.
func TestOrdinaryTurnCancelTerminatesLiveChild(t *testing.T) {
	tree, cmd := startOrdinaryTestTree(t, "sleep 60\n")
	pid := cmd.Process.Pid

	turn := &Turn{cmd: cmd, tree: tree}
	if err := turn.Interrupt(t.Context(), defaultCloseKillAfter); err != nil {
		t.Fatalf("cancel of a live ordinary turn = %v", err)
	}
	if _, completed := tree.commandWait().await(t.Context()); !completed {
		t.Fatal("cancelled ordinary child was not reaped")
	}
	// Close reports the child's own termination status verbatim; what it must
	// not do is fail the containment boundary or signal the reaped group again.
	if err := turn.Close(); !expectedExit(err) {
		t.Fatalf("close after ordinary cancel = %v", err)
	}
	if processPIDAlive(pid) {
		t.Fatalf("cancelled ordinary child pid %d survived cancellation", pid)
	}
}
