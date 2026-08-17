//go:build !unix

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The portable backend has no POSIX shell and no signal it can deliver to
// another process, so this mirror compiles the child it needs rather than
// assuming any system executable or script interpreter exists. Every test here
// is named TestPortable* so the runtime lane can select the whole class on a
// host that actually executes this file.

const portableHarnessSource = `package main

import (
	"fmt"
	"os"
	"time"
)

func main() {
	mode := "exit"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	if record := os.Getenv("PORTABLE_HARNESS_RECORD"); record != "" {
		file, err := os.OpenFile(record, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			os.Exit(3)
		}
		fmt.Fprintf(file, "%s\n", mode)
		_ = file.Close()
	}

	switch mode {
	case "live":
		if ready := os.Getenv("PORTABLE_HARNESS_READY"); ready != "" {
			_ = os.WriteFile(ready, []byte("1"), 0o600)
		}
		time.Sleep(10 * time.Minute)
	case "environment":
		fmt.Print(os.Getenv("PATH") + "\n" + os.Getenv("PATHEXT"))
	default:
		fmt.Print("ordinary:" + os.Getenv("PORTABLE_HARNESS_CANARY"))
	}
}
`

// portableHarness compiles the fake Amp harness once per test and returns its
// path plus the spawn ledger every launch appends to. The ledger is the spawn
// counter: a refusal that never spawns leaves it absent.
func portableHarness(t *testing.T) (string, string) {
	t.Helper()

	dir := t.TempDir()
	source := filepath.Join(dir, "harness.go")
	if err := os.WriteFile(source, []byte(portableHarnessSource), 0o600); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(dir, "harness")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}

	build := exec.Command("go", "build", "-o", binary, source)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build portable harness: %v\n%s", err, out)
	}

	return binary, filepath.Join(dir, "spawns.log")
}

func portableSpawnCount(t *testing.T, ledger string) int {
	t.Helper()

	data, err := os.ReadFile(ledger)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}

	return len(strings.Fields(string(data)))
}

func portableOrdinaryClient(t *testing.T, harness, ledger, canary string) *Client {
	t.Helper()

	return NewClient(nil, Options{
		CLIPath: harness,
		Cwd:     filepath.Dir(harness),
		OrdinaryEnvironment: map[string]string{
			"PATH":                    filepath.Dir(harness),
			"PORTABLE_HARNESS_RECORD": ledger,
			"PORTABLE_HARNESS_CANARY": canary,
		},
	})
}

// portableTree starts an ordinary portable launch of the harness and returns
// the tree together with the command it owns.
func portableTree(t *testing.T, harness string, environment []string, mode string) (*processTree, *exec.Cmd) {
	t.Helper()

	cmd := exec.Command(harness, mode)
	cmd.Dir = filepath.Dir(harness)
	cmd.Env = environment

	launch, err := prepareProcessTreeCommand(cmd, processLaunchOptions{})
	if err != nil {
		t.Fatalf("prepare ordinary portable launch: %v", err)
	}

	tree, err := startProcessTree(launch)
	if err != nil {
		t.Fatalf("start ordinary portable tree: %v", err)
	}

	return tree, launch.cmd
}

func portableEnvironment(client *Client, t *testing.T) []string {
	t.Helper()

	environment, err := client.buildEnvironment(nil, client.options.Cwd)
	if err != nil {
		t.Fatal(err)
	}

	return environment
}

func portableWaitReady(t *testing.T, ready string) {
	t.Helper()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if _, err := os.Stat(ready); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect portable harness readiness: %v", err)
		}

		select {
		case <-deadline.C:
			t.Fatal("portable harness did not become ready")
		case <-ticker.C:
		}
	}
}

// TestPortableOrdinaryLaunchRunsCompletesAndKeepsTheAmbientEnvironment is the
// portable runtime lane's ordinary launch proof: a real child runs as the
// current identity, carries the ordinary environment, completes, and claims no
// isolation authority.
func TestPortableOrdinaryLaunchRunsCompletesAndKeepsTheAmbientEnvironment(t *testing.T) {
	harness, ledger := portableHarness(t)
	client := portableOrdinaryClient(t, harness, ledger, "ambient-value")

	out, err := client.outputWithArgs(t.Context(), "version")
	if err != nil {
		t.Fatalf("ordinary portable launch: %v", err)
	}
	if got, want := string(out), "ordinary:ambient-value"; got != want {
		t.Fatalf("ordinary portable output = %q, want %q", got, want)
	}
	if spawns := portableSpawnCount(t, ledger); spawns != 1 {
		t.Fatalf("ordinary portable spawns = %d, want 1", spawns)
	}
	if client.options.Isolation != nil {
		t.Fatalf("ordinary portable launch fabricated policy %#v", client.options.Isolation)
	}

	launch, err := client.prepareProcessLaunch(t.Context(), exec.Command(harness))
	if err != nil {
		t.Fatalf("prepare ordinary portable launch: %v", err)
	}
	defer launch.close()
	if launch.nativeIsolation || launch.control != nil || len(launch.inherited) != 0 {
		t.Fatalf("ordinary portable launch acquired isolation authority: %#v", launch)
	}
}

// TestPortableCancelTerminatesLiveChildBoundedly pins the cancellation
// contract this platform can actually keep: a live child is terminated within
// the bounded wait, and the cancel reports success.
func TestPortableCancelTerminatesLiveChildBoundedly(t *testing.T) {
	harness, ledger := portableHarness(t)
	client := portableOrdinaryClient(t, harness, ledger, "cancel")
	ready := filepath.Join(filepath.Dir(harness), "cancel.ready")
	client.options.OrdinaryEnvironment["PORTABLE_HARNESS_READY"] = ready
	environment := portableEnvironment(client, t)

	tree, cmd := portableTree(t, harness, environment, "live")
	turn := &Turn{cmd: cmd, tree: tree}
	portableWaitReady(t, ready)

	started := time.Now()
	if err := turn.Interrupt(t.Context(), defaultCloseKillAfter); err != nil {
		t.Fatalf("portable cancel of a live child = %v, want nil", err)
	}
	if elapsed := time.Since(started); elapsed >= defaultCloseWait {
		t.Fatalf("portable cancel took %s, want a bounded termination under %s", elapsed, defaultCloseWait)
	}
	if _, completed := tree.commandWait().await(t.Context()); !completed {
		t.Fatal("portable cancel left the direct child unreaped")
	}
	// Close reports the child's own termination status verbatim; what it must
	// not do is fail the containment boundary.
	if err := turn.Close(); !expectedExit(err) {
		t.Fatalf("close after portable cancel = %v", err)
	}
	if spawns := portableSpawnCount(t, ledger); spawns != 1 {
		t.Fatalf("portable cancel spawns = %d, want 1", spawns)
	}
}

// TestPortableCloseAfterSettlementSendsNoTermination is the no-post-settlement
// signal proof: once the direct child has been reaped its handle is never
// touched again, and closing the settled turn is clean.
func TestPortableCloseAfterSettlementSendsNoTermination(t *testing.T) {
	harness, ledger := portableHarness(t)
	client := portableOrdinaryClient(t, harness, ledger, "settled")
	environment := portableEnvironment(client, t)

	tree, cmd := portableTree(t, harness, environment, "exit")
	if _, completed := tree.commandWait().await(t.Context()); !completed {
		t.Fatal("portable child did not settle")
	}

	var terminations atomic.Int64

	original := killProcessHandle
	t.Cleanup(func() { killProcessHandle = original })
	killProcessHandle = func(process *os.Process) error {
		terminations.Add(1)

		return original(process)
	}

	turn := &Turn{cmd: cmd, tree: tree}
	if err := turn.Close(); err != nil {
		t.Fatalf("close of an already-settled portable turn = %v, want nil", err)
	}
	if got := terminations.Load(); got != 0 {
		t.Fatalf("settled portable turn attempted %d terminations, want 0", got)
	}

	// The same gate holds for every entry point, not just Close.
	if err := tree.interrupt(); err != nil || terminations.Load() != 0 {
		t.Fatalf("settled interrupt = %v, terminations = %d", err, terminations.Load())
	}
	if err := tree.kill(); err != nil || terminations.Load() != 0 {
		t.Fatalf("settled kill = %v, terminations = %d", err, terminations.Load())
	}
}

// TestPortableExplicitProcessIsolationIsRefusedWithoutSpawn pins the
// fail-closed half: an explicit policy is refused on a platform that cannot
// honour it, nothing is spawned, and no ordinary launch is retried in its
// place.
func TestPortableExplicitProcessIsolationIsRefusedWithoutSpawn(t *testing.T) {
	harness, ledger := portableHarness(t)
	policy := &ProcessIsolation{
		UID: 20001, GID: 20001,
		BaseEnvironment: map[string]string{
			"PATH":                    filepath.Dir(harness),
			"PORTABLE_HARNESS_RECORD": ledger,
		},
		StandaloneOwnerID:   "portable-refusal",
		StandaloneStateRoot: filepath.Dir(harness),
	}

	client := NewClient(nil, Options{CLIPath: harness, Cwd: filepath.Dir(harness), Isolation: policy})
	if _, err := client.outputWithArgs(t.Context(), "version"); err == nil {
		t.Fatal("explicit portable policy launched")
	}
	if _, err := client.startTurn(t.Context(), nil, nil); err == nil {
		t.Fatal("explicit portable turn launched")
	}
	if _, err := prepareProcessTreeCommand(exec.Command(harness), processLaunchOptions{Isolation: policy}); err == nil {
		t.Fatal("explicit portable process tree prepared")
	}
	if _, err := prepareProcessTreeCommand(exec.Command(harness), processLaunchOptions{DarwinBestEffort: true}); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("portable Darwin best-effort = %v", err)
	}
	if spawns := portableSpawnCount(t, ledger); spawns != 0 {
		t.Fatalf("refused explicit portable policy spawned %d children, want 0", spawns)
	}
}

// TestPortableProcessTreeLifecycleBranches covers the remaining portable
// lifecycle statements: the nil and already-finished guards, the
// os.ErrProcessDone/EINVAL tolerance, and both terminateAndWait outcomes.
func TestPortableProcessTreeLifecycleBranches(t *testing.T) {
	if count, ok := (*processTree)(nil).descendantCount(); count != 0 || ok {
		t.Fatalf("portable descendant inventory = %d, %v", count, ok)
	}
	configureCommand(exec.Command("unused"))

	var nilTree *processTree
	if nilTree.commandWait() != nil {
		t.Fatal("nil portable tree published a waiter")
	}
	if nilTree.settled() {
		t.Fatal("nil portable tree reported settlement")
	}
	if err := nilTree.interrupt(); err != nil {
		t.Fatalf("nil portable interrupt = %v", err)
	}
	if err := nilTree.kill(); err != nil {
		t.Fatalf("nil portable kill = %v", err)
	}
	if err := nilTree.terminateAndWait(time.Millisecond); err != nil {
		t.Fatalf("nil portable terminate = %v", err)
	}
	if err := nilTree.finish(nil); err != nil {
		t.Fatalf("nil portable finish = %v", err)
	}

	processless := &processTree{waiter: &commandWait{done: make(chan struct{})}}
	if err := processless.interrupt(); err != nil {
		t.Fatalf("process-free portable interrupt = %v", err)
	}
	if err := processless.kill(); err != nil {
		t.Fatalf("process-free portable kill = %v", err)
	}
	if processless.settled() {
		t.Fatal("live portable waiter reported settlement")
	}

	waiterless := &processTree{}
	if waiterless.settled() {
		t.Fatal("waiterless portable tree reported settlement")
	}

	if err := interruptProcess(nil); err != nil {
		t.Fatalf("nil portable command interrupt = %v", err)
	}
	if err := killProcess(nil); err != nil {
		t.Fatalf("nil portable command kill = %v", err)
	}
	if err := killProcess(&exec.Cmd{}); err != nil {
		t.Fatalf("process-free portable command kill = %v", err)
	}

	harness, ledger := portableHarness(t)
	client := portableOrdinaryClient(t, harness, ledger, "branches")
	environment := portableEnvironment(client, t)

	// A reaped child answers Kill with os.ErrProcessDone or EINVAL; the backend
	// reports the termination it asked for rather than retaining that error.
	settledTree, settledCmd := portableTree(t, harness, environment, "exit")
	if _, completed := settledTree.commandWait().await(t.Context()); !completed {
		t.Fatal("portable child did not settle")
	}
	if err := killProcessHandle(settledCmd.Process); err != nil {
		t.Fatalf("terminating a reaped portable child = %v, want nil", err)
	}
	if err := interruptProcess(settledCmd); err != nil {
		t.Fatalf("interrupting a reaped portable command = %v, want nil", err)
	}
	if err := settledTree.terminateAndWait(commandWaitTimeout); err != nil {
		t.Fatalf("terminate of a settled portable tree = %v", err)
	}

	// A tree whose waiter never publishes reports incomplete containment
	// rather than blocking the bounded caller.
	stuck := &processTree{waiter: &commandWait{done: make(chan struct{})}}
	if err := stuck.terminateAndWait(time.Millisecond); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("stuck portable terminate = %v", err)
	}
	if err := stuck.finish(nil); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("memoized portable finish = %v", err)
	}
}

// TestPortableStartProcessTreeFailureBranches covers the portable start path:
// a refused native acquisition, a failed exec start, startup cancellation, and
// a start error that must terminate the child it already created.
func TestPortableStartProcessTreeFailureBranches(t *testing.T) {
	harness, ledger := portableHarness(t)
	client := portableOrdinaryClient(t, harness, ledger, "start")
	environment := portableEnvironment(client, t)

	command := func(mode string) *exec.Cmd {
		cmd := exec.Command(harness, mode)
		cmd.Dir = filepath.Dir(harness)
		cmd.Env = environment

		return cmd
	}

	acquireErr := errors.New("native root refused")
	if _, err := startProcessTree(&processTreeCommand{
		cmd:           command("exit"),
		acquireNative: func() (func(), error) { return nil, acquireErr },
	}); !errors.Is(err, acquireErr) {
		t.Fatalf("portable acquire refusal = %v", err)
	}

	released := 0
	missing := exec.Command(filepath.Join(t.TempDir(), "absent"))
	if _, err := startProcessTree(&processTreeCommand{
		cmd:           missing,
		acquireNative: func() (func(), error) { return func() { released++ }, nil },
	}); err == nil || released != 1 {
		t.Fatalf("portable start failure = %v, releases = %d", err, released)
	}

	canceledStart := errors.New("start canceled")
	tree, err := startProcessTree(&processTreeCommand{
		cmd:           command("live"),
		onStartCancel: func(cancel func()) func() bool { cancel(); return func() bool { return false } },
		startError:    func() error { return canceledStart },
	})
	if tree != nil || !errors.Is(err, canceledStart) {
		t.Fatalf("portable startup cancellation = %v, %v", tree, err)
	}

	clean, err := startProcessTree(&processTreeCommand{
		cmd:           command("exit"),
		onStartCancel: func(func()) func() bool { return func() bool { return true } },
		startError:    func() error { return nil },
	})
	if err != nil {
		t.Fatalf("portable clean start = %v", err)
	}
	if terminateErr := clean.terminateAndWait(commandWaitTimeout); terminateErr != nil {
		t.Fatalf("portable clean terminate = %v", terminateErr)
	}
}

// TestPortableOrdinaryCancellationReleasesTheContext proves the ordinary
// bounded-command path cancels a live portable child instead of waiting out
// the whole close interval.
func TestPortableOrdinaryCancellationReleasesTheContext(t *testing.T) {
	harness, ledger := portableHarness(t)
	client := portableOrdinaryClient(t, harness, ledger, "context")
	ready := filepath.Join(filepath.Dir(harness), "context.ready")
	client.options.OrdinaryEnvironment["PORTABLE_HARNESS_READY"] = ready

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		_, err := client.outputWithArgs(ctx, "live")
		result <- err
	}()

	portableWaitReady(t, ready)
	started := time.Now()
	cancel()

	var err error
	select {
	case err = <-result:
	case <-time.After(defaultCloseWait):
		t.Fatal("cancelled portable command did not return")
	}
	if err == nil {
		t.Fatal("cancelled portable command succeeded")
	}
	if elapsed := time.Since(started); elapsed >= defaultCloseWait {
		t.Fatalf("cancelled portable command took %s, want a bounded termination", elapsed)
	}
	if spawns := portableSpawnCount(t, ledger); spawns != 1 {
		t.Fatalf("cancelled portable command spawns = %d, want 1", spawns)
	}
}
