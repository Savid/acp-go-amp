//go:build darwin

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDarwinOutputReportsWaiterPastContainmentDeadline(t *testing.T) {
	originalPrepare := prepareProcessTree
	originalTerminate := processTreeTerminateAndWait
	originalKill := syscallKill
	originalTimeout := commandWaitTimeout
	originalCommand := commandContext
	t.Cleanup(func() {
		prepareProcessTree = originalPrepare
		processTreeTerminateAndWait = originalTerminate
		syscallKill = originalKill
		commandWaitTimeout = originalTimeout
		commandContext = originalCommand
	})

	prepareProcessTree = func(cmd *exec.Cmd, _ processLaunchOptions) (*processTreeCommand, error) {
		configureCommand(cmd)

		return &processTreeCommand{cmd: cmd}, nil
	}
	processTreeTerminateAndWait = func(*processTree, time.Duration) error { return nil }
	syscallKill = func(int, syscall.Signal) error { return nil }
	commandWaitTimeout = time.Millisecond

	var launched *exec.Cmd

	commandContext = func(ctx context.Context, path string, args ...string) *exec.Cmd {
		launched = exec.CommandContext(ctx, path, args...)

		return launched
	}

	path, state := fakeAmpPath(t, "hang-list")
	client := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.outputRaw(ctx, ampArgThreads, "list")
		done <- err
	}()
	argsPath := filepath.Join(state, "args.jsonl")
	deadline := time.After(3 * time.Second)
	for {
		if _, statErr := os.Stat(argsPath); statErr == nil {
			break
		}

		select {
		case earlyErr := <-done:
			t.Fatalf("output returned before helper readiness: %v", earlyErr)
		case <-deadline:
			t.Fatalf("%s was not created", argsPath)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	err := <-done
	if !errors.Is(err, ErrProcessContainmentIncomplete) || !strings.Contains(err.Error(), "wait for contained Amp command") {
		t.Fatalf("post-containment waiter error = %v", err)
	}
	if launched != nil && launched.Process != nil {
		_ = originalKill(-launched.Process.Pid, syscall.SIGKILL)
	}
}
