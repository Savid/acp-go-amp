//go:build linux

package amp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// launchResFailingWriter refuses every write, which is how a case makes the
// supervisor's completion or readiness channel stop accepting the proof it is
// trying to publish.
type launchResFailingWriter struct{ err error }

func (w launchResFailingWriter) Write([]byte) (int, error) { return 0, w.err }

// launchResDescriptors snapshots the descriptors this process holds so a
// refusal can be proven to release everything the launch opened on the way to
// it.
func launchResDescriptors(t *testing.T) map[string]struct{} {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	open := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		open[entry.Name()] = struct{}{}
	}

	return open
}

func launchResAssertNoLeak(t *testing.T, before map[string]struct{}) {
	t.Helper()
	for name := range launchResDescriptors(t) {
		if _, held := before[name]; !held {
			link, _ := os.Readlink("/proc/self/fd/" + name)
			t.Fatalf("refusal path leaked descriptor %s -> %s", name, link)
		}
	}
}

// TestLaunchResPreparationRefusesAnUnverifiableIsolation proves that when a
// launch wrapper hands back a command the platform did not isolate natively,
// the adapter applies the isolation policy itself and abandons the launch if it
// cannot. Reporting a prepared launch whose credentials were never applied
// would run the agent under the adapter's own identity while the caller
// believed it was contained.
func TestLaunchResPreparationRefusesAnUnverifiableIsolation(t *testing.T) {
	originalPrepare := prepareProcessTree
	t.Cleanup(func() { prepareProcessTree = originalPrepare })
	prepareProcessTree = func(cmd *exec.Cmd, options processLaunchOptions) (*processTreeCommand, error) {
		launch, err := originalPrepare(cmd, options)
		if err != nil {
			return nil, err
		}
		// Stand in for a platform whose launch wrapper carries no native
		// isolation, which is the only shape that reaches the adapter's own
		// application of the policy.
		launch.nativeIsolation = false

		return launch, nil
	}

	groupsErr := errors.New("no supplementary groups")
	originalGroups := processIsolationGetgroups
	t.Cleanup(func() { processIsolationGetgroups = originalGroups })
	processIsolationGetgroups = func() ([]int, error) { return nil, groupsErr }

	isolation := testProcessIsolation()
	isolation.TestOnlyNoCredential = false
	originalEUID, originalEGID := processIsolationGeteuid, processIsolationGetegid
	t.Cleanup(func() {
		processIsolationGeteuid = originalEUID
		processIsolationGetegid = originalEGID
	})
	// Report the process as already running under the complete standalone policy
	// identity. This reaches verification rather than installing a credential,
	// so the supplementary-group failure is the refusal under test.
	processIsolationGeteuid = func() int { return int(isolation.UID) }
	processIsolationGetegid = func() int { return int(isolation.GID) }
	isolation.TestOnlyIdentityLockRoot = t.TempDir()
	isolation.StandaloneOwnerID = "launch-residual-unverifiable"
	isolation.StandaloneStateRoot = "/var/tmp/acp-go-amp-launch-residual"
	client := NewClient(nil, Options{CLIPath: "/bin/true", Isolation: isolation})

	before := launchResDescriptors(t)
	launch, err := client.prepareProcessLaunch(t.Context(), exec.Command("/bin/true"))
	require.Nil(t, launch)
	require.ErrorIs(t, err, groupsErr)
	require.ErrorContains(t, err, "apply Amp process isolation")
	launchResAssertNoLeak(t, before)
}

func TestLaunchResRefusesMixedDarwinAndIsolationPolicies(t *testing.T) {
	isolation := testProcessIsolation()
	isolation.StandaloneOwnerID = "launch-residual-mixed-policy"
	isolation.StandaloneStateRoot = "/var/tmp/acp-go-amp-launch-residual-mixed-policy"
	client := NewClient(nil, Options{DarwinBestEffort: true, Isolation: isolation})

	launch, err := client.prepareProcessLaunch(t.Context(), exec.Command("/bin/true"))
	require.Nil(t, launch)
	require.ErrorContains(t, err, "cannot be combined")
}

func TestLaunchResOutputReportsWaiterPastContainmentDeadline(t *testing.T) {
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
	client := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.outputWithArgs(ctx, ampArgThreads, "list")
		done <- err
	}()
	waitForFile(t, filepath.Join(state, "args.jsonl"))
	cancel()
	err := <-done
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	require.ErrorContains(t, err, "wait for contained Amp command")
	if launched != nil && launched.Process != nil {
		_ = originalKill(-launched.Process.Pid, syscall.SIGKILL)
	}
}

// TestLaunchResOutputAbandonsACancelledOrUnstartableLaunch proves the
// one-shot command path refuses between preparing a launch and starting it. A
// caller who cancelled while the launch was being prepared must not have the
// agent started behind them, and a launch that cannot start must be reported
// against the arguments that asked for it rather than swallowed.
func TestLaunchResOutputAbandonsACancelledOrUnstartableLaunch(t *testing.T) {
	originalPrepare := prepareProcessTree
	t.Cleanup(func() { prepareProcessTree = originalPrepare })

	t.Run("cancelled while preparing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		prepareProcessTree = func(cmd *exec.Cmd, options processLaunchOptions) (*processTreeCommand, error) {
			launch, err := originalPrepare(cmd, options)
			if err != nil {
				return nil, err
			}
			cancel()

			return launch, nil
		}

		before := launchResDescriptors(t)
		output, err := newTestClient(t, nil, Options{CLIPath: "/bin/true", Cwd: t.TempDir()}).
			outputWithArgs(ctx, "--version")
		require.Nil(t, output)
		require.ErrorIs(t, err, context.Canceled)
		launchResAssertNoLeak(t, before)
	})

	t.Run("native root cannot be started", func(t *testing.T) {
		absent := "/nonexistent/acp-go-amp-launch-res"
		prepareProcessTree = func(cmd *exec.Cmd, options processLaunchOptions) (*processTreeCommand, error) {
			launch, err := originalPrepare(cmd, options)
			if err != nil {
				return nil, err
			}
			// The command the supervisor will exec is replaced with one no
			// kernel can start, so the refusal happens at the start itself
			// rather than anywhere the adapter could have caught earlier.
			launch.cmd.Path = absent
			launch.cmd.Args = []string{absent}

			return launch, nil
		}

		before := launchResDescriptors(t)
		output, err := newTestClient(t, nil, Options{CLIPath: "/bin/true", Cwd: t.TempDir()}).
			outputWithArgs(t.Context(), "--version")
		require.Nil(t, output)
		require.ErrorContains(t, err, "amp --version")
		require.ErrorIs(t, err, os.ErrNotExist)
		launchResAssertNoLeak(t, before)
	})
}

// TestLaunchResAuthLoginAbandonsAnUnstartableLaunch proves the login flow
// releases everything it staged when the process tree will not start: the
// stdio pipes it opened and the browser shim it generated on disk. A shim left
// behind is a directory the isolated identity still owns with no login that
// will ever use it.
func TestLaunchResAuthLoginAbandonsAnUnstartableLaunch(t *testing.T) {
	absent := "/nonexistent/acp-go-amp-login-res"
	originalPrepare := prepareProcessTree
	t.Cleanup(func() { prepareProcessTree = originalPrepare })
	prepareProcessTree = func(cmd *exec.Cmd, options processLaunchOptions) (*processTreeCommand, error) {
		launch, err := originalPrepare(cmd, options)
		if err != nil {
			return nil, err
		}
		launch.cmd.Path = absent
		launch.cmd.Args = []string{absent}

		return launch, nil
	}

	path, _ := fakeAmpPath(t, "login")
	scratch := makeInstalledAmpScratchParent(t)
	client := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir(), ScratchParent: scratch})

	before := launchResDescriptors(t)
	login, err := client.StartAuthLogin(t.Context())
	require.Nil(t, login)
	require.ErrorContains(t, err, "start amp login")
	require.ErrorIs(t, err, os.ErrNotExist)
	launchResAssertNoLeak(t, before)

	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Empty(t, entries, "the refused login must remove the browser shim it generated")
}

// TestLaunchResStartAbandonsALaunchItCannotValidate proves the shared unix
// start path abandons a launch whose best-effort generation refuses, rather
// than handing back a tree it never validated. The Linux implementation of that
// validation is a constant nil, so the guard is unreachable from any Linux
// input even though it is live on Darwin; reaching it through the seam is what
// holds the shared caller to the contract.
func TestLaunchResStartAbandonsALaunchItCannotValidate(t *testing.T) {
	want := errors.New("generation is not usable")
	original := processTreeValidateLaunch
	t.Cleanup(func() { processTreeValidateLaunch = original })
	// The real validator releases the paused waiter on every refusal path.
	// Capturing the release rather than calling it here keeps this case's own
	// reap of the child, and running it at cleanup ends the waiter's goroutine
	// with the test instead of parking it on its start gate for the whole run.
	release := func() {}
	t.Cleanup(func() { release() })
	processTreeValidateLaunch = func(_ *processTreeCommand, tree *processTree, beginWait func()) error {
		release = func() {
			beginWait()
			<-tree.waiter.done
		}

		return want
	}

	launch := &processTreeCommand{cmd: exec.Command("/bin/true")}
	tree, err := startProcessTree(launch)
	require.Nil(t, tree)
	require.ErrorIs(t, err, want)
	require.NotNil(t, launch.cmd.Process, "the case must have reached the validation after the start")
	require.NoError(t, launch.cmd.Wait())
}

// TestLaunchResSupervisorCompletionProofIsNeverAssumed proves the parent's wait
// on a supervised tree only reports containment when the supervisor actually
// said so. Without a completion channel, with a channel that will not answer,
// or with an answer that is not the completion token, the wait must report the
// containment as incomplete and still surface whatever the process itself did.
func TestLaunchResSupervisorCompletionProofIsNeverAssumed(t *testing.T) {
	waitErr := errors.New("native exited badly")
	wait := func() error { return waitErr }

	t.Run("no completion channel", func(t *testing.T) {
		err := awaitTurnSupervisorCompletion(wait, nil)
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
		require.ErrorIs(t, err, waitErr)
	})

	t.Run("completion channel never answers", func(t *testing.T) {
		read, write, pipeErr := os.Pipe()
		require.NoError(t, pipeErr)
		require.NoError(t, write.Close())

		err := awaitTurnSupervisorCompletion(wait, read)
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
		require.ErrorIs(t, err, waitErr)
		require.ErrorContains(t, err, "await Amp liveness completion proof")
	})

	t.Run("completion channel answers with something else", func(t *testing.T) {
		read, write, pipeErr := os.Pipe()
		require.NoError(t, pipeErr)
		_, _ = write.WriteString("incomplete\n")
		require.NoError(t, write.Close())

		err := awaitTurnSupervisorCompletion(wait, read)
		require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
		require.ErrorIs(t, err, waitErr)
		require.ErrorContains(t, err, `invalid Amp liveness completion proof "incomplete"`)
	})

	t.Run("completion channel answers with the token", func(t *testing.T) {
		read, write, pipeErr := os.Pipe()
		require.NoError(t, pipeErr)
		_, _ = write.WriteString("complete\n")
		require.NoError(t, write.Close())

		err := awaitTurnSupervisorCompletion(wait, read)
		require.ErrorIs(t, err, waitErr)
		require.NotErrorIs(t, err, ErrProcessContainmentIncomplete,
			"a proven containment must not also report itself incomplete",
		)
	})
}

// TestLaunchResNativeCompletionIsPublishedOnlyWhenContained proves the liveness
// core publishes its completion proof exactly when it actually contained the
// tree, and reports a failure to publish rather than losing it. The managed
// root retains the turn's resources until that proof arrives, so a proof
// published without containment would release them early and one silently
// dropped would retain them forever.
func TestLaunchResNativeCompletionIsPublishedOnlyWhenContained(t *testing.T) {
	config := func(t *testing.T) (*os.File, *os.File) {
		t.Helper()
		peerRead, peerWrite, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() { _ = peerRead.Close() })
		// A guardian peer whose write end is already gone reads as a guardian
		// that exited before the native launch, which is the refusal that
		// decides containment here.
		require.NoError(t, peerWrite.Close())

		return peerRead, peerWrite
	}

	t.Run("containment failed, so nothing is published", func(t *testing.T) {
		restoreTurnSupervisorSeams(t)
		peerRead, _ := config(t)
		turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
		turnSupervisorSignalStop = func(chan<- os.Signal) {}
		turnSupervisorContain = func(int, int) error { return errors.New("cannot contain") }

		var completion bytes.Buffer
		err := runTurnSupervisorNative(
			encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/true", Args: []string{"/bin/true"}}),
			[]io.Reader{strings.NewReader("")}, peerRead, io.Discard, &completion, 6, 7, true,
		)
		require.ErrorContains(t, err, "guardian exited before native launch")
		require.Empty(t, completion.String(),
			"a turn whose tree was not contained must not claim completion",
		)
	})

	t.Run("containment succeeded but the proof cannot be published", func(t *testing.T) {
		restoreTurnSupervisorSeams(t)
		peerRead, _ := config(t)
		turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
		turnSupervisorSignalStop = func(chan<- os.Signal) {}
		turnSupervisorContain = func(int, int) error { return nil }

		publishErr := errors.New("completion channel is gone")
		err := runTurnSupervisorNative(
			encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/true", Args: []string{"/bin/true"}}),
			[]io.Reader{strings.NewReader("")}, peerRead, io.Discard,
			launchResFailingWriter{err: publishErr}, 6, 7, true,
		)
		require.ErrorContains(t, err, "guardian exited before native launch")
		require.ErrorIs(t, err, publishErr)
		require.ErrorContains(t, err, "publish Amp liveness completion")
	})
}

// TestLaunchResGuardianAcceptsALivenessThatReportedDone proves the guardian
// takes its liveness child's own completion report as the end of the turn: when
// the child publishes readiness, then "done", then exits, the guardian returns
// the child's wait result without containing anything and without claiming a
// completion of its own. This is the ordinary end of a supervised turn, and a
// guardian that contained the tree here would kill a process group the liveness
// child had already accounted for.
func TestLaunchResGuardianAcceptsALivenessThatReportedDone(t *testing.T) {
	guardian := turnSupervisorCovNewGuardian(t)
	guardian.liveness(t, `printf 'ready:%d\ndone\n' "$$" >&5`)
	var ready strings.Builder

	require.NoError(t, runTurnSupervisorGuardian(
		turnSupervisorCovGuardianConfig(t), guardian.controlRead, &ready,
	))
	require.Equal(t, turnSupervisorReady, ready.String(),
		"a guardian whose liveness reported done must still have published readiness upstream",
	)
	require.Empty(t, guardian.contained,
		"a liveness child that reported done has already accounted for the tree",
	)
	require.Empty(t, guardian.completion(t),
		"the guardian must not claim a completion its liveness child already reported",
	)
}
