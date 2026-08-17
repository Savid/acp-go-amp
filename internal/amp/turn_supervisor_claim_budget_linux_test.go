//go:build linux

package amp

import (
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// turnSupervisorBudgetRecordDeadlines captures every read deadline the
// supervisor arms, as a duration from the moment it was armed, while still
// arming it for real. The armed budget is the observable the lifecycle
// contract is written about, so a case can hold each bound to the interval it
// actually covers instead of to whatever literal happens to be in the source.
func turnSupervisorBudgetRecordDeadlines(t *testing.T) *[]time.Duration {
	t.Helper()
	previous := turnSupervisorReadDeadline
	t.Cleanup(func() { turnSupervisorReadDeadline = previous })
	armed := make([]time.Duration, 0, 2)
	turnSupervisorReadDeadline = func(file *os.File, deadline time.Time) error {
		if !deadline.IsZero() {
			armed = append(armed, time.Until(deadline))
		}

		return previous(file, deadline)
	}

	return &armed
}

// turnSupervisorBudgetDuplicate hands the supervisor its own descriptor for an
// inherited channel, so the supervisor closing the channel it was given does
// not close the end the test still reads.
func turnSupervisorBudgetDuplicate(t *testing.T, file *os.File, name string) *os.File {
	t.Helper()
	fd, err := unix.Dup(int(file.Fd()))
	require.NoError(t, err)
	duplicate := os.NewFile(uintptr(fd), name)
	t.Cleanup(func() { _ = duplicate.Close() })

	return duplicate
}

// TestTurnSupervisorBudgetsSeparateTheClaimSpanFromTheLivenessHandshake proves
// the two read deadlines around a native launch cover different intervals and
// are therefore sized differently.
//
// The parent's readiness wait is armed before the guardian child claims its
// standalone identity and is satisfied only after the claim has completed, so
// it spans the claim. Proving an identity vacant walks every task in the
// initial PID namespace until the task set is stable, twice, which costs what
// the host costs rather than what this process costs. A readiness bound
// shorter than the claim maximum would cancel a claim that was still making
// progress and report a containment failure that never happened, so it must
// clear the claim budget.
//
// The guardian's own read of the liveness readiness line sits behind the
// claim: by the time it is armed the guardian already holds its authority, and
// nothing it covers can still be proving an identity vacant. It is a handshake
// bound on a child that is already running and is deliberately not the claim
// maximum. Tying it to the claim maximum would make a dead liveness helper take
// six times longer to report.
func TestTurnSupervisorBudgetsSeparateTheClaimSpanFromTheLivenessHandshake(t *testing.T) {
	// The old readiness bound. A claim outrunning this must survive.
	const supersededReadinessBound = 5 * time.Second

	t.Run("the readiness wait outlives a claim that is still making progress", func(t *testing.T) {
		armed := turnSupervisorBudgetRecordDeadlines(t)
		read, write, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() { _ = write.Close() })

		go func() {
			// Stands in for a guardian child whose vacancy proof is still
			// walking /proc when the superseded bound would have expired.
			time.Sleep(supersededReadinessBound + time.Second)
			_, _ = io.WriteString(write, turnSupervisorReady)
		}()

		started := time.Now()
		err = awaitProcessTreeReady(&processTreeCommand{ready: read})
		elapsed := time.Since(started)
		require.NoError(t, err, "a launch whose claim outran the superseded bound must not be cancelled")
		require.Greater(t, elapsed, supersededReadinessBound,
			"the case must actually have held the readiness wait past the superseded bound",
		)
		require.Len(t, *armed, 1)
		require.Greater(t, (*armed)[0], agentStandaloneClaimMax-time.Second,
			"the readiness wait must clear the standalone claim maximum it sits in front of",
		)
		require.LessOrEqual(t, (*armed)[0], agentStandaloneClaimMax,
			"the readiness wait must be the claim maximum itself, not a separately tuned bound",
		)
	})

	t.Run("the guardian's post-claim liveness wait is a tighter bound", func(t *testing.T) {
		restoreTurnSupervisorSeams(t)
		armed := turnSupervisorBudgetRecordDeadlines(t)

		completionRead, completionWrite, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() { _ = completionRead.Close() })
		controlRead, controlWrite, err := os.Pipe()
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = controlRead.Close()
			_ = controlWrite.Close()
		})

		turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
		turnSupervisorSignalStop = func(chan<- os.Signal) {}
		turnSupervisorEnable = func() error { return nil }
		contained := make([]int, 0, 1)
		turnSupervisorContain = func(supervisorPID int, nativePID int) error {
			require.Equal(t, os.Getpid(), supervisorPID)
			contained = append(contained, nativePID)

			return nil
		}
		turnSupervisorOpenFile = func(fd uintptr, name string) *os.File {
			require.Equal(t, uintptr(6), fd, "the guardian must only inherit its completion channel here")

			return turnSupervisorBudgetDuplicate(t, completionWrite, name)
		}
		// A liveness helper that holds the readiness channel open without ever
		// reporting, then exits, so the guardian's wait is decided by its own
		// deadline rather than by the channel closing.
		turnSupervisorExecutable = func() (string, error) { return "/bin/sleep", nil }
		turnSupervisorCommand = func(string, ...string) *exec.Cmd {
			return exec.Command("/bin/sleep", "6")
		}

		config := encodeSupervisorConfig(t, turnSupervisorConfig{
			Path: "/bin/true", Args: []string{"/bin/true"}, Env: []string{"PATH=/usr/bin:/bin"},
		})
		started := time.Now()
		err = runTurnSupervisorGuardian(config, controlRead, io.Discard)
		elapsed := time.Since(started)
		require.ErrorContains(t, err, "await Amp liveness readiness")
		require.ErrorIs(t, err, os.ErrDeadlineExceeded,
			"the wait must have been decided by its own deadline, not by the helper exiting",
		)
		require.Less(t, elapsed, agentStandaloneClaimMax,
			"the post-claim liveness wait must not sit on the claim budget",
		)
		require.Len(t, *armed, 1)
		require.Less(t, (*armed)[0], agentStandaloneClaimMax,
			"the post-claim liveness wait is a handshake bound, not the standalone claim maximum",
		)
		require.Equal(t, []int{0}, contained,
			"a guardian that never learned a native pid must contain its own tree",
		)

		require.NoError(t, completionWrite.Close())
		payload, err := io.ReadAll(completionRead)
		require.NoError(t, err)
		require.Equal(t, "complete\n", string(payload),
			"the guardian contained its tree, so it must report the turn complete",
		)
	})
}
