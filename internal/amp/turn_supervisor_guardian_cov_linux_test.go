//go:build linux

package amp

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// turnSupervisorCovGuardian drives runTurnSupervisorGuardian on the exact
// descriptor layout a real launch hands it: an inherited completion channel on
// descriptor 6 and a control channel the guardian must be able to treat as an
// inheritable file. It records every containment the guardian requests so a
// test can prove both that containment happened and which pid it named.
type turnSupervisorCovGuardian struct {
	completionRead  *os.File
	completionWrite *os.File
	controlRead     *os.File
	controlWrite    *os.File
	drained         bool
	contained       []int
	containErr      error
}

func turnSupervisorCovNewGuardian(t *testing.T) *turnSupervisorCovGuardian {
	t.Helper()
	restoreTurnSupervisorSeams(t)
	deadline := turnSupervisorReadDeadline
	t.Cleanup(func() { turnSupervisorReadDeadline = deadline })

	guardian := &turnSupervisorCovGuardian{}
	var err error
	guardian.completionRead, guardian.completionWrite, err = os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	guardian.controlRead, guardian.controlWrite, err = os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = guardian.completionRead.Close()
		_ = guardian.completionWrite.Close()
		_ = guardian.controlRead.Close()
		_ = guardian.controlWrite.Close()
	})

	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorContain = func(supervisorPID int, nativePID int) error {
		if supervisorPID != os.Getpid() {
			t.Errorf("guardian containment named supervisor pid %d, want %d", supervisorPID, os.Getpid())
		}
		guardian.contained = append(guardian.contained, nativePID)

		return guardian.containErr
	}
	turnSupervisorCovInherit(t, map[uintptr]*os.File{6: guardian.completionWrite})

	return guardian
}

// liveness installs the liveness command the guardian will launch. The script
// runs with the guardian's private descriptors: 5 is the readiness/data
// channel the guardian reads, and 9 is the guardian peer that closes when the
// guardian abandons the turn.
func (g *turnSupervisorCovGuardian) liveness(t *testing.T, script string) {
	t.Helper()
	turnSupervisorExecutable = func() (string, error) { return "/bin/sh", nil }
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		if len(args) != 0 {
			t.Errorf("guardian launched liveness with unexpected arguments %v", args)
		}

		return exec.Command(name, "-c", script)
	}
}

func (g *turnSupervisorCovGuardian) completion(t *testing.T) string {
	t.Helper()
	if !g.drained {
		if err := g.completionWrite.Close(); err != nil {
			t.Fatal(err)
		}
		g.drained = true
	}
	payload, err := io.ReadAll(g.completionRead)
	if err != nil {
		t.Fatal(err)
	}

	return string(payload)
}

func turnSupervisorCovGuardianConfig(t *testing.T) io.Reader {
	t.Helper()

	return encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"}, Env: []string{"PATH=/usr/bin:/bin"},
	})
}

// TestTurnSupervisorCovGuardianRefusesBeforeItCanReportCompletion proves that
// a guardian which cannot obtain or protect its completion channel refuses
// without ever claiming completion. The completion channel is what tells the
// managed root the tree was contained, so a guardian that cannot write to it
// must not pretend it did.
func TestTurnSupervisorCovGuardianRefusesBeforeItCanReportCompletion(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		restoreTurnSupervisorSeams(t)
		turnSupervisorOpenFile = func(uintptr, string) *os.File { return nil }
		err := runTurnSupervisorGuardian(strings.NewReader("{}"), strings.NewReader(""), io.Discard)
		if err == nil || !strings.Contains(err.Error(), "amp guardian completion descriptor is unavailable") {
			t.Fatalf("absent guardian completion = %v", err)
		}
	})

	t.Run("unsealable", func(t *testing.T) {
		guardian := turnSupervisorCovNewGuardian(t)
		sealErr := errors.New("close-on-exec refused")
		turnSupervisorFcntl = func(uintptr, int, int) (int, error) { return 0, sealErr }
		err := runTurnSupervisorGuardian(
			turnSupervisorCovGuardianConfig(t), guardian.controlRead, io.Discard,
		)
		if !errors.Is(err, sealErr) {
			t.Fatalf("unsealable guardian completion = %v", err)
		}
		if got := guardian.completion(t); got != "" {
			t.Fatalf("guardian reported completion it could not protect: %q", got)
		}
	})
}

// TestTurnSupervisorCovGuardianReportsCompletionForEveryPreAuthorityRefusal
// proves that once the guardian owns its completion channel, every refusal it
// makes before it holds an authority or a liveness child releases the managed
// root by publishing completion. Those refusals contain nothing, because
// nothing was started, so failing to publish would strand the caller waiting
// on a boundary that will never close.
func TestTurnSupervisorCovGuardianReportsCompletionForEveryPreAuthorityRefusal(t *testing.T) {
	for name, test := range map[string]struct {
		config  func(t *testing.T) io.Reader
		control func(g *turnSupervisorCovGuardian) io.Reader
		arrange func()
		message string
	}{
		"control_is_not_a_file": {
			config:  turnSupervisorCovGuardianConfig,
			control: func(*turnSupervisorCovGuardian) io.Reader { return strings.NewReader("") },
			message: "amp guardian control input is not an inheritable file",
		},
		"malformed_config": {
			config:  func(*testing.T) io.Reader { return strings.NewReader("{") },
			message: "decode Amp guardian config",
		},
		"incomplete_config": {
			config:  func(*testing.T) io.Reader { return strings.NewReader("{}") },
			message: "amp native supervisor config is incomplete",
		},
		"authority_refused": {
			config: func(t *testing.T) io.Reader {
				t.Helper()

				return encodeSupervisorConfig(t, turnSupervisorConfig{
					Path: "/bin/true", Args: []string{"/bin/true"},
					Isolation: ProcessIsolation{
						UID: 64401, GID: 64402, BaseEnvironment: map[string]string{},
						TestOnlyNoCredential: true,
						StandaloneOwnerID:    "cov-guardian-authority",
						StandaloneStateRoot:  "/var/tmp/acp-go-amp-cov-guardian",
					},
				})
			},
			arrange: func() {
				turnSupervisorAcquireStandalone = func(
					uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
				) (*agentStandaloneIdentity, error) {
					return nil, errors.New("standalone claim refused")
				}
			},
			message: "acquire Amp standalone agent identity authority",
		},
		"privileges_refused": {
			config: turnSupervisorCovGuardianConfig,
			arrange: func() {
				turnSupervisorEnable = func() error { return errors.New("prctl refused") }
			},
			message: "enable Amp guardian privileges",
		},
		"liveness_launch_refused": {
			config: turnSupervisorCovGuardianConfig,
			arrange: func() {
				turnSupervisorMemfd = func(string, int) (int, error) {
					return 0, errors.New("liveness config memfd refused")
				}
			},
			message: "liveness config memfd refused",
		},
	} {
		t.Run(name, func(t *testing.T) {
			guardian := turnSupervisorCovNewGuardian(t)
			if test.arrange != nil {
				test.arrange()
			}
			control := io.Reader(guardian.controlRead)
			if test.control != nil {
				control = test.control(guardian)
			}

			err := runTurnSupervisorGuardian(test.config(t), control, io.Discard)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("guardian refusal = %v, want %q", err, test.message)
			}
			if got := guardian.completion(t); got != "complete\n" {
				t.Fatalf("guardian completion after refusal = %q", got)
			}
		})
	}
}

// TestTurnSupervisorCovGuardianRefusesAnAuthorityItCannotRevalidate proves
// that a guardian holding a standalone authority whose owner binding cannot be
// proven contains itself and reports completion instead of launching a
// liveness child. The authority is already held at that point, so the only
// safe exit is to contain and release rather than to supervise under an
// identity nothing backs.
func TestTurnSupervisorCovGuardianRefusesAnAuthorityItCannotRevalidate(t *testing.T) {
	const (
		uid = uint32(64411)
		gid = uint32(64412)
	)
	guardian := turnSupervisorCovNewGuardian(t)
	fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)
	turnSupervisorCovInherit(t, map[uintptr]*os.File{6: guardian.completionWrite})

	claimed := &agentStandaloneIdentity{
		identity: &agentIdentityLock{}, authority: &agentIdentityLock{},
		owner: agentStandaloneOwner{
			Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
			Provider: agentStandaloneOwnerID, OwnerID: "cov-guardian-unbacked",
			StateRoot: agentStandaloneStateRoot{Path: "/var/tmp/acp-go-amp-cov-guardian", Dev: 7, Ino: 8},
		},
	}
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return claimed, nil
	}
	var launched bool
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		launched = true

		return exec.Command(name, args...)
	}

	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"},
		Isolation: ProcessIsolation{
			UID: uid, GID: gid, BaseEnvironment: map[string]string{},
			TestOnlyNoCredential:     true,
			TestOnlyIdentityLockRoot: fixture.root,
			StandaloneOwnerID:        "cov-guardian-unbacked",
			StandaloneStateRoot:      "/var/tmp/acp-go-amp-cov-guardian",
		},
	})
	err := runTurnSupervisorGuardian(config, guardian.controlRead, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "validate Amp guardian identity disposition") ||
		!strings.Contains(err.Error(), "load standalone agent identity owner") {
		t.Fatalf("unbacked guardian authority = %v", err)
	}
	if launched {
		t.Fatal("guardian launched a liveness child under an unbacked authority")
	}
	if len(guardian.contained) != 1 || guardian.contained[0] != 0 {
		t.Fatalf("guardian containment for unbacked authority = %v", guardian.contained)
	}
	if got := guardian.completion(t); got != "complete\n" {
		t.Fatalf("guardian completion after unbacked authority = %q", got)
	}
}

// TestTurnSupervisorCovGuardianContainsWhenLivenessReadinessIsUnusable proves
// that every way the liveness readiness handshake can fail leaves the guardian
// containing the tree and publishing completion, and never publishing
// readiness upstream. A guardian that published readiness here would tell the
// managed root a native command is running when the guardian cannot even read
// its liveness child's report.
func TestTurnSupervisorCovGuardianContainsWhenLivenessReadinessIsUnusable(t *testing.T) {
	deadlineErr := errors.New("liveness data deadline refused")

	for name, test := range map[string]struct {
		script  string
		arrange func(t *testing.T)
		message string
	}{
		"deadline_unarmable": {
			script: "exit 0",
			arrange: func(*testing.T) {
				turnSupervisorReadDeadline = func(*os.File, time.Time) error { return deadlineErr }
			},
			message: deadlineErr.Error(),
		},
		"no_readiness": {
			script:  "exit 0",
			message: "await Amp liveness readiness",
		},
		"invalid_readiness": {
			script:  `printf 'garbage\n' >&5`,
			message: "invalid Amp liveness readiness",
		},
	} {
		t.Run(name, func(t *testing.T) {
			guardian := turnSupervisorCovNewGuardian(t)
			guardian.liveness(t, test.script)
			if test.arrange != nil {
				test.arrange(t)
			}
			var ready strings.Builder

			err := runTurnSupervisorGuardian(
				turnSupervisorCovGuardianConfig(t), guardian.controlRead, &ready,
			)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("unusable liveness readiness = %v, want %q", err, test.message)
			}
			if ready.String() != "" {
				t.Fatalf("guardian published readiness upstream: %q", ready.String())
			}
			if len(guardian.contained) != 1 || guardian.contained[0] != 0 {
				t.Fatalf("guardian containment for unusable readiness = %v", guardian.contained)
			}
			if got := guardian.completion(t); got != "complete\n" {
				t.Fatalf("guardian completion for unusable readiness = %q", got)
			}
		})
	}
}

// TestTurnSupervisorCovGuardianAbandonsTheTurnWhenItCannotClearItsReadDeadline
// proves that a guardian which can no longer control its liveness data channel
// after readiness closes the guardian peer and returns without claiming a
// containment it did not perform. Closing the peer is how the still-running
// liveness child learns its guardian is gone and contains the tree itself.
func TestTurnSupervisorCovGuardianAbandonsTheTurnWhenItCannotClearItsReadDeadline(t *testing.T) {
	guardian := turnSupervisorCovNewGuardian(t)
	guardian.liveness(t, `printf 'ready:%d\n' "$$" >&5`)
	deadlineErr := errors.New("liveness data deadline refused")
	calls := 0
	turnSupervisorReadDeadline = func(file *os.File, when time.Time) error {
		calls++
		if calls == 1 {
			return file.SetReadDeadline(when)
		}

		return deadlineErr
	}
	var ready strings.Builder

	err := runTurnSupervisorGuardian(
		turnSupervisorCovGuardianConfig(t), guardian.controlRead, &ready,
	)
	if !errors.Is(err, deadlineErr) {
		t.Fatalf("uncontrollable liveness data channel = %v", err)
	}
	if calls != 2 {
		t.Fatalf("guardian read-deadline calls = %d", calls)
	}
	if ready.String() != "" {
		t.Fatalf("guardian published readiness upstream: %q", ready.String())
	}
	if len(guardian.contained) != 0 {
		t.Fatalf("guardian claimed containment it did not perform: %v", guardian.contained)
	}
	if got := guardian.completion(t); got != "" {
		t.Fatalf("guardian claimed completion it did not perform: %q", got)
	}
}

// TestTurnSupervisorCovGuardianContainsTheExactNativePIDWhenReadinessCannotBePublished
// proves that when the guardian cannot publish readiness upstream it contains
// the tree naming the exact native pid its liveness child reported, not pid 0.
// Containing pid 0 would leave the already-running native command alive.
func TestTurnSupervisorCovGuardianContainsTheExactNativePIDWhenReadinessCannotBePublished(t *testing.T) {
	guardian := turnSupervisorCovNewGuardian(t)
	guardian.liveness(t, `printf 'ready:%d\n' "$$" >&5`)
	var launched *exec.Cmd
	command := turnSupervisorCommand
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		launched = command(name, args...)

		return launched
	}
	readyErr := errors.New("readiness channel refused")

	err := runTurnSupervisorGuardian(
		turnSupervisorCovGuardianConfig(t), guardian.controlRead,
		supervisorWriteSeeker{writeErr: readyErr},
	)
	if !errors.Is(err, readyErr) {
		t.Fatalf("unpublishable readiness = %v", err)
	}
	if launched == nil {
		t.Fatal("guardian never launched a liveness child")
	}
	if len(guardian.contained) != 1 || guardian.contained[0] != launched.Process.Pid {
		t.Fatalf(
			"guardian containment = %v, want the reported native pid %d",
			guardian.contained, launched.Process.Pid,
		)
	}
	if got := guardian.completion(t); got != "complete\n" {
		t.Fatalf("guardian completion after unpublishable readiness = %q", got)
	}
}

// TestTurnSupervisorCovGuardianContainsWhenLivenessExitsWithoutReportingDone
// proves that a supervised turn ended by the control channel closing is only
// treated as contained when the liveness child says so. When the child exits
// without its completion report the guardian contains the tree itself, naming
// the native pid, and publishes completion rather than trusting a silent exit.
func TestTurnSupervisorCovGuardianContainsWhenLivenessExitsWithoutReportingDone(t *testing.T) {
	guardian := turnSupervisorCovNewGuardian(t)
	pidPath := filepath.Join(t.TempDir(), "liveness.pid")
	guardian.liveness(t, `printf 'ready:%d\n' "$$" >&5; printf '%d\n' "$$" > `+pidPath+`; read discarded <&9`)
	var ready strings.Builder
	result := make(chan error, 1)
	go func() {
		result <- runTurnSupervisorGuardian(
			turnSupervisorCovGuardianConfig(t), guardian.controlRead, &ready,
		)
	}()

	pid := awaitSupervisorPIDFile(t, pidPath)
	if err := guardian.controlWrite.Close(); err != nil {
		t.Fatal(err)
	}

	var err error
	select {
	case err = <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("guardian did not end the turn when its control channel closed")
	}
	if err == nil || !strings.Contains(err.Error(), "amp liveness exited without completion report") {
		t.Fatalf("silent liveness exit = %v", err)
	}
	if ready.String() != turnSupervisorReady {
		t.Fatalf("guardian readiness = %q", ready.String())
	}
	if len(guardian.contained) != 1 || guardian.contained[0] != pid {
		t.Fatalf("guardian containment = %v, want the reported native pid %d", guardian.contained, pid)
	}
	if got := guardian.completion(t); got != "complete\n" {
		t.Fatalf("guardian completion after a silent liveness exit = %q", got)
	}
}

// TestTurnSupervisorCovGuardianForwardsOnlyRealSignalsToTheLivenessGroup
// proves that a signal delivered to the guardian is forwarded to the liveness
// process group, and that a notification carrying no operating-system signal
// is ignored rather than forwarded as something else. The liveness group is
// the whole supervised tree, so a wrong or invented signal would be delivered
// to the native command.
func TestTurnSupervisorCovGuardianForwardsOnlyRealSignalsToTheLivenessGroup(t *testing.T) {
	guardian := turnSupervisorCovNewGuardian(t)
	guardian.liveness(t, `printf 'ready:%d\n' "$$" >&5; read discarded <&9`)
	var launched *exec.Cmd
	command := turnSupervisorCommand
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		launched = command(name, args...)

		return launched
	}
	turnSupervisorSignalNotify = func(signals chan<- os.Signal, _ ...os.Signal) {
		signals <- supervisorTestSignal("not-an-os-signal")
		signals <- syscall.SIGTERM
	}
	var ready strings.Builder

	err := runTurnSupervisorGuardian(
		turnSupervisorCovGuardianConfig(t), guardian.controlRead, &ready,
	)
	if err == nil || !strings.Contains(err.Error(), "amp liveness exited without completion report") {
		t.Fatalf("signalled liveness turn = %v", err)
	}
	if ready.String() != turnSupervisorReady {
		t.Fatalf("guardian readiness = %q", ready.String())
	}
	if launched == nil || launched.ProcessState == nil {
		t.Fatalf("guardian liveness child = %#v", launched)
	}
	status, ok := launched.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGTERM {
		t.Fatalf("liveness child exit = %v, want SIGTERM", launched.ProcessState)
	}
	if len(guardian.contained) != 1 || guardian.contained[0] != launched.Process.Pid {
		t.Fatalf(
			"guardian containment = %v, want the reported native pid %d",
			guardian.contained, launched.Process.Pid,
		)
	}
	if got := guardian.completion(t); got != "complete\n" {
		t.Fatalf("guardian completion after signalling = %q", got)
	}
}
