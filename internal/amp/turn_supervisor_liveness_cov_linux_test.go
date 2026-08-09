//go:build linux

package amp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// turnSupervisorCovInherit installs the inherited-descriptor seam for a fixed
// descriptor map, so a supervisor core can be driven with exactly the
// descriptors a real launch would have handed it and with nothing else.
func turnSupervisorCovInherit(t *testing.T, files map[uintptr]*os.File) {
	t.Helper()
	turnSupervisorOpenFile = func(fd uintptr, name string) *os.File {
		file, known := files[fd]
		if !known {
			t.Errorf("supervisor opened unexpected inherited descriptor %d (%s)", fd, name)

			return nil
		}
		if file == nil {
			return nil
		}

		return turnSupervisorCovDuplicate(t, file, name)
	}
}

// turnSupervisorCovReadyWriter records the supervisor's readiness protocol and
// releases a waiter once the native pid has been published, so a test can act
// on the supervised tree only after it genuinely exists.
type turnSupervisorCovReadyWriter struct {
	mu       sync.Mutex
	written  bytes.Buffer
	ready    chan struct{}
	announce sync.Once
}

func (w *turnSupervisorCovReadyWriter) Write(value []byte) (int, error) {
	w.mu.Lock()
	w.written.Write(value)
	w.mu.Unlock()
	w.announce.Do(func() { close(w.ready) })

	return len(value), nil
}

func (w *turnSupervisorCovReadyWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.written.String()
}

// TestTurnSupervisorCovLivenessRefusesIncompleteInheritedDescriptors proves
// that the liveness core refuses to run unless it inherited both the
// completion channel and the guardian peer, and that it closes whichever one
// it did inherit. A liveness process that ran with only one of them could
// neither report containment nor notice its guardian dying, and a retained
// descriptor would keep the guardian's peer pipe open and make the guardian
// believe a liveness process is still supervising the tree.
func TestTurnSupervisorCovLivenessRefusesIncompleteInheritedDescriptors(t *testing.T) {
	for name, test := range map[string]struct{ completion, peer bool }{
		"neither":         {},
		"completion_only": {completion: true},
		"peer_only":       {peer: true},
	} {
		t.Run(name, func(t *testing.T) {
			restoreTurnSupervisorSeams(t)
			handed := map[uintptr]*os.File{8: nil, 9: nil}
			var inherited []*os.File
			for fd, present := range map[uintptr]bool{8: test.completion, 9: test.peer} {
				if !present {
					continue
				}
				read, write, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = write.Close() })
				handed[fd] = read
				inherited = append(inherited, read)
			}
			var opened []*os.File
			turnSupervisorOpenFile = func(fd uintptr, fdName string) *os.File {
				source, known := handed[fd]
				if !known {
					t.Errorf("liveness opened unexpected inherited descriptor %d", fd)

					return nil
				}
				if source == nil {
					return nil
				}
				duplicate := turnSupervisorCovDuplicate(t, source, fdName)
				opened = append(opened, duplicate)

				return duplicate
			}

			err := runTurnSupervisorLiveness(strings.NewReader("{}"), strings.NewReader(""), io.Discard)
			if err == nil || !strings.Contains(err.Error(), "amp liveness inherited descriptors are unavailable") {
				t.Fatalf("incomplete liveness descriptors = %v", err)
			}
			if len(opened) != len(inherited) {
				t.Fatalf("liveness opened %d descriptors, want %d", len(opened), len(inherited))
			}
			for index, file := range opened {
				if !errors.Is(file.Close(), os.ErrClosed) {
					t.Fatalf("liveness retained inherited descriptor %d after refusing", index)
				}
			}
		})
	}
}

// TestTurnSupervisorCovLivenessRefusesUnsealableInheritedDescriptors proves
// that the liveness core refuses to run when either the completion channel or
// the guardian peer cannot be marked close-on-exec. Both are private to the
// supervisor protocol, and the liveness core is about to exec an untrusted
// native command that must not inherit either of them.
func TestTurnSupervisorCovLivenessRefusesUnsealableInheritedDescriptors(t *testing.T) {
	for name, target := range map[string]int{"completion": 0, "peer": 1} {
		t.Run(name, func(t *testing.T) {
			restoreTurnSupervisorSeams(t)
			completionRead, completionWrite, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer completionRead.Close()
			defer completionWrite.Close()
			peerRead, peerWrite, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer peerRead.Close()
			defer peerWrite.Close()
			turnSupervisorCovInherit(t, map[uintptr]*os.File{8: completionWrite, 9: peerRead})

			sealErr := errors.New("close-on-exec refused")
			sealed := 0
			turnSupervisorFcntl = func(_ uintptr, command int, _ int) (int, error) {
				if command != unix.F_SETFD {
					return 0, nil
				}
				defer func() { sealed++ }()
				if sealed == target {
					return 0, sealErr
				}

				return 0, nil
			}

			err = runTurnSupervisorLiveness(strings.NewReader("{}"), strings.NewReader(""), io.Discard)
			if !errors.Is(err, sealErr) {
				t.Fatalf("unsealable liveness descriptor = %v", err)
			}
			if sealed != target+1 {
				t.Fatalf("liveness sealed %d descriptors before refusing, want %d", sealed, target+1)
			}
		})
	}
}

// TestTurnSupervisorCovLivenessSupervisesAndReportsThroughItsOwnDescriptors
// proves the liveness core's whole protocol on its real inherited layout: it
// launches the native command, publishes the native pid on the readiness
// channel so the guardian can contain that exact pid, contains the tree when
// the native exits, publishes completion on the private completion channel,
// and only then tells the guardian it is done.
func TestTurnSupervisorCovLivenessSupervisesAndReportsThroughItsOwnDescriptors(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	contained := 0
	containedNativePID := 0
	turnSupervisorContain = func(_ int, nativePID int) error {
		contained++
		containedNativePID = nativePID

		return nil
	}

	completionRead, completionWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer completionRead.Close()
	defer completionWrite.Close()
	peerRead, peerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer peerRead.Close()
	defer peerWrite.Close()
	turnSupervisorCovInherit(t, map[uintptr]*os.File{8: completionWrite, 9: peerRead})

	marker := filepath.Join(t.TempDir(), "native-ran")
	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", `printf 'ran\n' > "$1"`, "probe", marker},
		Env: []string{"PATH=/usr/bin:/bin"},
	})
	controlRead, controlWrite := io.Pipe()
	defer controlWrite.Close()
	var ready bytes.Buffer

	if err = runTurnSupervisorLiveness(config, controlRead, &ready); err != nil {
		t.Fatalf("liveness supervision: %v", err)
	}
	if payload, readErr := os.ReadFile(marker); readErr != nil || string(payload) != "ran\n" {
		t.Fatalf("supervised native command did not run: %q, %v", payload, readErr)
	}
	line, done, found := strings.Cut(ready.String(), "\n")
	if !found || done != "done\n" {
		t.Fatalf("liveness readiness protocol = %q", ready.String())
	}
	pid, err := parseTurnSupervisorLivenessReady(line + "\n")
	if err != nil {
		t.Fatalf("published liveness readiness %q: %v", line, err)
	}
	if contained != 1 || containedNativePID != pid {
		t.Fatalf("liveness contained %d times for pid %d, published pid %d", contained, containedNativePID, pid)
	}
	completion, err := bufio.NewReader(completionRead).ReadString('\n')
	if err != nil || completion != "complete\n" {
		t.Fatalf("liveness completion = %q, %v", completion, err)
	}
}

// TestTurnSupervisorCovNativeRefusesHalfAdoptedInheritedAuthority proves that
// the native core adopts the borrowed pair as a unit: an unusable identity
// half stops it before it touches the domain half, and an unusable domain
// half releases the already-adopted identity descriptor. Either way no native
// command is launched, because a supervisor holding half a pair cannot be
// judged by the disposition checks that follow.
func TestTurnSupervisorCovNativeRefusesHalfAdoptedInheritedAuthority(t *testing.T) {
	const (
		uid = uint32(64361)
		gid = uint32(64362)
	)
	restoreTurnSupervisorSeams(t)
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)
	marker := filepath.Join(t.TempDir(), "native-launched")

	newConfig := func() io.Reader {
		return encodeSupervisorConfig(t, turnSupervisorConfig{
			Path: "/bin/sh", Args: []string{"sh", "-c", `touch "$1"`, "probe", marker},
			Env:          []string{"PATH=/usr/bin:/bin"},
			IdentityLock: true, AuthorityDomain: true, AuthorityOrigin: turnSupervisorOriginBorrowed,
			Isolation: ProcessIsolation{
				UID: uid, GID: gid, BaseEnvironment: map[string]string{},
				TestOnlyIdentityLockRoot: fixture.root,
			},
		})
	}

	turnSupervisorCovInherit(t, map[uintptr]*os.File{6: nil, 7: nil})
	err := runTurnSupervisorNative(
		newConfig(), []io.Reader{strings.NewReader("")}, nil, io.Discard, io.Discard, 6, 7, false,
	)
	if err == nil || !strings.Contains(err.Error(), "adopt Amp agent identity lock") {
		t.Fatalf("unusable identity half = %v", err)
	}

	var adopted *os.File
	turnSupervisorOpenFile = func(fd uintptr, name string) *os.File {
		if fd != 6 {
			return nil
		}
		adopted = turnSupervisorCovDuplicate(t, fixture.identity.file, name)

		return adopted
	}
	err = runTurnSupervisorNative(
		newConfig(), []io.Reader{strings.NewReader("")}, nil, io.Discard, io.Discard, 6, 7, false,
	)
	if err == nil || !strings.Contains(err.Error(), "adopt Amp agent authority domain") {
		t.Fatalf("unusable domain half = %v", err)
	}
	if adopted == nil || !errors.Is(adopted.Close(), os.ErrClosed) {
		t.Fatal("half-adopted identity descriptor was retained by the native core")
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("native command ran without a complete authority: %v", statErr)
	}
}

// TestTurnSupervisorCovNativeRechecksTheDeclaredOriginInsideThePrivilegedWindow
// proves that the authority origin declared by the config is revalidated in
// the privileged pre-start window, after the inherited pair is adopted and
// isolation is applied but before the native command is executed. A config
// that claims a standalone origin must still be able to prove the exact owner
// binding at that moment; when the authority root holds no such binding the
// launch is refused, the tree is contained and completion is published,
// rather than running the native command under an origin nothing backs.
func TestTurnSupervisorCovNativeRechecksTheDeclaredOriginInsideThePrivilegedWindow(t *testing.T) {
	const (
		uid = uint32(64371)
		gid = uint32(64372)
	)
	restoreTurnSupervisorSeams(t)
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	contained := 0
	turnSupervisorContain = func(_ int, nativePID int) error {
		contained++
		if nativePID != 0 {
			t.Errorf("pre-launch containment named native pid %d", nativePID)
		}

		return nil
	}
	fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)
	turnSupervisorCovInherit(t, map[uintptr]*os.File{
		6: fixture.identity.file, 7: fixture.domain.file,
	})

	marker := filepath.Join(t.TempDir(), "native-launched")
	var native *exec.Cmd
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		native = exec.Command(name, args...)

		return native
	}
	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", `touch "$1"`, "probe", marker},
		Env:          []string{"PATH=/usr/bin:/bin"},
		IdentityLock: true, AuthorityDomain: true, AuthorityOrigin: turnSupervisorOriginStandalone,
		StandaloneOwner: &agentStandaloneOwner{
			Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
			Provider: agentStandaloneOwnerID, OwnerID: "cov-late-origin",
			StateRoot: agentStandaloneStateRoot{Path: "/var/tmp/acp-go-amp-cov-late", Dev: 5, Ino: 6},
		},
		Isolation: ProcessIsolation{
			UID: uid, GID: gid, BaseEnvironment: map[string]string{},
			TestOnlyIdentityLockRoot: fixture.root,
		},
	})
	var completion bytes.Buffer
	err := runTurnSupervisorNative(
		config, []io.Reader{strings.NewReader("")}, nil, io.Discard, &completion, 6, 7, true,
	)
	if err == nil || !strings.Contains(err.Error(), "load standalone agent identity owner") {
		t.Fatalf("late origin recheck = %v", err)
	}
	if native == nil || native.Process != nil {
		t.Fatalf("native command was started despite the unbacked origin: %#v", native)
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("native command ran despite the unbacked origin: %v", statErr)
	}
	if contained != 1 || completion.String() != "complete\n" {
		t.Fatalf("late origin containment contained=%d completion=%q", contained, completion.String())
	}
}

// TestTurnSupervisorCovNativeRefusesAnIncompleteStandaloneAuthority proves
// that the native core will not run a native command when the standalone
// claim returns without both authority halves, and that a failed claim is
// reported rather than silently downgraded to an unauthenticated launch.
func TestTurnSupervisorCovNativeRefusesAnIncompleteStandaloneAuthority(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	marker := filepath.Join(t.TempDir(), "native-launched")
	newConfig := func() io.Reader {
		return encodeSupervisorConfig(t, turnSupervisorConfig{
			Path: "/bin/sh", Args: []string{"sh", "-c", `touch "$1"`, "probe", marker},
			Env: []string{"PATH=/usr/bin:/bin"},
			Isolation: ProcessIsolation{
				UID: 64381, GID: 64382, BaseEnvironment: map[string]string{},
				StandaloneOwnerID:   "cov-native-standalone",
				StandaloneStateRoot: "/var/tmp/acp-go-amp-cov-native",
			},
		})
	}

	claimErr := errors.New("standalone claim refused")
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return nil, claimErr
	}
	err := runTurnSupervisorNative(
		newConfig(), []io.Reader{strings.NewReader("")}, nil, io.Discard, io.Discard, 6, 7, false,
	)
	if !errors.Is(err, claimErr) ||
		!strings.Contains(err.Error(), "acquire Amp standalone agent identity authority") {
		t.Fatalf("failed standalone claim = %v", err)
	}

	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return &agentStandaloneIdentity{}, nil
	}
	err = runTurnSupervisorNative(
		newConfig(), []io.Reader{strings.NewReader("")}, nil, io.Discard, io.Discard, 6, 7, false,
	)
	if err == nil || !strings.Contains(err.Error(), "amp agent identity authority is incomplete") {
		t.Fatalf("incomplete standalone authority = %v", err)
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("native command ran without a standalone authority: %v", statErr)
	}
}

// TestTurnSupervisorCovNativeRefusesToStartWithoutProvableIdentity proves
// that when the supervisor cannot establish the native identity in the
// privileged window, the native command is never started: the isolation
// application is the last point at which the launch can still be abandoned.
// It also proves the shared arm reaches that refusal without consulting
// supplementary groups, which belong to the account the supervisor was started
// under and which it can neither shed nor re-enter.
func TestTurnSupervisorCovNativeRefusesToStartWithoutProvableIdentity(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return &agentStandaloneIdentity{identity: &agentIdentityLock{}, authority: &agentIdentityLock{}}, nil
	}

	uidOriginal, gidOriginal, groupsOriginal := processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups
	t.Cleanup(func() {
		processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups =
			uidOriginal, gidOriginal, groupsOriginal
	})
	processIsolationGeteuid = func() int { return 64391 }
	processIsolationGetegid = func() int { return 64393 }
	groupsErr := errors.New("supplementary groups refused")
	processIsolationGetgroups = func() ([]int, error) { return nil, groupsErr }

	marker := filepath.Join(t.TempDir(), "native-launched")
	var native *exec.Cmd
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		native = exec.Command(name, args...)

		return native
	}
	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", `touch "$1"`, "probe", marker},
		Env: []string{"PATH=/usr/bin:/bin"},
		Isolation: ProcessIsolation{
			UID: 64391, GID: 64392, BaseEnvironment: map[string]string{},
		},
		AuthorityOrigin: turnSupervisorOriginShared,
	})
	err := runTurnSupervisorNative(
		config, []io.Reader{strings.NewReader("")}, nil, io.Discard, io.Discard, 6, 7, false,
	)
	if errors.Is(err, groupsErr) {
		t.Fatalf("shared identity consulted supplementary groups: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "native group 64392 cannot be entered from group 64393") ||
		!strings.Contains(err.Error(), "apply Amp native process isolation") {
		t.Fatalf("unprovable native identity = %v", err)
	}
	if native == nil || native.Process != nil {
		t.Fatalf("native command started without a provable identity: %#v", native)
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("native command ran without a provable identity: %v", statErr)
	}
}

// TestTurnSupervisorCovNativeContainsTheTreeWhenTheGuardianDiesAfterLaunch
// proves that the guardian peer channel, not just the control channel, ends
// the supervised turn. When the guardian dies after the native command is
// already running, the liveness core kills the native process group, contains
// the tree and publishes completion, so the managed root is never left
// retaining a tree whose guardian is gone.
func TestTurnSupervisorCovNativeContainsTheTreeWhenTheGuardianDiesAfterLaunch(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return &agentStandaloneIdentity{identity: &agentIdentityLock{}, authority: &agentIdentityLock{}}, nil
	}
	killed := make(chan int, 4)
	turnSupervisorSignalGroup = func(pid int, signal syscall.Signal) error {
		if signal == syscall.SIGKILL {
			killed <- pid
		}

		return signalProcessGroupID(pid, signal)
	}
	contained := 0
	turnSupervisorContain = func(int, int) error {
		contained++

		return nil
	}

	peerRead, peerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer peerRead.Close()
	controlRead, controlWrite := io.Pipe()
	defer controlWrite.Close()

	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", "while :; do sleep 1; done"},
		Env: []string{"PATH=/usr/bin:/bin"},
	})
	ready := &turnSupervisorCovReadyWriter{ready: make(chan struct{})}
	var completion bytes.Buffer
	result := make(chan error, 1)
	go func() {
		result <- runTurnSupervisorNative(
			config, []io.Reader{controlRead}, peerRead, ready, &completion, 6, 7, true,
		)
	}()

	select {
	case <-ready.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("supervised native command never published readiness")
	}
	pid, err := parseTurnSupervisorLivenessReady(ready.String())
	if err != nil {
		t.Fatalf("published readiness %q: %v", ready.String(), err)
	}
	if err = peerWrite.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err = <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("liveness core did not end the turn when its guardian died")
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("guardian death turn end = %v, want the native exit status", err)
	}
	status, statusOK := exitErr.Sys().(syscall.WaitStatus)
	if !statusOK || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("native exit after guardian death = %v", exitErr)
	}
	select {
	case victim := <-killed:
		if victim != pid {
			t.Fatalf("liveness killed process group %d, published native pid %d", victim, pid)
		}
	default:
		t.Fatal("liveness did not kill the native process group")
	}
	if contained != 1 || completion.String() != "complete\n" {
		t.Fatalf("guardian death containment contained=%d completion=%q", contained, completion.String())
	}
	if !strings.HasSuffix(ready.String(), "done\n") {
		t.Fatalf("liveness did not report done to its guardian: %q", ready.String())
	}
	if processPIDAlive(pid) {
		t.Fatalf("native process %d survived its guardian's death", pid)
	}
}
