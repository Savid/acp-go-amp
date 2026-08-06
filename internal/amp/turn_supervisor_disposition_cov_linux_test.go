//go:build linux

package amp

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// turnSupervisorCovCapability is an inherited-authority stand-in whose
// duplication outcome the test chooses, so the parent-side handoff can be
// driven through the exact failure the kernel would report for a descriptor
// that stopped being duplicable between validation and launch.
type turnSupervisorCovCapability struct {
	err  error
	path string
}

func (c turnSupervisorCovCapability) Duplicate() (*os.File, error) {
	if c.err != nil {
		return nil, c.err
	}

	return os.Open(c.path)
}

// turnSupervisorCovDescriptors snapshots the descriptors this process holds,
// so a refusal path can be proven to leak none of the memfd, pipe or
// duplicated authority descriptors it opened on the way to the refusal.
func turnSupervisorCovDescriptors(t *testing.T) map[string]struct{} {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	open := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		open[entry.Name()] = struct{}{}
	}

	return open
}

func turnSupervisorCovAssertNoLeak(t *testing.T, before map[string]struct{}) {
	t.Helper()
	for name := range turnSupervisorCovDescriptors(t) {
		if _, held := before[name]; !held {
			link, _ := os.Readlink("/proc/self/fd/" + name)
			t.Fatalf("refusal path leaked descriptor %s -> %s", name, link)
		}
	}
}

// TestTurnSupervisorCovPrepareRefusesIncompleteIsolationAndAuthorityPairs
// proves that the parent side refuses to build a supervised launch whenever
// the isolation policy is absent or incomplete, or when only one half of the
// borrowed authority pair is supplied. A supervisor started with half an
// authority pair would run the native command with an identity nobody can
// revalidate downstream, so the refusal must happen before any descriptor is
// created.
func TestTurnSupervisorCovPrepareRefusesIncompleteIsolationAndAuthorityPairs(t *testing.T) {
	restoreTurnSupervisorSeams(t)

	if err := validateTurnSupervisorIdentity(nil); err == nil ||
		!strings.Contains(err.Error(), "process isolation is required") {
		t.Fatalf("absent isolation identity = %v", err)
	}

	before := turnSupervisorCovDescriptors(t)
	for name, isolation := range map[string]*ProcessIsolation{
		"absent":     nil,
		"zero_ident": {BaseEnvironment: map[string]string{}},
		"no_environ": {UID: 11, GID: 22},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := prepareProcessTreeCommand(
				exec.Command("/bin/true"), processLaunchOptions{Isolation: isolation},
			)
			if err == nil || !strings.Contains(err.Error(), "prepare Amp native supervisor isolation") {
				t.Fatalf("invalid isolation preparation = %v", err)
			}
		})
	}

	for name, mutate := range map[string]func(*ProcessIsolation){
		"identity_without_domain": func(isolation *ProcessIsolation) {
			isolation.IdentityLock = turnSupervisorCovCapability{path: os.DevNull}
		},
		"domain_without_identity": func(isolation *ProcessIsolation) {
			isolation.AuthorityDomain = turnSupervisorCovCapability{path: os.DevNull}
		},
	} {
		t.Run(name, func(t *testing.T) {
			isolation := supervisorTestIsolation()
			mutate(isolation)
			_, err := prepareProcessTreeCommand(
				exec.Command("/bin/true"), processLaunchOptions{Isolation: isolation},
			)
			if err == nil || !strings.Contains(err.Error(), "must be supplied together") {
				t.Fatalf("half authority pair preparation = %v", err)
			}
		})
	}
	turnSupervisorCovAssertNoLeak(t, before)
}

// TestTurnSupervisorCovPrepareFailsClosedWhenAuthorityCannotBeDuplicated
// proves that when either half of the borrowed authority pair cannot be
// duplicated for the supervisor, the launch is refused, the failing half is
// named, and every descriptor already opened for the launch, including the
// authority half that duplicated successfully, is released. A leaked
// authority duplicate would keep an agent identity lock held with no
// supervisor that can ever release it.
func TestTurnSupervisorCovPrepareFailsClosedWhenAuthorityCannotBeDuplicated(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	identityErr := errors.New("identity duplicate refused")
	domainErr := errors.New("domain duplicate refused")

	for name, test := range map[string]struct {
		identity ProcessIdentityLockCapability
		domain   ProcessIdentityLockCapability
		want     error
		message  string
	}{
		"identity_half": {
			identity: turnSupervisorCovCapability{err: identityErr},
			domain:   turnSupervisorCovCapability{path: os.DevNull},
			want:     identityErr,
			message:  "duplicate Amp agent identity lock",
		},
		"domain_half": {
			identity: turnSupervisorCovCapability{path: os.DevNull},
			domain:   turnSupervisorCovCapability{err: domainErr},
			want:     domainErr,
			message:  "duplicate Amp agent authority domain",
		},
	} {
		t.Run(name, func(t *testing.T) {
			isolation := supervisorTestIsolation()
			isolation.IdentityLock = test.identity
			isolation.AuthorityDomain = test.domain
			before := turnSupervisorCovDescriptors(t)
			launch, err := prepareProcessTreeCommand(
				exec.Command("/bin/true"), processLaunchOptions{Isolation: isolation},
			)
			if launch != nil {
				t.Fatal("unduplicable authority produced a launch")
			}
			if !errors.Is(err, test.want) || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("authority duplication refusal = %v", err)
			}
			turnSupervisorCovAssertNoLeak(t, before)
		})
	}
}

// TestTurnSupervisorCovConfigValidationRefusesUnusableIsolation proves that a
// decoded supervisor config carrying an isolation policy the launch validator
// rejects is refused by the supervisor itself, not merely by its parent. The
// config arrives over an inherited descriptor, so the supervisor cannot
// assume the parent validated it.
func TestTurnSupervisorCovConfigValidationRefusesUnusableIsolation(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	config := turnSupervisorConfig{
		Path:      "/bin/true",
		Args:      []string{"/bin/true"},
		Isolation: ProcessIsolation{UID: 11, GID: 22},
	}
	if err := validateTurnSupervisorConfig(config); err == nil ||
		!strings.Contains(err.Error(), "validate Amp native supervisor isolation") {
		t.Fatalf("unusable supervisor isolation = %v", err)
	}
}

// TestTurnSupervisorCovAuthorityCloseIsNilSafeAndPrefersTheStandaloneHolder
// proves that closing the supervisor's authority releases the standalone
// holder when one was acquired, rather than closing the two borrowed halves
// separately. The standalone holder owns both halves plus its own marker
// state, so closing the halves alone would leave that state published.
func TestTurnSupervisorCovAuthorityCloseIsNilSafeAndPrefersTheStandaloneHolder(t *testing.T) {
	if err := (*turnSupervisorAuthority)(nil).Close(); err != nil {
		t.Fatalf("absent authority close = %v", err)
	}

	identityRead, identityWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer identityWrite.Close()
	domainRead, domainWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer domainWrite.Close()
	identity := &agentIdentityLock{file: identityRead}
	domain := &agentIdentityLock{file: domainRead}
	standalone := &agentStandaloneIdentity{identity: identity, authority: domain}
	authority := &turnSupervisorAuthority{identity: identity, domain: domain, standalone: standalone}
	if err = authority.Close(); err != nil {
		t.Fatalf("standalone authority close = %v", err)
	}
	if standalone.identity != nil || standalone.authority != nil {
		t.Fatal("standalone holder retained its authority halves after close")
	}
	if !errors.Is(identityRead.Close(), os.ErrClosed) || !errors.Is(domainRead.Close(), os.ErrClosed) {
		t.Fatal("standalone authority close left a half open")
	}
}

// TestTurnSupervisorCovDispositionDispatchesOnTheDeclaredAuthorityOrigin
// proves that the late disposition check routes to the validator matching the
// authority the supervisor actually holds. A borrowed origin must be judged
// against the ownerless active disposition, a standalone origin against the
// exact owner tuple, an unknown origin must be refused outright, and a
// standalone origin without its owner tuple must never fall back to the
// weaker borrowed check.
func TestTurnSupervisorCovDispositionDispatchesOnTheDeclaredAuthorityOrigin(t *testing.T) {
	const (
		uid = uint32(64301)
		gid = uint32(64302)
	)
	restoreTurnSupervisorSeams(t)
	fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)

	base := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"},
		Isolation: ProcessIsolation{
			UID: uid, GID: gid, BaseEnvironment: map[string]string{},
			TestOnlyIdentityLockRoot: fixture.root,
		},
	}

	inherited := base
	if err := validateTurnSupervisorConfigDisposition(inherited, true); err != nil {
		t.Fatalf("inherited authority disposition = %v", err)
	}

	borrowed := base
	borrowed.AuthorityOrigin = turnSupervisorOriginBorrowed
	if err := validateTurnSupervisorConfigDisposition(borrowed, true); err != nil {
		t.Fatalf("borrowed authority disposition = %v", err)
	}

	standaloneNoOwner := base
	standaloneNoOwner.AuthorityOrigin = turnSupervisorOriginStandalone
	err := validateTurnSupervisorConfigDisposition(standaloneNoOwner, true)
	if err == nil || !strings.Contains(err.Error(), "standalone authority owner tuple is unavailable") {
		t.Fatalf("ownerless standalone disposition = %v", err)
	}

	standalone := standaloneNoOwner
	owner := agentStandaloneOwner{
		Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "cov-standalone",
		StateRoot: agentStandaloneStateRoot{Path: "/var/tmp/acp-go-amp-cov", Dev: 1, Ino: 2},
	}
	standalone.StandaloneOwner = &owner
	err = validateTurnSupervisorConfigDisposition(standalone, true)
	if err == nil || !strings.Contains(err.Error(), "load standalone agent identity owner") {
		t.Fatalf("standalone disposition did not consult the owner binding: %v", err)
	}

	unknown := base
	unknown.AuthorityOrigin = "hijack"
	err = validateTurnSupervisorConfigDisposition(unknown, true)
	if err == nil || !strings.Contains(err.Error(), `amp authority origin "hijack" is invalid`) {
		t.Fatalf("unknown authority origin = %v", err)
	}
}

// TestTurnSupervisorCovHeldStandaloneAuthorityOutranksTheDeclaredOrigin
// proves that when the supervisor actually holds a standalone authority, its
// disposition is judged against that live owner tuple even though the config
// declares no authority origin at all. Trusting the config here would let a
// forged config downgrade a standalone launch to an unchecked one.
func TestTurnSupervisorCovHeldStandaloneAuthorityOutranksTheDeclaredOrigin(t *testing.T) {
	const (
		uid = uint32(64311)
		gid = uint32(64312)
	)
	restoreTurnSupervisorSeams(t)
	fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)

	config := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"},
		Isolation: ProcessIsolation{
			UID: uid, GID: gid, BaseEnvironment: map[string]string{},
			TestOnlyIdentityLockRoot: fixture.root,
		},
	}
	if err := validateTurnSupervisorAuthorityDisposition(config, nil); err != nil {
		t.Fatalf("inherited authority disposition = %v", err)
	}

	authority := &turnSupervisorAuthority{standalone: &agentStandaloneIdentity{
		owner: agentStandaloneOwner{
			Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
			Provider: agentStandaloneOwnerID, OwnerID: "cov-held",
			StateRoot: agentStandaloneStateRoot{Path: "/var/tmp/acp-go-amp-cov", Dev: 1, Ino: 2},
		},
	}}
	err := validateTurnSupervisorAuthorityDisposition(config, authority)
	if err == nil || !strings.Contains(err.Error(), "load standalone agent identity owner") {
		t.Fatalf("held standalone authority disposition = %v", err)
	}
}

// TestTurnSupervisorCovLivenessReadinessMustCarryAnExactNativePID proves that
// the guardian only accepts a liveness readiness line that names a plausible
// native pid. The guardian signals and contains that pid, so a truncated,
// mislabelled or non-positive value must be refused rather than silently
// containing pid 0, which on Linux means the caller's own process group.
func TestTurnSupervisorCovLivenessReadinessMustCarryAnExactNativePID(t *testing.T) {
	for name, test := range map[string]struct{ line, message string }{
		"unterminated": {line: "ready:42", message: "not newline terminated"},
		"wrong_prefix": {line: "steady:42\n", message: "invalid Amp liveness readiness"},
		"empty_pid":    {line: "ready:\n", message: "invalid Amp liveness native pid"},
		"not_a_number": {line: "ready:abc\n", message: "invalid Amp liveness native pid"},
		"zero":         {line: "ready:0\n", message: "invalid Amp liveness native pid"},
		"negative":     {line: "ready:-3\n", message: "invalid Amp liveness native pid"},
	} {
		t.Run(name, func(t *testing.T) {
			pid, err := parseTurnSupervisorLivenessReady(test.line)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("readiness %q = %v", test.line, err)
			}
			if pid != 0 {
				t.Fatalf("refused readiness returned pid %d", pid)
			}
		})
	}

	pid, err := parseTurnSupervisorLivenessReady("ready:4242\n")
	if err != nil || pid != 4242 {
		t.Fatalf("valid readiness = %d, %v", pid, err)
	}
}

// TestTurnSupervisorCovGuardianPeerPollFailureRefusesTheNativeLaunch proves
// that when the kernel will not report the guardian peer descriptor's state,
// the liveness side refuses to launch instead of assuming the guardian is
// still alive. The peer poll is the only pre-launch evidence that a guardian
// still exists to contain the tree.
func TestTurnSupervisorCovGuardianPeerPollFailureRefusesTheNativeLaunch(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	peerRead, peerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer peerRead.Close()
	defer peerWrite.Close()

	if err = validateTurnSupervisorGuardianPeer(nil, nil); err != nil {
		t.Fatalf("absent guardian peer = %v", err)
	}

	pollErr := errors.New("poll refused")
	polled := 0
	turnSupervisorPoll = func(fds []unix.PollFd, _ int) (int, error) {
		polled++
		if len(fds) != 1 || fds[0].Fd != int32(peerRead.Fd()) {
			t.Errorf("guardian peer poll set = %#v", fds)
		}

		return 0, pollErr
	}
	err = validateTurnSupervisorGuardianPeer(peerRead, make(chan struct{}))
	if !errors.Is(err, pollErr) || !strings.Contains(err.Error(), "poll Amp guardian before native launch") {
		t.Fatalf("guardian peer poll failure = %v", err)
	}
	if polled != 1 {
		t.Fatalf("guardian peer polls = %d", polled)
	}
}

// TestTurnSupervisorCovInheritedInputFailsClosedWhenDescriptorsCannotBeSealed
// proves that when the inherited config, control or readiness descriptor
// cannot be marked close-on-exec, every one of the three is closed and the
// supervisor refuses to start. Leaving them open would hand the supervised
// native command a live channel to the guardian's private protocol.
func TestTurnSupervisorCovInheritedInputFailsClosedWhenDescriptorsCannotBeSealed(t *testing.T) {
	for name, test := range map[string]struct {
		fail    int
		message string
	}{
		"read_flags":    {fail: unix.F_GETFD, message: "read inherited Amp supervisor descriptor flags"},
		"protect_flags": {fail: unix.F_SETFD, message: "protect inherited Amp supervisor descriptor from exec"},
	} {
		t.Run(name, func(t *testing.T) {
			restoreTurnSupervisorSeams(t)
			inherited := make([]*os.File, 0, 3)
			for range 3 {
				read, write, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = write.Close() })
				inherited = append(inherited, read)
			}
			next := 0
			turnSupervisorOpenFile = func(uintptr, string) *os.File {
				file := inherited[next]
				next++

				return file
			}
			fcntlErr := errors.New("descriptor flags refused")
			turnSupervisorFcntl = func(_ uintptr, command int, _ int) (int, error) {
				if command == test.fail {
					return 0, fcntlErr
				}

				return 0, nil
			}

			config, control, ready, err := inheritedTurnSupervisorInput()
			if config != nil || control != nil || ready != nil {
				t.Fatal("unsealed inherited descriptors were returned to the supervisor")
			}
			if !errors.Is(err, fcntlErr) || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("unsealed inherited input = %v", err)
			}
			for index, file := range inherited {
				if !errors.Is(file.Close(), os.ErrClosed) {
					t.Fatalf("inherited descriptor %d survived the refusal", index)
				}
			}
		})
	}
}

// TestTurnSupervisorCovEnableRefusesWhenDumpabilityCannotBeDropped proves
// that the supervisor refuses to continue when PR_SET_DUMPABLE fails. A
// dumpable supervisor can be attached to, and it holds the agent identity
// lock and the whole private guardian protocol.
func TestTurnSupervisorCovEnableRefusesWhenDumpabilityCannotBeDropped(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorSetrlimit = func(int, *unix.Rlimit) error { return nil }
	dumpableErr := errors.New("dumpable refused")
	var attempted []int
	turnSupervisorPrctl = func(option int, _, _, _, _ uintptr) error {
		attempted = append(attempted, option)
		if option == unix.PR_SET_DUMPABLE {
			return dumpableErr
		}

		return nil
	}
	if err := enableTurnSupervisor(); !errors.Is(err, dumpableErr) {
		t.Fatalf("dumpable failure = %v", err)
	}
	if len(attempted) != 2 || attempted[0] != unix.PR_SET_CHILD_SUBREAPER ||
		attempted[1] != unix.PR_SET_DUMPABLE {
		t.Fatalf("privilege attempts before refusal = %v", attempted)
	}
}

// TestTurnSupervisorCovBootstrapDispatchesLivenessMode proves that the
// private bootstrap hands a liveness-mode process to the liveness core and
// never to the guardian core. The two modes inherit different descriptor
// layouts, so dispatching to the wrong one would make the process adopt the
// wrong descriptors as its authority.
func TestTurnSupervisorCovBootstrapDispatchesLivenessMode(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	t.Setenv(turnSupervisorModeEnv, turnSupervisorLivenessMode)

	exitCode := -1
	turnSupervisorExit = func(code int) { exitCode = code }
	closed := make([]bool, 3)
	turnSupervisorInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return &recordingReadCloser{Reader: strings.NewReader("config"), closed: &closed[0]},
			&recordingReadCloser{Reader: strings.NewReader("control"), closed: &closed[1]},
			&recordingWriteCloser{Writer: io.Discard, closed: &closed[2]}, nil
	}
	turnSupervisorRun = func(io.Reader, io.Reader, io.Writer) error {
		t.Error("liveness mode dispatched to the guardian core")

		return nil
	}
	livenessRuns := 0
	turnSupervisorRunLiveness = func(io.Reader, io.Reader, io.Writer) error {
		livenessRuns++

		return nil
	}

	turnSupervisorBootstrap()
	if livenessRuns != 1 || exitCode != 0 {
		t.Fatalf("liveness bootstrap runs=%d exit=%d", livenessRuns, exitCode)
	}
	if !closed[0] || !closed[1] || !closed[2] {
		t.Fatalf("liveness bootstrap left inherited descriptors open: %v", closed)
	}
}
