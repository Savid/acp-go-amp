//go:build linux

package amp

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// turnSupervisorCovDuplicate hands the code under test its own descriptor for
// an authority half the fixture still owns, so adoption can take and release
// a descriptor without tearing down the fixture's own hold on the lock.
func turnSupervisorCovDuplicate(t *testing.T, file *os.File, name string) *os.File {
	t.Helper()
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}

	return os.NewFile(uintptr(fd), name)
}

// turnSupervisorCovClosedFile returns a descriptor whose kernel file is gone,
// which is how a supervisor sees an authority half that was released between
// validation and use.
func turnSupervisorCovClosedFile(t *testing.T) *os.File {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = write.Close(); err != nil {
		t.Fatal(err)
	}
	if err = read.Close(); err != nil {
		t.Fatal(err)
	}

	return read
}

// TestTurnSupervisorCovAuthorityAdoptionReleasesTheHalfItAlreadyHolds proves
// that the guardian adopts the inherited borrowed pair as a unit. When the
// identity half is missing the guardian holds nothing; when the identity half
// adopts but the domain half does not, the adopted identity descriptor is
// closed again rather than left held by a guardian that will never run.
func TestTurnSupervisorCovAuthorityAdoptionReleasesTheHalfItAlreadyHolds(t *testing.T) {
	const (
		uid = uint32(64321)
		gid = uint32(64322)
	)
	restoreTurnSupervisorSeams(t)
	fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)

	config := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"}, IdentityLock: true, AuthorityDomain: true,
		AuthorityOrigin: turnSupervisorOriginBorrowed,
		Isolation: ProcessIsolation{
			UID: uid, GID: gid, BaseEnvironment: map[string]string{},
			TestOnlyIdentityLockRoot: fixture.root,
		},
	}

	turnSupervisorOpenFile = func(uintptr, string) *os.File { return nil }
	authority, err := acquireTurnSupervisorAuthority(config, 7, 8, nil, nil)
	if authority != nil {
		t.Fatal("guardian kept an authority it could not adopt")
	}
	if err == nil || !strings.Contains(err.Error(), "adopt Amp agent identity lock") {
		t.Fatalf("absent identity half = %v", err)
	}

	var adopted *os.File
	turnSupervisorOpenFile = func(fd uintptr, name string) *os.File {
		if fd == 7 {
			adopted = turnSupervisorCovDuplicate(t, fixture.identity.file, name)

			return adopted
		}

		return nil
	}
	authority, err = acquireTurnSupervisorAuthority(config, 7, 8, nil, nil)
	if authority != nil {
		t.Fatal("guardian kept a half-adopted authority")
	}
	if err == nil || !strings.Contains(err.Error(), "adopt Amp agent authority domain") {
		t.Fatalf("absent domain half = %v", err)
	}
	if adopted == nil {
		t.Fatal("identity half was never adopted")
	}
	if !errors.Is(adopted.Close(), os.ErrClosed) {
		t.Fatal("half-adopted identity descriptor was retained")
	}
}

// TestTurnSupervisorCovAuthorityAdoptsTheCompleteBorrowedPair proves that a
// guardian handed the two trusted named locks adopts both and reports no
// standalone holder, so the later disposition check judges it as borrowed
// rather than as a standalone owner it never claimed.
func TestTurnSupervisorCovAuthorityAdoptsTheCompleteBorrowedPair(t *testing.T) {
	const (
		uid = uint32(64331)
		gid = uint32(64332)
	)
	restoreTurnSupervisorSeams(t)
	fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)

	config := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"}, IdentityLock: true, AuthorityDomain: true,
		AuthorityOrigin: turnSupervisorOriginBorrowed,
		Isolation: ProcessIsolation{
			UID: uid, GID: gid, BaseEnvironment: map[string]string{},
			TestOnlyIdentityLockRoot: fixture.root,
		},
	}
	turnSupervisorOpenFile = func(fd uintptr, name string) *os.File {
		switch fd {
		case 7:
			return turnSupervisorCovDuplicate(t, fixture.identity.file, name)
		case 8:
			return turnSupervisorCovDuplicate(t, fixture.domain.file, name)
		default:
			t.Errorf("guardian opened unexpected inherited descriptor %d", fd)

			return nil
		}
	}

	authority, err := acquireTurnSupervisorAuthority(config, 7, 8, nil, nil)
	if err != nil {
		t.Fatalf("adopt borrowed authority pair: %v", err)
	}
	if authority.standalone != nil {
		t.Fatal("borrowed adoption invented a standalone holder")
	}
	if authority.identity == nil || authority.identity.file == nil ||
		authority.domain == nil || authority.domain.file == nil {
		t.Fatalf("adopted borrowed authority = %#v", authority)
	}
	if err = validateTurnSupervisorAuthorityDisposition(config, authority); err != nil {
		t.Fatalf("adopted borrowed authority disposition = %v", err)
	}
	if err = authority.Close(); err != nil {
		t.Fatalf("close adopted borrowed authority: %v", err)
	}
	if authority.identity.file != nil || authority.domain.file != nil {
		t.Fatal("closing the borrowed authority left a half held")
	}
}

// TestTurnSupervisorCovAuthorityStandaloneClaimCarriesTheCancellationChannels
// proves that a guardian with no inherited authority claims a standalone one
// using the exact owner tuple from its config, and that the claim is handed
// the control-EOF and signal channels so a claim that has to wait can still
// be abandoned. It also proves a failed claim yields no authority at all.
func TestTurnSupervisorCovAuthorityStandaloneClaimCarriesTheCancellationChannels(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	config := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"},
		Isolation: ProcessIsolation{
			UID: 64341, GID: 64342, BaseEnvironment: map[string]string{},
			StandaloneOwnerID: "cov-standalone-claim", StandaloneStateRoot: "/var/tmp/acp-go-amp-cov-claim",
			TestOnlyIdentityLockRoot: t.TempDir(),
		},
	}
	canceled := make(chan struct{})
	signals := make(chan os.Signal, 1)

	claimErr := errors.New("standalone claim refused")
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		return nil, claimErr
	}
	authority, err := acquireTurnSupervisorAuthority(config, 7, 8, canceled, signals)
	if authority != nil {
		t.Fatal("failed standalone claim produced an authority")
	}
	if !errors.Is(err, claimErr) ||
		!strings.Contains(err.Error(), "acquire Amp standalone agent identity authority") {
		t.Fatalf("failed standalone claim = %v", err)
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
	claimed := &agentStandaloneIdentity{
		identity:  &agentIdentityLock{file: identityRead},
		authority: &agentIdentityLock{file: domainRead},
		owner: agentStandaloneOwner{
			Version: 1, UID: config.Isolation.UID, GID: config.Isolation.GID,
			Kind: agentStandaloneOwnerKind, Provider: agentStandaloneOwnerID,
			OwnerID:   config.Isolation.StandaloneOwnerID,
			StateRoot: agentStandaloneStateRoot{Path: config.Isolation.StandaloneStateRoot, Dev: 1, Ino: 2},
		},
	}
	var (
		gotUID, gotGID       uint32
		gotOwner, gotRoot    string
		gotTestOnly          bool
		gotTestRoot          string
		gotCanceledIsClaim   bool
		gotSignalsIsGuardian bool
	)
	turnSupervisorAcquireStandalone = func(
		uid uint32, gid uint32, ownerID string, stateRoot string, testOnly bool, testRoot string,
		claimCanceled <-chan struct{}, claimSignals <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		gotUID, gotGID, gotOwner, gotRoot = uid, gid, ownerID, stateRoot
		gotTestOnly, gotTestRoot = testOnly, testRoot
		gotCanceledIsClaim = claimCanceled == (<-chan struct{})(canceled)
		gotSignalsIsGuardian = claimSignals == (<-chan os.Signal)(signals)

		return claimed, nil
	}

	authority, err = acquireTurnSupervisorAuthority(config, 7, 8, canceled, signals)
	if err != nil {
		t.Fatalf("standalone claim: %v", err)
	}
	if authority.standalone != claimed || authority.identity != claimed.identity ||
		authority.domain != claimed.authority {
		t.Fatalf("standalone authority = %#v", authority)
	}
	if gotUID != config.Isolation.UID || gotGID != config.Isolation.GID ||
		gotOwner != config.Isolation.StandaloneOwnerID || gotRoot != config.Isolation.StandaloneStateRoot ||
		gotTestOnly != config.Isolation.TestOnlyNoCredential || gotTestRoot != config.Isolation.TestOnlyIdentityLockRoot {
		t.Fatalf(
			"standalone claim tuple = uid=%d gid=%d owner=%q root=%q testOnly=%t testRoot=%q",
			gotUID, gotGID, gotOwner, gotRoot, gotTestOnly, gotTestRoot,
		)
	}
	if !gotCanceledIsClaim || !gotSignalsIsGuardian {
		t.Fatal("standalone claim was not given the guardian cancellation channels")
	}
	if err = authority.Close(); err != nil {
		t.Fatalf("close standalone authority: %v", err)
	}
	if !errors.Is(identityRead.Close(), os.ErrClosed) || !errors.Is(domainRead.Close(), os.ErrClosed) {
		t.Fatal("closing the standalone authority left a half held")
	}
}

// TestTurnSupervisorCovLivenessConfigDeclaresTheHeldAuthorityOrigin proves
// that the config the guardian seals for its liveness child names the exact
// origin of the authority the guardian holds, and that it never carries the
// guardian's own lock capabilities or standalone claim fields. The liveness
// child revalidates against that origin, so a config that understated it
// would let the child run with an unchecked identity.
func TestTurnSupervisorCovLivenessConfigDeclaresTheHeldAuthorityOrigin(t *testing.T) {
	owner := agentStandaloneOwner{
		Version: 1, UID: 64351, GID: 64352, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "cov-liveness-origin",
		StateRoot: agentStandaloneStateRoot{Path: "/var/tmp/acp-go-amp-cov-origin", Dev: 3, Ino: 4},
	}

	for name, test := range map[string]struct {
		standalone bool
		wantOrigin string
	}{
		"borrowed":   {wantOrigin: turnSupervisorOriginBorrowed},
		"standalone": {standalone: true, wantOrigin: turnSupervisorOriginStandalone},
	} {
		t.Run(name, func(t *testing.T) {
			restoreTurnSupervisorSeams(t)
			identityRead, identityWrite, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer identityRead.Close()
			defer identityWrite.Close()
			domainRead, domainWrite, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer domainRead.Close()
			defer domainWrite.Close()

			authority := &turnSupervisorAuthority{
				identity: &agentIdentityLock{file: identityRead},
				domain:   &agentIdentityLock{file: domainRead},
			}
			if test.standalone {
				authority.standalone = &agentStandaloneIdentity{
					identity: authority.identity, authority: authority.domain, owner: owner,
				}
			}

			var sealed turnSupervisorConfig
			writeErr := errors.New("liveness config write refused")
			turnSupervisorWriteConfig = func(_ io.WriteSeeker, config turnSupervisorConfig) error {
				sealed = config

				return writeErr
			}

			guardianConfig := turnSupervisorConfig{
				Path: "/bin/true", Args: []string{"/bin/true"},
				AuthorityOrigin: "guardian-only",
				StandaloneOwner: &agentStandaloneOwner{OwnerID: "guardian-only"},
				Isolation: ProcessIsolation{
					UID: owner.UID, GID: owner.GID, BaseEnvironment: map[string]string{},
					IdentityLock: authority.identity, AuthorityDomain: authority.domain,
					StandaloneOwnerID: "guardian-claim", StandaloneStateRoot: "/var/tmp/guardian-claim",
				},
			}
			_, _, _, err = startTurnSupervisorLiveness(guardianConfig, nil, nil, authority)
			if !errors.Is(err, writeErr) {
				t.Fatalf("liveness config write refusal = %v", err)
			}
			if !sealed.IdentityLock || !sealed.AuthorityDomain || sealed.AuthorityOrigin != test.wantOrigin {
				t.Fatalf("sealed liveness authority = %#v", sealed)
			}
			if test.standalone {
				if sealed.StandaloneOwner == nil || *sealed.StandaloneOwner != owner {
					t.Fatalf("sealed liveness standalone owner = %#v", sealed.StandaloneOwner)
				}
			} else if sealed.StandaloneOwner != nil {
				t.Fatalf("borrowed liveness config carried a standalone owner: %#v", sealed.StandaloneOwner)
			}
			if sealed.Isolation.IdentityLock != nil || sealed.Isolation.AuthorityDomain != nil {
				t.Fatal("sealed liveness config carried the guardian's lock capabilities")
			}
			if sealed.Isolation.StandaloneOwnerID != "" || sealed.Isolation.StandaloneStateRoot != "" {
				t.Fatal("sealed liveness config carried the guardian's standalone claim fields")
			}
		})
	}
}

// TestTurnSupervisorCovLivenessLaunchFailsClosedOnEveryResource proves that
// every resource the guardian opens for its liveness child is released when
// any later step of the launch fails. The guardian survives these failures
// and keeps supervising, so a descriptor leaked here would accumulate across
// turns and could keep an authority duplicate alive past its own supervisor.
func TestTurnSupervisorCovLivenessLaunchFailsClosedOnEveryResource(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer controlRead.Close()
	defer controlWrite.Close()
	completionRead, completionWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer completionRead.Close()
	defer completionWrite.Close()

	memfdErr := errors.New("memfd refused")
	sealErr := errors.New("seal refused")
	dataPipeErr := errors.New("data pipe refused")
	peerPipeErr := errors.New("peer pipe refused")
	executableErr := errors.New("executable refused")

	borrowed := func(t *testing.T, identityUsable, domainUsable bool) *turnSupervisorAuthority {
		t.Helper()
		half := func(usable bool) *agentIdentityLock {
			if !usable {
				return &agentIdentityLock{file: turnSupervisorCovClosedFile(t)}
			}
			read, write, pipeErr := os.Pipe()
			if pipeErr != nil {
				t.Fatal(pipeErr)
			}
			t.Cleanup(func() {
				_ = read.Close()
				_ = write.Close()
			})

			return &agentIdentityLock{file: read}
		}

		return &turnSupervisorAuthority{identity: half(identityUsable), domain: half(domainUsable)}
	}

	for name, test := range map[string]struct {
		arrange func() *turnSupervisorAuthority
		want    error
	}{
		"memfd": {
			arrange: func() *turnSupervisorAuthority {
				turnSupervisorMemfd = func(string, int) (int, error) { return 0, memfdErr }

				return &turnSupervisorAuthority{}
			},
			want: memfdErr,
		},
		"seal": {
			arrange: func() *turnSupervisorAuthority {
				turnSupervisorSealConfig = func(uintptr, int, int) (int, error) { return 0, sealErr }

				return &turnSupervisorAuthority{}
			},
			want: sealErr,
		},
		"data_pipe": {
			arrange: func() *turnSupervisorAuthority {
				turnSupervisorPipe = func() (*os.File, *os.File, error) { return nil, nil, dataPipeErr }

				return &turnSupervisorAuthority{}
			},
			want: dataPipeErr,
		},
		"peer_pipe": {
			arrange: func() *turnSupervisorAuthority {
				calls := 0
				turnSupervisorPipe = func() (*os.File, *os.File, error) {
					calls++
					if calls == 2 {
						return nil, nil, peerPipeErr
					}

					return os.Pipe()
				}

				return &turnSupervisorAuthority{}
			},
			want: peerPipeErr,
		},
		"identity_duplicate": {
			arrange: func() *turnSupervisorAuthority { return borrowed(t, false, true) },
			want:    unix.EBADF,
		},
		"domain_duplicate": {
			arrange: func() *turnSupervisorAuthority { return borrowed(t, true, false) },
			want:    unix.EBADF,
		},
		"executable": {
			arrange: func() *turnSupervisorAuthority {
				turnSupervisorExecutable = func() (string, error) { return "", executableErr }

				return borrowed(t, true, true)
			},
			want: executableErr,
		},
		"start": {
			arrange: func() *turnSupervisorAuthority {
				turnSupervisorExecutable = func() (string, error) { return "/nonexistent/amp-supervisor", nil }

				return borrowed(t, true, true)
			},
			want: os.ErrNotExist,
		},
	} {
		t.Run(name, func(t *testing.T) {
			restoreTurnSupervisorSeams(t)
			authority := test.arrange()
			before := turnSupervisorCovDescriptors(t)
			liveness, data, peer, launchErr := startTurnSupervisorLiveness(
				turnSupervisorConfig{}, controlRead, completionWrite, authority,
			)
			if liveness != nil || data != nil || peer != nil {
				t.Fatalf("failed liveness launch returned %v, %v, %v", liveness, data, peer)
			}
			if !errors.Is(launchErr, test.want) {
				t.Fatalf("liveness launch failure = %v, want %v", launchErr, test.want)
			}
			turnSupervisorCovAssertNoLeak(t, before)
		})
	}
}
