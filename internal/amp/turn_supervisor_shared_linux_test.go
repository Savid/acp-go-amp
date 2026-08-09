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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func sharedSupervisorIsolation() *ProcessIsolation {
	return &ProcessIsolation{UID: 1000, GID: 1000, BaseEnvironment: map[string]string{}}
}

// TestSupervisorIdentityRuleAcceptsTheIdentityItAlreadyRuns proves the root
// assertion is skipped only when the native identity is the running one, and
// that every other shape still meets the refusal it always met, byte for byte.
func TestSupervisorIdentityRuleAcceptsTheIdentityItAlreadyRuns(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }

	require.NoError(t, validateTurnSupervisorIdentity(sharedSupervisorIsolation()))

	differentGroup := sharedSupervisorIsolation()
	differentGroup.GID = 1001
	require.NoError(t, validateTurnSupervisorIdentity(differentGroup))

	distinctTarget := sharedSupervisorIsolation()
	distinctTarget.UID, distinctTarget.GID = 65534, 65534
	require.ErrorContains(t, validateTurnSupervisorIdentity(distinctTarget),
		"trusted root identity is required, effective uid is 1000")

	processIsolationGeteuid = func() int { return 0 }
	turnSupervisorEffectiveUID = func() int { return 0 }

	rootTarget := sharedSupervisorIsolation()
	rootTarget.UID, rootTarget.GID = 0, 0
	require.ErrorContains(t, validateTurnSupervisorIdentity(rootTarget),
		"native target identity must differ from the trusted supervisor")
	require.NoError(t, validateTurnSupervisorIdentity(distinctTarget))
	require.ErrorContains(t, validateTurnSupervisorIdentity(nil), "process isolation is required")
}

// TestPrepareTurnSupervisorStampsTheAuthorityItsIdentityAllows proves the
// parent records the one decision it made in the sealed config, so the guardian
// and the liveness child act on the same origin the parent derived, and that
// the isolated stamping is exactly what it was.
func TestPrepareTurnSupervisorStampsTheAuthorityItsIdentityAllows(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)
	processIsolationGeteuid = func() int { return 1000 }

	var stamped []turnSupervisorConfig

	turnSupervisorWriteConfig = func(file io.WriteSeeker, config turnSupervisorConfig) error {
		stamped = append(stamped, config)

		return writeTurnSupervisorConfig(file, config)
	}

	launch, err := prepareProcessTreeCommand(
		exec.Command("/bin/true"), processLaunchOptions{Isolation: sharedSupervisorIsolation()},
	)
	require.NoError(t, err)
	require.NoError(t, launch.close())
	require.Len(t, stamped, 1)
	require.Equal(t, turnSupervisorOriginShared, stamped[0].AuthorityOrigin)
	require.False(t, stamped[0].IdentityLock)
	require.False(t, stamped[0].AuthorityDomain)
	require.Nil(t, stamped[0].StandaloneOwner)

	stamped = nil
	launch, err = prepareProcessTreeCommand(
		exec.Command("/bin/true"), processLaunchOptions{Isolation: supervisorTestIsolation()},
	)
	require.NoError(t, err)
	require.NoError(t, launch.close())
	require.Len(t, stamped, 1)
	require.Empty(t, stamped[0].AuthorityOrigin)
}

// TestSupervisorConfigRefusesAnAuthorityOriginItsIdentityContradicts proves the
// stamp is never followed on its own: each child re-derives the decision from
// its own identity and refuses a config that disagrees in either direction, and
// a shared stamp that carries a capability or a standalone field is refused
// because the arm creates neither.
func TestSupervisorConfigRefusesAnAuthorityOriginItsIdentityContradicts(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)
	processIsolationGeteuid = func() int { return 1000 }

	shared := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"},
		Isolation: *sharedSupervisorIsolation(), AuthorityOrigin: turnSupervisorOriginShared,
	}
	require.NoError(t, validateTurnSupervisorConfig(shared))

	foreign := shared
	foreign.Isolation.UID, foreign.Isolation.GID = 65534, 65534
	require.ErrorContains(t, validateTurnSupervisorConfig(foreign),
		"amp native supervisor authority origin does not match the identity it runs as")

	for _, origin := range []string{"", turnSupervisorOriginBorrowed, turnSupervisorOriginStandalone} {
		contradicted := shared
		contradicted.AuthorityOrigin = origin
		require.ErrorContains(t, validateTurnSupervisorConfig(contradicted),
			"amp native supervisor authority origin does not match the identity it runs as")
	}

	for name, corrupt := range map[string]func(*turnSupervisorConfig){
		"identity_lock": func(config *turnSupervisorConfig) {
			config.IdentityLock, config.AuthorityDomain = true, true
		},
		"standalone_owner": func(config *turnSupervisorConfig) {
			config.StandaloneOwner = &agentStandaloneOwner{}
		},
		"owner_id": func(config *turnSupervisorConfig) {
			config.Isolation.StandaloneOwnerID = "acp-go-amp-shared"
		},
		"state_root": func(config *turnSupervisorConfig) {
			config.Isolation.StandaloneStateRoot = "/var/tmp/acp-go-amp-shared"
		},
	} {
		t.Run(name, func(t *testing.T) {
			inconsistent := shared
			corrupt(&inconsistent)
			require.ErrorContains(t, validateTurnSupervisorConfig(inconsistent),
				"amp native supervisor shared authority origin is inconsistent")
		})
	}
}

// TestSharedIdentityGuardianTakesNoAgentAuthority proves the guardian and the
// liveness child both take an empty authority under a shared identity: nothing
// is claimed, nothing is adopted, and the disposition validators that
// self-quarantine on a durable record have no record to read.
func TestSharedIdentityGuardianTakesNoAgentAuthority(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)
	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		t.Error("a shared identity claimed a standalone agent identity")

		return nil, errors.New("unreachable")
	}

	config := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"},
		Isolation: *sharedSupervisorIsolation(), AuthorityOrigin: turnSupervisorOriginShared,
	}

	authority, err := acquireTurnSupervisorAuthority(config, 7, 8, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, authority)
	require.Nil(t, authority.standalone)
	require.NoError(t, validateTurnSupervisorAuthorityDisposition(config, authority))
	require.NoError(t, authority.Close())

	identity, domain, standalone, err := acquireTurnSupervisorNativeAuthority(config, 7, 8, nil, nil)
	require.NoError(t, err)
	require.Nil(t, standalone)
	require.NotNil(t, identity)
	require.NotNil(t, domain)
	require.NoError(t, errors.Join(identity.Close(), domain.Close()))
}

// TestSharedIdentityLivenessInheritsTheSharedOrigin proves the origin survives
// the re-stamp the guardian performs for its liveness child. The borrowed and
// standalone legs rewrite it from the authority they hold, and a shared launch
// holds none, so without carrying it the child would decode a config its own
// identity contradicts.
func TestSharedIdentityLivenessInheritsTheSharedOrigin(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)
	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorExecutable = func() (string, error) { return "/bin/true", nil }
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd { return exec.Command(name, args...) }

	var sealed []turnSupervisorConfig

	turnSupervisorWriteConfig = func(file io.WriteSeeker, config turnSupervisorConfig) error {
		sealed = append(sealed, config)

		return writeTurnSupervisorConfig(file, config)
	}

	controlRead, controlWrite, err := os.Pipe()
	require.NoError(t, err)

	defer controlRead.Close()
	defer controlWrite.Close()

	completionRead, completionWrite, err := os.Pipe()
	require.NoError(t, err)

	defer completionRead.Close()
	defer completionWrite.Close()

	liveness, data, peer, err := startTurnSupervisorLiveness(
		turnSupervisorConfig{
			Path: "/bin/true", Args: []string{"/bin/true"},
			Isolation: *sharedSupervisorIsolation(), AuthorityOrigin: turnSupervisorOriginShared,
		},
		controlRead, completionWrite,
		&turnSupervisorAuthority{identity: &agentIdentityLock{}, domain: &agentIdentityLock{}},
	)
	require.NoError(t, err)

	defer data.Close()
	defer peer.Close()

	require.NoError(t, liveness.Wait())
	require.Len(t, sealed, 1)
	require.Equal(t, turnSupervisorOriginShared, sealed[0].AuthorityOrigin)
	require.False(t, sealed[0].IdentityLock)
	require.False(t, sealed[0].AuthorityDomain)
	require.Nil(t, sealed[0].StandaloneOwner)
	require.NoError(t, validateTurnSupervisorConfig(sealed[0]))
}

// TestRunTurnSupervisorNativeLaunchesUnderASharedIdentity drives the liveness
// body end to end on the shared arm: no authority is taken, the native launch
// requests no credential change, readiness is published, and the tree is
// contained.
func TestRunTurnSupervisorNativeLaunchesUnderASharedIdentity(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)
	processIsolationGeteuid = func() int { return 1000 }
	processIsolationGetegid = func() int { return 1000 }
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	turnSupervisorProcessID = func() int { return 99 }
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		t.Error("a shared identity claimed a standalone agent identity")

		return nil, errors.New("unreachable")
	}

	var native *exec.Cmd

	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		native = exec.Command(name, args...)

		return native
	}

	contained := 0
	turnSupervisorContain = func(int, int) error {
		contained++

		return nil
	}

	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", "exit 0"},
		Isolation: *sharedSupervisorIsolation(), AuthorityOrigin: turnSupervisorOriginShared,
	})

	var ready bytes.Buffer

	// The control channel has to stay open for the launch: a reader that is
	// already at EOF races the native exit and turns the successful path into a
	// control-side teardown.
	controlRead, controlWrite := io.Pipe()

	require.NoError(t, runTurnSupervisor(config, controlRead, &ready))
	require.NoError(t, controlWrite.Close())
	require.Equal(t, "ready\n", ready.String())
	require.Equal(t, 1, contained)
	require.NotNil(t, native)
	require.NotNil(t, native.SysProcAttr)
	require.Nil(t, native.SysProcAttr.Credential)
}

// TestSharedIdentitySupervisorContainsARealNativeLaunch is the unseamed proof:
// an unprivileged supervisor prepares the production launch, self-execs the
// guardian and liveness pair, runs the native command under the identity it
// already holds, and completes the containment handshake. Root cannot run it,
// because root can never enter the arm.
func TestSharedIdentitySupervisorContainsARealNativeLaunch(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("a shared agent identity requires an unprivileged supervisor")
	}

	isolation := &ProcessIsolation{
		UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()), BaseEnvironment: map[string]string{},
	}
	report := filepath.Join(t.TempDir(), "report")
	script := `echo "uid=$(id -u) gid=$(id -g)" > "$1"`
	native := exec.Command("/bin/sh", "-c", script, "probe", report) // #nosec G204 -- fixed test script.
	native.Args = []string{"sh", "-c", script, "probe", report}
	native.Env = []string{"PATH=/usr/bin:/bin"}

	launch, err := prepareProcessTreeCommand(native, processLaunchOptions{Isolation: isolation})
	require.NoError(t, err)

	tree, err := startProcessTree(launch)
	require.NoError(t, err)

	waitCtx, cancelWait := context.WithTimeout(t.Context(), 30*time.Second)
	waitErr, completed := tree.waiter.await(waitCtx)

	cancelWait()
	require.True(t, completed, "shared identity supervisor did not exit")
	require.NoError(t, errors.Join(waitErr, processTreeTerminateAndWait(tree, commandWaitTimeout)))

	result, err := os.ReadFile(report) // #nosec G304 -- test-owned temporary path.
	require.NoError(t, err)
	require.Equal(t, "uid="+strconv.Itoa(os.Geteuid())+" gid="+strconv.Itoa(os.Getegid())+"\n", string(result))
}
