package ampacp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func TestAgentContainmentModeAndObservation(t *testing.T) {
	if got := (*Agent)(nil).ContainmentMode(); got != RuntimeContainmentUnavailable {
		t.Fatalf("nil agent mode = %q", got)
	}
	var observed []RuntimeContainmentMode
	defaultAgent := NewAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
			observed = append(observed, mode)
		},
	}))
	var want RuntimeContainmentMode
	switch runtime.GOOS {
	case "linux":
		// No explicit policy means the native tree runs as this process's own
		// identity, and the report says so.
		want = RuntimeContainmentSharedIdentity
	default:
		want = RuntimeContainmentUnavailable
	}
	if got := defaultAgent.ContainmentMode(); got != want {
		t.Fatalf("default mode = %q, want %q", got, want)
	}
	if len(observed) != 1 || observed[0] != want {
		t.Fatalf("containment observations = %v", observed)
	}

	var logs bytes.Buffer
	var snapshots int
	opted := NewAgent(
		WithDarwinBestEffortContainment(),
		WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { snapshots++ },
		}),
	)
	if runtime.GOOS == "darwin" {
		if opted.ContainmentMode() != RuntimeContainmentBestEffort {
			t.Fatalf("opted mode = %q", opted.ContainmentMode())
		}
		if !strings.Contains(logs.String(), `"containment":"best_effort"`) || !strings.Contains(logs.String(), "escaped descendants may survive") {
			t.Fatalf("structured best-effort warning = %q", logs.String())
		}
		observer := opted.newProcessSnapshotObserver(t.Context(), func() (int, bool) { return 7, true })
		observer.Refresh(t.Context())
		observer.Complete(t.Context())
		observer.Incomplete()
		if snapshots != 0 {
			t.Fatalf("best-effort provider snapshots = %d", snapshots)
		}

		return
	}
	if opted.ContainmentMode() != RuntimeContainmentUnavailable {
		t.Fatalf("off-Darwin opted mode = %q", opted.ContainmentMode())
	}
	if _, err := opted.Initialize(t.Context(), acp.InitializeRequest{}); err == nil || !strings.Contains(err.Error(), "supported only on darwin") {
		t.Fatalf("off-Darwin opt-in initialization error = %v", err)
	}
}

func TestConfigureNativeClientDarwinGenerationResources(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin generation registry is platform-specific")
	}

	originalMkdir := mkdirTemp
	originalRemove := removeSessionDir
	t.Cleanup(func() {
		mkdirTemp = originalMkdir
		removeSessionDir = originalRemove
	})

	want := errors.New("resource")
	reserved := 0
	released := 0
	newConfigured := func(scratch string, reserve func(context.Context, RuntimeResourceKind) (func(), error)) nativeamp.Options {
		agent := NewAgent(
			WithScratchDir(scratch),
			WithDarwinBestEffortContainment(),
			WithRuntimeResourceHooks(RuntimeResourceHooks{ReserveScratchRoot: reserve}),
		)
		var options nativeamp.Options
		agent.configureNativeClient(&options, RuntimeResourcePrompt)

		return options
	}

	options := newConfigured(t.TempDir(), func(context.Context, RuntimeResourceKind) (func(), error) { return nil, want })
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) {
		t.Fatalf("reserve error = %v", err)
	}

	fileParent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileParent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	reserve := func(context.Context, RuntimeResourceKind) (func(), error) {
		reserved++

		return func() { released++ }, nil
	}
	options = newConfigured(fileParent, reserve)
	if _, err := options.NewDarwinGeneration(t.Context()); err == nil || reserved != 1 || released != 1 {
		t.Fatalf("scratch-parent error=%v reserved=%d released=%d", err, reserved, released)
	}

	parent := t.TempDir()
	mkdirTemp = func(string, string) (string, error) { return "", want }
	options = newConfigured(parent, reserve)
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) || released != 2 {
		t.Fatalf("mkdir error=%v released=%d", err, released)
	}
	mkdirTemp = originalMkdir

	registry := filepath.Join(parent, "acp-go-amp-containment")
	if err := os.WriteFile(registry, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	options = newConfigured(parent, reserve)
	if _, err := options.NewDarwinGeneration(t.Context()); err == nil || released != 3 {
		t.Fatalf("record error=%v released=%d", err, released)
	}
	removeSessionDir = func(string) error { return want }
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) || released != 3 {
		t.Fatalf("record/remove error=%v released=%d", err, released)
	}
	removeSessionDir = originalRemove
	if err := os.Remove(registry); err != nil {
		t.Fatal(err)
	}

	generation, err := options.NewDarwinGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	beforeRelease := released
	if releaseErr := generation.Release(false); releaseErr != nil || released != beforeRelease {
		t.Fatalf("incomplete release error=%v releases=%d", releaseErr, released)
	}
	removeSessionDir = func(string) error { return want }
	if releaseErr := generation.Release(true); !errors.Is(releaseErr, want) || released != beforeRelease {
		t.Fatalf("failed complete release error=%v releases=%d", releaseErr, released)
	}
	removeSessionDir = originalRemove

	generation, err = options.NewDarwinGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := generation.Release(true); releaseErr != nil || released != beforeRelease+1 {
		t.Fatalf("complete release error=%v releases=%d", releaseErr, released)
	}
}

func TestSimulatedDarwinContainmentConfiguration(t *testing.T) {
	originalGOOS := runtimeGOOS
	originalMkdir := mkdirTemp
	originalRemove := removeSessionDir
	originalRecord := newDarwinGenerationRecord
	t.Cleanup(func() {
		runtimeGOOS = originalGOOS
		mkdirTemp = originalMkdir
		removeSessionDir = originalRemove
		newDarwinGenerationRecord = originalRecord
	})
	runtimeGOOS = platformDarwin

	var logs bytes.Buffer
	want := errors.New("resource failed")
	agent := NewAgent(
		WithDarwinBestEffortContainment(),
		WithScratchDir(testScratchDir(t)),
		WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot:  func(context.Context, RuntimeResourceKind) (func(), error) { return nil, want },
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, want },
		}),
	)
	if agent.ContainmentMode() != RuntimeContainmentBestEffort || !strings.Contains(logs.String(), "escaped descendants may survive") {
		t.Fatalf("simulated Darwin mode=%q logs=%q", agent.ContainmentMode(), logs.String())
	}

	var options nativeamp.Options
	agent.configureNativeClient(&options, RuntimeResourcePrompt)
	if _, err := options.AcquireNativeRoot(t.Context()); !errors.Is(err, want) {
		t.Fatalf("native admission = %v", err)
	}
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) {
		t.Fatalf("scratch reservation = %v", err)
	}

	released := 0
	agent.options.RuntimeResourceHooks.ReserveScratchRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
		return func() { released++ }, nil
	}
	fileParent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileParent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	agent.options.ScratchDir = fileParent
	agent.configureNativeClient(&options, RuntimeResourcePrompt)
	if _, err := options.NewDarwinGeneration(t.Context()); err == nil || released != 1 {
		t.Fatalf("scratch parent = %v, released=%d", err, released)
	}

	agent.options.ScratchDir = t.TempDir()
	mkdirTemp = func(string, string) (string, error) { return "", want }
	agent.configureNativeClient(&options, RuntimeResourcePrompt)
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) || released != 2 {
		t.Fatalf("generation root = %v, released=%d", err, released)
	}
	mkdirTemp = func(string, string) (string, error) { return t.TempDir(), nil }

	newDarwinGenerationRecord = func(string, string, string) (*nativeamp.DarwinGeneration, error) { return nil, want }
	removeSessionDir = func(string) error { return nil }
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) || released != 3 {
		t.Fatalf("generation record = %v, released=%d", err, released)
	}
	removeSessionDir = func(string) error { return want }
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) || released != 3 {
		t.Fatalf("record cleanup = %v, released=%d", err, released)
	}

	removeSessionDir = func(string) error { return nil }
	newDarwinGenerationRecord = func(string, string, string) (*nativeamp.DarwinGeneration, error) {
		return &nativeamp.DarwinGeneration{}, nil
	}
	generation, err := options.NewDarwinGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := generation.Release(false); releaseErr != nil || released != 3 {
		t.Fatalf("incomplete release = %v, released=%d", releaseErr, released)
	}
	removeSessionDir = func(string) error { return want }
	if releaseErr := generation.Release(true); !errors.Is(releaseErr, want) || released != 3 {
		t.Fatalf("failed release = %v, released=%d", releaseErr, released)
	}
	removeSessionDir = func(string) error { return nil }
	generation, err = options.NewDarwinGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := generation.Release(true); err != nil || released != 4 {
		t.Fatalf("complete release = %v, released=%d", err, released)
	}
}

// TestContainmentModeReportsASharedAgentIdentity proves the Linux boundary
// names the identity it actually established: the authoritative report is kept
// for a launch that crosses a credential boundary, and a launch that runs the
// agent under the supervisor's own identity says so instead. An omitted policy
// is such a launch by construction — root or not — and no other platform
// changes.
func TestContainmentModeReportsASharedAgentIdentity(t *testing.T) {
	originalGOOS, originalUID := runtimeGOOS, containmentEffectiveUID
	t.Cleanup(func() { runtimeGOOS, containmentEffectiveUID = originalGOOS, originalUID })

	shared := &ProcessIsolation{UID: 1000, GID: 1000}
	distinct := &ProcessIsolation{UID: 65534, GID: 65534}

	runtimeGOOS = platformLinux
	containmentEffectiveUID = func() int { return 1000 }
	if got := containmentMode(Options{ProcessIsolation: shared}); got != RuntimeContainmentSharedIdentity {
		t.Fatalf("shared identity mode = %q", got)
	}
	if got := containmentMode(Options{ProcessIsolation: distinct}); got != RuntimeContainmentAuthoritative {
		t.Fatalf("distinct identity mode = %q", got)
	}
	if got := containmentMode(Options{}); got != RuntimeContainmentSharedIdentity {
		t.Fatalf("absent policy mode = %q", got)
	}

	containmentEffectiveUID = func() int { return 0 }
	if got := containmentMode(Options{ProcessIsolation: shared}); got != RuntimeContainmentAuthoritative {
		t.Fatalf("trusted root mode = %q", got)
	}
	if got := containmentMode(Options{}); got != RuntimeContainmentSharedIdentity {
		t.Fatalf("absent policy root mode = %q", got)
	}

	containmentEffectiveUID = func() int { return 1000 }
	runtimeGOOS = platformDarwin
	if got := containmentMode(Options{ProcessIsolation: shared}); got != RuntimeContainmentUnavailable {
		t.Fatalf("Darwin shared identity mode = %q", got)
	}

	if !RuntimeContainmentSharedIdentity.provesWholeTreeLifecycle() ||
		!RuntimeContainmentAuthoritative.provesWholeTreeLifecycle() {
		t.Fatal("a Linux boundary stopped proving whole-tree lifecycle")
	}
	if RuntimeContainmentBestEffort.provesWholeTreeLifecycle() ||
		RuntimeContainmentUnavailable.provesWholeTreeLifecycle() {
		t.Fatal("a weaker boundary claimed whole-tree lifecycle")
	}
}

// TestSharedIdentityAgentKeepsItsLifecycleSurfaces proves the honest report
// does not quietly take away what the boundary still proves: the agent
// publishes shared_identity and keeps the descendant inventory the subreaper
// tree makes truthful.
func TestSharedIdentityAgentKeepsItsLifecycleSurfaces(t *testing.T) {
	originalGOOS, originalUID := runtimeGOOS, containmentEffectiveUID
	t.Cleanup(func() { runtimeGOOS, containmentEffectiveUID = originalGOOS, originalUID })

	runtimeGOOS = platformLinux
	containmentEffectiveUID = func() int { return 1000 }

	var (
		observed  []RuntimeContainmentMode
		snapshots int
	)
	agent := NewAgent(
		WithProcessIsolation(ProcessIsolation{
			UID: 1000, GID: 1000, BaseEnvironment: map[string]string{"PATH": "/usr/bin"},
		}),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
				observed = append(observed, mode)
			},
			ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { snapshots++ },
		}),
	)
	if len(observed) != 1 || observed[0] != RuntimeContainmentSharedIdentity {
		t.Fatalf("shared identity observations = %v", observed)
	}
	if got := agent.ContainmentMode(); got != RuntimeContainmentSharedIdentity {
		t.Fatalf("shared identity agent mode = %q", got)
	}

	observer := agent.newProcessSnapshotObserver(t.Context(), func() (int, bool) { return 3, true })
	observer.Refresh(t.Context())
	observer.Complete(t.Context())
	if snapshots == 0 {
		t.Fatal("shared identity agent stopped publishing descendant snapshots")
	}
}

// TestSharedIdentityProcessIsolationOptionCarriesNoStandaloneOwnerFields proves
// the public option validator moves with the native one: the canonical shared
// shape is accepted, standalone fields that promise a durable record are
// refused with a remedy, and the isolated refusals are untouched.
func TestSharedIdentityProcessIsolationOptionCarriesNoStandaloneOwnerFields(t *testing.T) {
	originalGOOS, originalUID := runtimeGOOS, containmentEffectiveUID
	t.Cleanup(func() { runtimeGOOS, containmentEffectiveUID = originalGOOS, originalUID })

	runtimeGOOS = platformLinux
	containmentEffectiveUID = func() int { return 1000 }

	if err := validateProcessIsolationOption(&ProcessIsolation{UID: 1000, GID: 1000}); err != nil {
		t.Fatalf("canonical shared policy: %v", err)
	}
	err := validateProcessIsolationOption(&ProcessIsolation{
		UID: 1000, GID: 1000, StandaloneOwnerID: "acp-go-amp-shared",
	})
	if err == nil || !strings.Contains(err.Error(), sharedIdentitySupervisorRemedy) {
		t.Fatalf("shared owner id error = %v", err)
	}
	err = validateProcessIsolationOption(&ProcessIsolation{
		UID: 1000, GID: 1000, StandaloneStateRoot: "/var/tmp/acp-go-amp-shared",
	})
	if err == nil || !strings.Contains(err.Error(), sharedIdentitySupervisorRemedy) {
		t.Fatalf("shared state root error = %v", err)
	}
	err = validateProcessIsolationOption(&ProcessIsolation{UID: 65534, GID: 65534})
	if err == nil || !strings.Contains(err.Error(), "standalone owner id must match") {
		t.Fatalf("isolated policy error = %v", err)
	}
}

// TestAgentSessionDefaultsToOrdinaryExecution proves omitting
// WithProcessIsolation is the ordinary default: session establishment
// succeeds, the ambient credential is honored through the implicit base
// environment, and every native client is handed a clone of the one
// current-identity capture rather than an isolation policy.
func TestAgentSessionDefaultsToOrdinaryExecution(t *testing.T) {
	t.Setenv("AMP_API_KEY", "ambient-key")
	t.Setenv("ACP_GO_AMP_TEST_CANARY", "ambient-canary")

	probes := 0
	agent := NewAgent(WithScratchDir(testScratchDir(t)))
	agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) error {
		probes++

		return nil
	}
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	resp, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession without isolation: %v", err)
	}
	if resp.SessionId == "" || probes != 1 {
		t.Fatalf("session = %q, probes = %d", resp.SessionId, probes)
	}

	isolation := agent.nativeIsolation()
	if isolation == nil || !isolation.Implicit {
		t.Fatalf("native isolation = %#v, want implicit capture", isolation)
	}
	if int64(isolation.UID) != int64(os.Geteuid()) || int64(isolation.GID) != int64(os.Getegid()) {
		t.Fatalf("implicit identity = %d:%d, process runs as %d:%d", isolation.UID, isolation.GID, os.Geteuid(), os.Getegid())
	}
	if isolation.BaseEnvironment["ACP_GO_AMP_TEST_CANARY"] != "ambient-canary" ||
		isolation.BaseEnvironment["AMP_API_KEY"] != "ambient-key" {
		t.Fatalf("implicit base environment missed ambient values: %#v", isolation.BaseEnvironment)
	}
	if isolation.IdentityLock != nil || isolation.AuthorityDomain != nil ||
		isolation.StandaloneOwnerID != "" || isolation.StandaloneStateRoot != "" {
		t.Fatalf("implicit capture carries isolation authority: %#v", isolation)
	}

	isolation.BaseEnvironment["ACP_GO_AMP_TEST_CANARY"] = "mutated"
	if agent.nativeIsolation().BaseEnvironment["ACP_GO_AMP_TEST_CANARY"] != "ambient-canary" {
		t.Fatal("implicit capture is shared rather than cloned")
	}

	if sharedProcessIdentity(nil) {
		t.Fatal("a nil policy reported an explicit shared identity")
	}
	if (&Agent{}).nativeIsolation() != nil {
		t.Fatal("an agent without any capture produced a launch policy")
	}
}

// TestExplicitProcessIsolationPreservesPolicy proves supplying
// WithProcessIsolation stays explicit hardening: the policy reaches native
// clients verbatim with no ambient environment mixed in, and an invalid
// policy fails session establishment closed with no ordinary-mode fallback.
func TestExplicitProcessIsolationPreservesPolicy(t *testing.T) {
	t.Setenv("ACP_GO_AMP_TEST_CANARY", "ambient-canary")

	uid, gid := testIsolationIdentity()
	policy := ProcessIsolation{
		UID: uid, GID: gid,
		BaseEnvironment: map[string]string{"PATH": "/policy/bin", "AMP_API_KEY": "policy-key"},
	}
	if testIsolationClaimsStandaloneAuthority(uid) {
		policy.StandaloneOwnerID = "acp-go-amp-tests"
		policy.StandaloneStateRoot = testStandaloneStateRoot(uid, gid)
	}

	agent := NewAgent(WithProcessIsolation(policy))
	isolation := agent.nativeIsolation()
	if isolation == nil || isolation.Implicit {
		t.Fatalf("native isolation = %#v, want the explicit policy", isolation)
	}
	if isolation.UID != policy.UID || isolation.GID != policy.GID {
		t.Fatalf("explicit identity = %d:%d, want %d:%d", isolation.UID, isolation.GID, policy.UID, policy.GID)
	}
	if _, ambient := isolation.BaseEnvironment["ACP_GO_AMP_TEST_CANARY"]; ambient {
		t.Fatal("explicit policy absorbed ambient environment")
	}
	if isolation.BaseEnvironment["AMP_API_KEY"] != "policy-key" {
		t.Fatalf("explicit base environment = %#v", isolation.BaseEnvironment)
	}
	if agent.implicitIsolation != nil {
		t.Fatal("explicit policy still captured an implicit fallback")
	}

	invalid := NewAgent(WithProcessIsolation(ProcessIsolation{UID: 0, GID: 0}))
	if _, err := invalid.NewSession(t.Context(), NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("invalid explicit policy did not fail session establishment")
	}
	t.Cleanup(func() {
		_ = agent.Close()
		_ = invalid.Close()
	})
}
