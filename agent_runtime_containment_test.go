package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
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
	want := RuntimeContainmentSharedIdentity
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

func TestContainmentModeReportsOrdinaryExecutionWithoutAuthority(t *testing.T) {
	originalGOOS := runtimeGOOS
	t.Cleanup(func() { runtimeGOOS = originalGOOS })

	for _, platform := range []string{platformLinux, platformDarwin, platformWindows, "freebsd"} {
		runtimeGOOS = platform
		if got := containmentMode(Options{}); got != RuntimeContainmentSharedIdentity {
			t.Fatalf("%s ordinary mode = %q", platform, got)
		}
	}

	runtimeGOOS = platformLinux
	if got := containmentMode(Options{ProcessIsolation: &ProcessIsolation{UID: 65534, GID: 65534}}); got != RuntimeContainmentAuthoritative {
		t.Fatalf("Linux explicit mode = %q", got)
	}
	runtimeGOOS = platformDarwin
	if got := containmentMode(Options{ProcessIsolation: &ProcessIsolation{UID: 65534, GID: 65534}}); got != RuntimeContainmentUnavailable {
		t.Fatalf("Darwin explicit mode = %q", got)
	}

	if !RuntimeContainmentAuthoritative.provesWholeTreeLifecycle() {
		t.Fatal("authoritative containment stopped proving whole-tree lifecycle")
	}
	for _, mode := range []RuntimeContainmentMode{RuntimeContainmentSharedIdentity, RuntimeContainmentBestEffort, RuntimeContainmentUnavailable} {
		if mode.provesWholeTreeLifecycle() {
			t.Fatalf("mode %q claimed whole-tree lifecycle", mode)
		}
	}
}

func TestOrdinaryExecutionPublishesNoDescendantInventory(t *testing.T) {
	var snapshots int
	agent := NewAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { snapshots++ },
	}))

	observer := agent.newProcessSnapshotObserver(t.Context(), func() (int, bool) { return 3, true })
	observer.Refresh(t.Context())
	observer.Complete(t.Context())
	observer.Incomplete()
	if snapshots != 0 {
		t.Fatalf("ordinary execution published %d descendant snapshots", snapshots)
	}
}

func TestExplicitProcessIsolationAlwaysRequiresAuthority(t *testing.T) {
	originalGOOS := runtimeGOOS
	t.Cleanup(func() { runtimeGOOS = originalGOOS })
	runtimeGOOS = platformLinux

	err := validateProcessIsolationOption(&ProcessIsolation{UID: 1000, GID: 1000})
	if err == nil || !strings.Contains(err.Error(), "standalone owner id must match") {
		t.Fatalf("authority-free explicit policy error = %v", err)
	}
}

// ordinaryHarnessProbeThreadID is the deliberately missing thread id the native
// startup probe uses for its method-present checks. Those probe launches are
// startup, not the prompt's execute turn, so the canonical gate classifies them
// separately.
const ordinaryHarnessProbeThreadID = "T-00000000-0000-0000-0000-000000000000"

// ordinaryHarnessLaunch is one recorded native invocation of the containment
// fixture harness.
type ordinaryHarnessLaunch struct {
	Args    []string `json:"args"`
	UID     int      `json:"uid"`
	EUID    int      `json:"euid"`
	Private []string `json:"private"`
}

// ordinaryAmpHarnessSource is a fake Amp the ordinary launch path can really
// run. Every invocation appends the identity it ran as and the private adapter
// environment it was handed, so the canonical containment tests can count
// spawns and classify each one instead of inferring either from constructed
// options. An ordinary launch carries no adapter-private environment key at
// all; the hardened launcher consumes and scrubs its private handoff before
// native exec.
const ordinaryAmpHarnessSource = `package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const privatePrefix = "ACP_" + "GO_AMP_INTERNAL_"

func main() {
	args := os.Args[1:]
	state := os.Getenv("ORDINARY_HARNESS_STATE")

	private := []string{}
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, privatePrefix) {
			private = append(private, strings.SplitN(entry, "=", 2)[0])
		}
	}
	record(state, map[string]any{
		"args": args, "uid": os.Getuid(), "euid": os.Geteuid(), "private": private,
	})

	for _, arg := range args {
		if arg == "T-00000000-0000-0000-0000-000000000000" {
			os.Stderr.WriteString("Thread not found\n")
			os.Exit(1)
		}
	}

	if len(args) > 0 && args[0] == "version" {
		os.Stdout.WriteString("0.0.1784765892-gfake\n")
		return
	}

	for i, arg := range args {
		if arg == "threads" && i+1 < len(args) && args[i+1] == "list" {
			os.Stdout.WriteString("[]\n")
			return
		}
	}

	_, _ = io.ReadAll(os.Stdin)
	os.Stdout.WriteString("{\"type\":\"system\",\"subtype\":\"init\",\"cwd\":\"/tmp/project\",\"session_id\":\"T-ordinary-thread\"}\n")
	os.Stdout.WriteString("{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"ordinary\"}]},\"session_id\":\"T-ordinary-thread\"}\n")
	os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"success\",\"duration_ms\":1,\"is_error\":false,\"num_turns\":1,\"result\":\"done\",\"session_id\":\"T-ordinary-thread\"}\n")
}

func record(state string, value any) {
	if state == "" {
		return
	}
	file, err := os.OpenFile(filepath.Join(state, "launch.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(3)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(value); err != nil {
		os.Exit(3)
	}
}
`

// ordinaryAmpHarness builds the fixture harness and returns its path plus the
// state directory it records launches into. The state directory is published to
// the child through the ambient environment, so a launch that never reaches the
// child is indistinguishable from no launch at all: an empty ledger.
func ordinaryAmpHarness(t *testing.T) (string, string) {
	t.Helper()

	dir, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	state := filepath.Join(dir, "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}

	source := filepath.Join(dir, "ordinary_amp.go")
	if err := os.WriteFile(source, []byte(ordinaryAmpHarnessSource), 0o600); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "amp")
	if out, buildErr := exec.Command("go", "build", "-o", path, source).CombinedOutput(); buildErr != nil {
		t.Fatalf("build ordinary amp harness: %v\n%s", buildErr, out)
	}

	t.Setenv("ORDINARY_HARNESS_STATE", state)

	return path, state
}

func ordinaryHarnessLaunches(t *testing.T, state string) []ordinaryHarnessLaunch {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(state, "launch.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}

	var launches []ordinaryHarnessLaunch

	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		var launch ordinaryHarnessLaunch
		if unmarshalErr := json.Unmarshal([]byte(line), &launch); unmarshalErr != nil {
			t.Fatalf("decode harness launch %q: %v", line, unmarshalErr)
		}

		launches = append(launches, launch)
	}

	return launches
}

// TestAgentSessionDefaultsToOrdinaryExecution is the canonical ordinary-default
// gate. It establishes a real session and runs a real prompt against a native
// harness, then proves the three properties the default owes: the child ran as
// the identity that supervises it (root or not), it was handed no authority,
// and no descendant inventory was published for it.
func TestAgentSessionDefaultsToOrdinaryExecution(t *testing.T) {
	t.Setenv("AMP_API_KEY", "ambient-key")
	t.Setenv("ACP_GO_AMP_TEST_ACTUAL_AMBIENT", "ambient-canary")

	harness, state := ordinaryAmpHarness(t)

	var snapshots, containments int

	var observedModes []RuntimeContainmentMode

	agent := NewAgent(
		WithExecutablePath(harness),
		WithScratchDir(testScratchDir(t)),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { snapshots++ },
			ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
				containments++
				observedModes = append(observedModes, mode)
			},
		}),
	)
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	resp, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession without isolation: %v", err)
	}
	if resp.SessionId == "" {
		t.Fatal("ordinary session established no id")
	}

	promptResp, err := agent.Prompt(t.Context(), TextPromptRequest(resp.SessionId, "ordinary-turn", "hello"))
	if err != nil {
		t.Fatalf("ordinary prompt: %v", err)
	}
	if promptResp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("ordinary prompt stop reason = %q", promptResp.StopReason)
	}

	launches := ordinaryHarnessLaunches(t, state)
	if len(launches) == 0 {
		t.Fatal("ordinary session and prompt spawned no native process")
	}

	var probed, executed int

	for _, launch := range launches {
		// Identity: ordinary execution never changes who runs the child. On a
		// root runner that means the child is root; on any other runner it is
		// that same unprivileged identity. Both are the supervisor's identity,
		// which is the whole claim.
		if launch.UID != os.Getuid() || launch.EUID != os.Geteuid() {
			t.Fatalf("ordinary child identity = %d/%d, want the supervisor's %d/%d", launch.UID, launch.EUID, os.Getuid(), os.Geteuid())
		}
		// Authority: the hardened launcher is the only thing that hands a child
		// private adapter state, and it was never selected.
		if len(launch.Private) != 0 {
			t.Fatalf("ordinary child received private adapter environment %v", launch.Private)
		}

		switch {
		case len(launch.Args) > 0 && launch.Args[0] == "version":
			probed++
		case slices.Contains(launch.Args, ordinaryHarnessProbeThreadID):
		case slices.Contains(launch.Args, "-x"):
			executed++
		}
	}

	if probed == 0 {
		t.Fatalf("ordinary startup ran no version probe: %#v", launches)
	}
	if executed != 1 {
		t.Fatalf("ordinary prompt ran %d execute launches, want 1", executed)
	}
	if os.Geteuid() == 0 {
		t.Log("ordinary default executed under a root supervisor: no de-privileging or adapter-private control state")
	}

	// No inventory: ordinary execution cannot see a whole process tree, so it
	// publishes no descendant counts rather than an unproven number.
	if snapshots != 0 {
		t.Fatalf("ordinary execution published %d descendant snapshots", snapshots)
	}
	if containments != 1 || observedModes[0] != RuntimeContainmentSharedIdentity {
		t.Fatalf("ordinary containment observations = %v", observedModes)
	}
	if agent.ContainmentMode().provesWholeTreeLifecycle() {
		t.Fatal("ordinary execution claimed whole-tree lifecycle")
	}

	var options nativeamp.Options
	agent.configureNativeClient(&options, RuntimeResourcePrompt)
	if options.Isolation != nil {
		t.Fatalf("ordinary native isolation = %#v, want nil", options.Isolation)
	}
	if options.OrdinaryEnvironment["ACP_GO_AMP_TEST_ACTUAL_AMBIENT"] != "ambient-canary" ||
		options.OrdinaryEnvironment["AMP_API_KEY"] != "ambient-key" {
		t.Fatalf("ordinary environment missed ambient values: %#v", options.OrdinaryEnvironment)
	}
}

// TestExplicitProcessIsolationPreservesPolicy is the canonical explicit-policy
// gate. An explicit policy is carried verbatim, selects the authoritative
// launcher, and is fail-closed everywhere it cannot be honoured: each refusal
// establishes no session, spawns nothing, and never retries the launch through
// the ordinary launcher.
func TestExplicitProcessIsolationPreservesPolicy(t *testing.T) {
	t.Setenv("AMP_API_KEY", "ambient-key")
	t.Setenv("ACP_GO_AMP_TEST_ACTUAL_AMBIENT", "ambient-canary")

	harness, state := ordinaryAmpHarness(t)

	uid, gid := testIsolationIdentity()
	policy := ProcessIsolation{
		UID: uid, GID: gid,
		BaseEnvironment: map[string]string{"PATH": "/policy/bin", "AMP_API_KEY": "policy-key"},
	}
	if testIsolationClaimsStandaloneAuthority(uid) {
		policy.StandaloneOwnerID = "acp-go-amp-tests"
		policy.StandaloneStateRoot = testStandaloneStateRoot(t, uid, gid)
	}

	agent := NewAgent(WithProcessIsolation(policy))
	t.Cleanup(func() { _ = agent.Close() })

	var nativeOptions nativeamp.Options
	agent.configureNativeClient(&nativeOptions, RuntimeResourcePrompt)
	isolation := nativeOptions.Isolation
	if isolation == nil {
		t.Fatalf("native isolation = %#v, want the explicit policy", isolation)
	}
	if isolation.UID != policy.UID || isolation.GID != policy.GID {
		t.Fatalf("explicit identity = %d:%d, want %d:%d", isolation.UID, isolation.GID, policy.UID, policy.GID)
	}
	if _, ambient := isolation.BaseEnvironment["ACP_GO_AMP_TEST_ACTUAL_AMBIENT"]; ambient {
		t.Fatal("explicit policy absorbed ambient environment")
	}
	if isolation.BaseEnvironment["AMP_API_KEY"] != "policy-key" {
		t.Fatalf("explicit base environment = %#v", isolation.BaseEnvironment)
	}

	originalGOOS := runtimeGOOS
	t.Cleanup(func() { runtimeGOOS = originalGOOS })

	// Selection: a valid strict policy is a distinct authoritative mode, not
	// the ordinary default wearing a policy. The state root only has to be a
	// clean absolute path for validation; the launcher is what binds it.
	strictPolicy := ProcessIsolation{
		UID: 65534, GID: 65534,
		BaseEnvironment:     map[string]string{"PATH": filepath.Dir(harness)},
		StandaloneOwnerID:   "acp-go-amp-tests",
		StandaloneStateRoot: "/var/lib/acp-go-amp-tests",
	}

	runtimeGOOS = platformLinux
	if err := validateProcessIsolationOption(&strictPolicy); err != nil {
		t.Fatalf("valid strict policy rejected: %v", err)
	}

	mode := containmentMode(Options{ProcessIsolation: &strictPolicy})
	if mode != RuntimeContainmentAuthoritative || !mode.provesWholeTreeLifecycle() {
		t.Fatalf("valid strict policy mode = %q", mode)
	}

	runtimeGOOS = originalGOOS

	authorityFree := ProcessIsolation{UID: uid, GID: gid, BaseEnvironment: policy.BaseEnvironment}
	for _, refusal := range []struct {
		name   string
		goos   string
		policy ProcessIsolation
		want   string
	}{
		{name: "zero identity", goos: platformLinux, policy: ProcessIsolation{UID: 0, GID: 0}, want: "must be nonzero"},
		{name: "authority free identity", goos: platformLinux, policy: authorityFree, want: "standalone owner id must match"},
		{name: "unsupported darwin", goos: platformDarwin, policy: policy, want: "supported only on linux"},
		{name: "unsupported windows", goos: "windows", policy: policy, want: "supported only on linux"},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			runtimeGOOS = refusal.goos
			t.Cleanup(func() { runtimeGOOS = originalGOOS })

			refused := NewAgent(
				WithExecutablePath(harness),
				WithProcessIsolation(refusal.policy),
				WithScratchDir(testScratchDir(t)),
			)
			t.Cleanup(func() { _ = refused.Close() })

			_, err := refused.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
			if err == nil || !strings.Contains(err.Error(), refusal.want) {
				t.Fatalf("refusal = %v, want %q", err, refusal.want)
			}
			if launches := ordinaryHarnessLaunches(t, state); len(launches) != 0 {
				t.Fatalf("refused explicit policy spawned %d native processes: %#v", len(launches), launches)
			}
		})
	}

	// The valid strict policy against the real host launcher. Driving the
	// native client directly is what makes this a selection proof rather than
	// another validation proof: the refusal text can only come from the
	// platform's hardened launcher, which the ordinary path never reaches.
	nativeClient := nativeamp.NewClient(nil, nativeamp.Options{
		CLIPath: harness,
		Cwd:     filepath.Dir(harness),
		Isolation: &nativeamp.ProcessIsolation{
			UID: strictPolicy.UID, GID: strictPolicy.GID,
			BaseEnvironment:     map[string]string{"PATH": filepath.Dir(harness)},
			StandaloneOwnerID:   strictPolicy.StandaloneOwnerID,
			StandaloneStateRoot: strictPolicy.StandaloneStateRoot,
		},
	})

	switch {
	case runtime.GOOS != platformLinux:
		if _, err := nativeClient.Version(t.Context()); err == nil || !strings.Contains(err.Error(), "process isolation is unsupported") {
			t.Fatalf("off-platform strict selection = %v", err)
		}
		if launches := ordinaryHarnessLaunches(t, state); len(launches) != 0 {
			t.Fatalf("off-platform strict refusal spawned %d native processes: %#v", len(launches), launches)
		}
	case os.Geteuid() != 0:
		// The hardened Linux launcher was selected and refused a supervisor
		// that does not hold trusted root. The ordinary launcher has no such
		// check, so reaching this message is the selection evidence.
		if _, err := nativeClient.Version(t.Context()); err == nil || !strings.Contains(err.Error(), "trusted root identity is required") {
			t.Fatalf("non-root strict selection = %v", err)
		}
		if launches := ordinaryHarnessLaunches(t, state); len(launches) != 0 {
			t.Fatalf("non-root strict refusal spawned %d native processes: %#v", len(launches), launches)
		}
	default:
		// A root Linux runner can execute the valid selection directly. The
		// test-only no-credential switch keeps the fixture process at the
		// supervisor identity, but it does not bypass the authoritative
		// launcher, its private handoff, or its identity authority.
		strictPolicy.StandaloneStateRoot = testStandaloneStateRoot(t, strictPolicy.UID, strictPolicy.GID)
		strictPolicy.BaseEnvironment["ORDINARY_HARNESS_STATE"] = state
		strictAgentOptions := testContainmentOptions([]Option{
			WithExecutablePath(harness),
			WithProcessIsolation(strictPolicy),
			WithScratchDir(testScratchDir(t)),
		})
		identityRoot := t.TempDir()
		strictAgentOptions = append(strictAgentOptions, func(options *Options) {
			options.testOnlyIdentityLockRoot = identityRoot
		})
		strictAgent := NewAgent(strictAgentOptions...)
		t.Cleanup(func() { _ = strictAgent.Close() })

		var strictOptions nativeamp.Options
		strictAgent.configureNativeClient(&strictOptions, RuntimeResourcePrompt)
		strictOptions.CLIPath = harness
		strictOptions.Cwd = filepath.Dir(harness)
		if _, err := nativeamp.NewClient(nil, strictOptions).Version(t.Context()); err != nil {
			t.Fatalf("root hardened selection: %v", err)
		}
		if launches := ordinaryHarnessLaunches(t, state); len(launches) != 1 {
			t.Fatalf("root hardened selection spawned %d native processes, want exactly 1: %#v", len(launches), launches)
		}
	}
}
