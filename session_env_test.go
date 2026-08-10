package ampacp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/savid/acp-go-amp/internal/amp"
)

// simulateWindowsEnvironment selects the Windows environment key identity for
// the duration of a test so the composition contract can be exercised on any
// host, not only on a Windows runner.
func simulateWindowsEnvironment(t *testing.T) {
	t.Helper()

	original := runtimeGOOS
	t.Cleanup(func() { runtimeGOOS = original })

	runtimeGOOS = platformWindows
}

// childEnvironments reads every environment block the fake amp recorded, one
// per spawned process.
func childEnvironments(t *testing.T, state string) [][]string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(state, "env.jsonl"))
	if err != nil {
		t.Fatalf("read recorded child environments: %v", err)
	}

	blocks := make([][]string, 0, 4)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var entries []string
		if err := json.Unmarshal([]byte(line), &entries); err != nil {
			t.Fatalf("decode recorded child environment %q: %v", line, err)
		}

		blocks = append(blocks, entries)
	}

	return blocks
}

// startupProbeThreadID is the deliberately non-existent thread id the native
// method-present probes use. It is what makes a probe child recognizable in a
// recorded argv.
const startupProbeThreadID = "T-00000000-0000-0000-0000-000000000000"

// childRun is one recorded amp child: the exact argv it was launched with and
// the exact environment block it received. Correlating the two is the only way
// to state which command observed which PATH.
type childRun struct {
	Args []string `json:"args"`
	Env  []string `json:"env"`
}

// isPrompt reports the one child class that carries the session: a stream-json
// execute or continue turn. The startup continue probe names the missing probe
// thread, so it is a probe rather than a turn.
func (r childRun) isPrompt() bool {
	return slices.Contains(r.Args, "-x") && !slices.Contains(r.Args, startupProbeThreadID)
}

// isVersionProbe reports the bare `amp version` child that selects and
// validates the harness.
func (r childRun) isVersionProbe() bool {
	return len(r.Args) > 0 && r.Args[0] == "version"
}

// childRuns reads every correlated child record the fake amp appended.
func childRuns(t *testing.T, state string) []childRun {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(state, "child.jsonl"))
	if err != nil {
		t.Fatalf("read recorded child runs: %v", err)
	}

	runs := make([]childRun, 0, 8)

	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		var run childRun
		if err := json.Unmarshal([]byte(line), &run); err != nil {
			t.Fatalf("decode recorded child run %q: %v", line, err)
		}

		runs = append(runs, run)
	}

	return runs
}

// requireProbeAndPromptPaths is the hard cut stated as one assertion: the
// version probe and every startup method probe observe the static PATH, the
// prompt observes the session PATH, and both classes are actually present.
func requireProbeAndPromptPaths(t *testing.T, runs []childRun, staticPath, sessionPath string) {
	t.Helper()

	versions, probes, prompts := 0, 0, 0

	for _, run := range runs {
		switch {
		case run.isPrompt():
			prompts++

			requireChildEnv(t, run.Env, "PATH", sessionPath)
		case run.isVersionProbe():
			versions++

			requireChildEnv(t, run.Env, "PATH", staticPath)
		default:
			probes++

			requireChildEnv(t, run.Env, "PATH", staticPath)
		}
	}

	if versions == 0 || probes == 0 || prompts == 0 {
		t.Fatalf("recorded %d version probes, %d method probes, %d prompts; want each nonzero", versions, probes, prompts)
	}
}

// childEnvValues returns every value a child received under names that are
// equal to name when case is ignored. A collapsed environment yields exactly
// one value; a duplicated one yields more.
func childEnvValues(entries []string, name string) []string {
	values := make([]string, 0, 1)

	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(key, name) {
			values = append(values, value)
		}
	}

	return values
}

func requireChildEnv(t *testing.T, entries []string, name, want string) {
	t.Helper()

	values := childEnvValues(entries, name)
	if len(values) != 1 || values[0] != want {
		t.Fatalf("child %s = %#v, want exactly [%q]", name, values, want)
	}
}

// TestWindowsSessionEnvironmentComposesOneCaseInsensitiveChain drives the
// public request surface with agent and session values whose spellings differ
// only by case and proves the whole chain — agent base, session overrides, then
// the adapter-managed residence — collapses to one variable per name under the
// Windows key identity, with the later phase winning. It also proves the
// probe/prompt cut by correlating each recorded argv with the environment that
// child actually received: the version and startup probes stay on the static
// agent PATH, and only the prompt carries the session PATH.
func TestWindowsSessionEnvironmentComposesOneCaseInsensitiveChain(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "record-env")

	simulateWindowsEnvironment(t)

	agentPathDir := t.TempDir()
	sessionPathDir := t.TempDir()
	callerHome := t.TempDir()
	scratch := testScratchDir(t)

	agent := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(scratch),
		WithEnv(map[string]string{
			"Path":        agentPathDir,
			"home":        callerHome,
			"AMP_API_KEY": "agent-key",
		}),
	)
	t.Cleanup(func() { _ = agent.Close() })

	ctx := context.Background()
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionAmpOptions(NewAmpOptions(
		WithAmpEnv(map[string]string{
			"PATH":         sessionPathDir,
			"Home":         callerHome,
			"Amp_Api_Key":  "session-key",
			"Session_Only": "session-value",
		}),
	))))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x")); promptErr != nil {
		t.Fatalf("Prompt: %v", promptErr)
	}

	session, err := agent.session(resp.SessionId)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	managedHome := filepath.Join(session.settingsDir, "home")

	runs := childRuns(t, state)
	requireProbeAndPromptPaths(t, runs, agentPathDir, sessionPathDir)

	for _, run := range runs {
		// The credential is a named operation value, so admission and every
		// child agree on it under the folded spelling.
		requireChildEnv(t, run.Env, "AMP_API_KEY", "session-key")

		// A session value that is not a named operation value is the prompt's
		// alone; a probe never sees one.
		if run.isPrompt() {
			// The residence phase is applied after every caller phase, so the
			// isolated home stands even though both caller phases named HOME
			// under a spelling that only Windows folds together.
			requireChildEnv(t, run.Env, "HOME", managedHome)
			requireChildEnv(t, run.Env, "SESSION_ONLY", "session-value")

			continue
		}

		// A probe runs in its own adapter-managed residence under the scratch
		// parent, never in a caller-named home.
		homes := childEnvValues(run.Env, "HOME")
		if len(homes) != 1 || filepath.Dir(filepath.Dir(homes[0])) != scratch {
			t.Fatalf("probe child %v received HOME %#v, want one residence under %q", run.Args, homes, scratch)
		}

		if values := childEnvValues(run.Env, "SESSION_ONLY"); len(values) != 0 {
			t.Fatalf("probe child %v received session-only values %#v", run.Args, values)
		}
	}
}

// TestWindowsSessionEnvironmentGatesTheFoldedAPIKey proves the credential gate
// reads the key the child would actually receive: under the Windows identity a
// differently spelled session key satisfies the gate, and under the Unix
// identity the same spelling is a different variable that does not.
func TestWindowsSessionEnvironmentGatesTheFoldedAPIKey(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "record-env")

	t.Setenv("AMP_API_KEY", "")

	request := NewSessionRequest(t.TempDir(), WithSessionAmpOptions(NewAmpOptions(
		WithAmpEnv(map[string]string{"Amp_Api_Key": "session-key"}),
	)))

	unixAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	t.Cleanup(func() { _ = unixAgent.Close() })

	if _, err := unixAgent.NewSession(context.Background(), request); err == nil {
		t.Fatal("a differently spelled key satisfied the gate under the Unix key identity")
	} else if !strings.Contains(err.Error(), "AMP_API_KEY is not set") {
		t.Fatalf("NewSession error = %v, want the missing-key refusal", err)
	}

	simulateWindowsEnvironment(t)

	windowsAgent := NewAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	t.Cleanup(func() { _ = windowsAgent.Close() })

	if _, err := windowsAgent.NewSession(context.Background(), request); err != nil {
		t.Fatalf("folded session key refused under the Windows key identity: %v", err)
	}
}

// TestAmbiguousEnvironmentKeysAreRefused pins the family rule: one public map
// may not name the same platform variable twice, because a Go map carries no
// order from which the delivered value could be derived.
func TestAmbiguousEnvironmentKeysAreRefused(t *testing.T) {
	ambiguous := map[string]string{"Path": "a", "PATH": "b"}

	if previous, key := ambiguousEnvKeys(ambiguous); previous != "" || key != "" {
		t.Fatalf("Unix identity reported ambiguity: %q, %q", previous, key)
	}

	simulateWindowsEnvironment(t)

	if previous, key := ambiguousEnvKeys(ambiguous); previous != "PATH" || key != "Path" {
		t.Fatalf("ambiguous keys = %q, %q; want \"PATH\", \"Path\"", previous, key)
	}

	agent := NewAgent(WithEnv(ambiguous))
	t.Cleanup(func() { _ = agent.Close() })

	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "name the same variable") {
		t.Fatalf("agent env ambiguity = %v, want a refusal", err)
	}

	sessionAgent := NewAgent(WithScratchDir(testScratchDir(t)))
	t.Cleanup(func() { _ = sessionAgent.Close() })

	_, err = sessionAgent.NewSession(context.Background(), NewSessionRequest(t.TempDir(), WithSessionAmpOptions(
		NewAmpOptions(WithAmpEnv(ambiguous)),
	)))
	requireInvalidParamsData(t, err, map[string]any{
		jsonFieldError: valAmbiguous,
		jsonFieldField: "_meta.amp.options.env.Path",
	})
}

// TestInvalidEnvironmentNamesAreRefused pins that a key which cannot be an
// environment variable name at all is named by the public surface instead of
// being dropped on the way to the child.
func TestInvalidEnvironmentNamesAreRefused(t *testing.T) {
	for _, key := range []string{"", "A=B", "A\x00B"} {
		agent := NewAgent(WithEnv(map[string]string{key: "x"}))
		t.Cleanup(func() { _ = agent.Close() })

		_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
		if err == nil || !strings.Contains(err.Error(), "is not a valid variable name") {
			t.Fatalf("agent env key %q = %v, want a refusal", key, err)
		}

		sessionAgent := NewAgent(WithScratchDir(testScratchDir(t)))
		t.Cleanup(func() { _ = sessionAgent.Close() })

		_, err = sessionAgent.NewSession(context.Background(), NewSessionRequest(t.TempDir(), WithSessionAmpOptions(
			NewAmpOptions(WithAmpEnv(map[string]string{key: "x"})),
		)))
		requireInvalidParamsData(t, err, map[string]any{jsonFieldField: "_meta.amp.options.env." + key})
	}
}

// TestOperationSessionEnvNamesOnlyTheOperationValues pins the phase a
// non-prompt child receives from a session: the credential and the deployment
// URL, nothing else, and under the platform key identity.
func TestOperationSessionEnvNamesOnlyTheOperationValues(t *testing.T) {
	session := map[string]string{
		"AMP_API_KEY":  "key",
		"AMP_URL":      "https://amp.example",
		"PATH":         "/session/bin",
		"Amp_Api_Key":  "folded",
		"SESSION_ONLY": "value",
	}

	unix := operationSessionEnv(session)
	if len(unix) != 2 || unix["AMP_API_KEY"] != "key" || unix["AMP_URL"] != "https://amp.example" {
		t.Fatalf("Unix operation values = %#v", unix)
	}

	simulateWindowsEnvironment(t)

	// Under the Windows identity the folded spelling is the same variable. The
	// public surface refuses such a map as ambiguous before it gets here, but
	// the phase still resolves it the way the credential gate does — sorted,
	// last wins — so the two can never disagree.
	windows := operationSessionEnv(session)
	if len(windows) != 2 || windows["AMP_API_KEY"] != "folded" || windows["AMP_URL"] != "https://amp.example" {
		t.Fatalf("Windows operation values = %#v", windows)
	}

	if !amp.HasAPIKey(windows) {
		t.Fatal("the folded credential did not satisfy the gate under the Windows identity")
	}
}

// TestComposeEnvAppliesPhasesInOrder pins the composition primitive itself:
// later phases replace earlier ones under the platform identity, and the
// managed residence phase is what activeRequestEnv removes.
func TestComposeEnvAppliesPhasesInOrder(t *testing.T) {
	simulateWindowsEnvironment(t)

	composed := composeEnv(
		map[string]string{"Path": "base", "KEEP": "base"},
		map[string]string{"PATH": "session"},
		managedSessionEnv("home", "config", "cache", "data", "state"),
	)
	want := map[string]string{
		"PATH":           "session",
		"KEEP":           "base",
		envHome:          "home",
		envXDGConfigHome: "config",
		envXDGCacheHome:  "cache",
		envXDGDataHome:   "data",
		envXDGStateHome:  "state",
	}
	for key, value := range want {
		if composed[key] != value {
			t.Fatalf("composed[%q] = %q, want %q", key, composed[key], value)
		}
	}

	if len(composed) != len(want) {
		t.Fatalf("composed = %#v, want exactly %d entries", composed, len(want))
	}

	active := activeRequestEnv(composed)
	if len(active) != 2 || active["PATH"] != "session" || active["KEEP"] != "base" {
		t.Fatalf("active request env = %#v", active)
	}
}
