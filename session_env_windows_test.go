//go:build windows

package ampacp

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

// The Windows environment contract cannot be proven by cross-compilation: the
// key identity, the executable search rules, and the environment block the
// kernel hands a child are all real-host facts. This case is named
// TestPortable* so the Windows runtime lane selects it, and it drives the
// public request surface end to end — request builder, session meta, session
// composition, the native client resolver, and real child processes.

// portableBuild compiles one single-file Windows helper to an exact name.
func portableBuild(t *testing.T, source, out string) {
	t.Helper()

	file := filepath.Join(t.TempDir(), "main.go")
	if err := os.WriteFile(file, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}

	if output, err := exec.Command("go", "build", "-o", out, file).CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", filepath.Base(out), err, output)
	}
}

// portableAmpHarnessSource is the amp stand-in. It appends one correlated
// record per child — the exact argv and the exact environment block it
// received — so a proof can say which command observed which PATH. On a turn it
// also resolves and runs the session's marker command out of the PATH the
// process actually received, which is the only place a session's raw PATH can
// be observed doing work.
func portableAmpHarnessSource(record string) string {
	return `package main

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const recordDir = ` + strconv.Quote(record) + `

func record(name string, value any) {
	file, err := os.OpenFile(filepath.Join(recordDir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(3)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(value); err != nil {
		os.Exit(3)
	}
}

func recordMarker() {
	name := os.Getenv("AMP_CARRIER_MARKER")
	if name == "" {
		return
	}
	facts := map[string]string{"name": name, "path": os.Getenv("PATH"), "bearer": os.Getenv("AMP_API_KEY")}
	resolved, err := exec.LookPath(name)
	if err != nil {
		facts["error"] = err.Error()
		record("marker.jsonl", facts)
		return
	}
	facts["resolved"] = resolved
	out, runErr := exec.Command(resolved).Output()
	if runErr != nil {
		facts["runError"] = runErr.Error()
	}
	facts["output"] = strings.TrimSpace(string(out))
	record("marker.jsonl", facts)
}

func main() {
	args := os.Args[1:]
	record("child.jsonl", map[string]any{"args": args, "env": os.Environ()})

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

	joined := strings.Join(args, " ")
	switch {
	case strings.Contains(joined, "threads list"):
		os.Stdout.WriteString("[]\n")
	case strings.Contains(joined, "-x"):
		recordMarker()
		_, _ = io.ReadAll(os.Stdin)
		os.Stdout.WriteString("{\"type\":\"system\",\"subtype\":\"init\",\"cwd\":\"/tmp/project\",\"session_id\":\"T-agent-thread\"}\n")
		os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"success\",\"duration_ms\":1,\"is_error\":false,\"num_turns\":1,\"result\":\"done\",\"session_id\":\"T-agent-thread\"}\n")
	default:
		os.Stderr.WriteString("unsupported args: " + joined + "\n")
		os.Exit(2)
	}
}
`
}

// portableShadowSource is the amp.exe planted on the session PATH. It is a real
// executable that records its own launch and then fails, so "the session
// directory never selected the harness" is an observation rather than an
// inference from a passing turn.
func portableShadowSource(log string) string {
	return `package main

import "os"

func main() {
	file, err := os.OpenFile(` + strconv.Quote(log) + `, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err == nil {
		_, _ = file.WriteString(os.Args[0] + "\n")
		_ = file.Close()
	}
	os.Stderr.WriteString("session PATH shadowed the amp harness\n")
	os.Exit(9)
}
`
}

// portableMarkerSource prints the path it was resolved to, so a recorded run
// names the exact file the native process found on the PATH it received.
const portableMarkerSource = `package main

import (
	"fmt"
	"os"
)

func main() { fmt.Println(os.Args[0]) }
`

// TestPortableWindowsSessionEnvironmentPrecedence proves the whole Windows
// environment chain on a real Windows host, argv correlated with environment:
// the agent base, the session overrides that name the same variables under a
// different case, and the adapter-managed residence collapse to one variable
// each; `amp version` and the startup method probes observe the static agent
// PATH; the prompt observes the session PATH and nothing else does; the amp.exe
// planted on the session PATH never runs; and the marker command in that same
// directory is resolved and executed by the prompt child.
func TestPortableWindowsSessionEnvironmentPrecedence(t *testing.T) {
	harnessDir := t.TempDir()
	sessionDir := t.TempDir()
	callerHome := t.TempDir()
	scratch := t.TempDir()
	record := t.TempDir()

	portableBuild(t, portableAmpHarnessSource(record), filepath.Join(harnessDir, "amp.exe"))
	portableBuild(t, portableShadowSource(filepath.Join(record, shadowLogName)), filepath.Join(sessionDir, "amp.exe"))
	portableBuild(t, portableMarkerSource, filepath.Join(sessionDir, "amp-marker.exe"))

	agent := NewAgent(
		WithScratchDir(scratch),
		WithEnv(map[string]string{
			"Path":        harnessDir,
			"home":        callerHome,
			"AMP_API_KEY": "agent-key",
		}),
	)
	t.Cleanup(func() { _ = agent.Close() })

	ctx := context.Background()
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionAmpOptions(NewAmpOptions(
		WithAmpEnv(map[string]string{
			"PATH":               sessionDir,
			"Amp_Api_Key":        "session-key",
			"AMP_CARRIER_MARKER": "amp-marker.exe",
		}),
	))))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	if _, err := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x")); err != nil {
		t.Fatalf("Prompt: %v", err)
	}

	session, err := agent.session(resp.SessionId)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	runs := childRuns(t, record)
	requireProbeAndPromptPaths(t, runs, harnessDir, sessionDir)

	for _, run := range runs {
		// One variable per Windows identity, later phase winning: the folded
		// session credential is the named operation value every child receives.
		requireChildEnv(t, run.Env, "AMP_API_KEY", "session-key")

		if run.isPrompt() {
			// The residence phase is applied after the caller phase, so the
			// isolated home stands even though the agent named HOME under a
			// spelling only Windows folds together.
			requireChildEnv(t, run.Env, "HOME", filepath.Join(session.settingsDir, "home"))
			requireChildEnv(t, run.Env, "AMP_CARRIER_MARKER", "amp-marker.exe")

			continue
		}

		homes := childEnvValues(run.Env, "HOME")
		if len(homes) != 1 || filepath.Dir(filepath.Dir(homes[0])) != scratch {
			t.Fatalf("probe child %v received HOME %#v, want one residence under %q", run.Args, homes, scratch)
		}

		if values := childEnvValues(run.Env, "AMP_CARRIER_MARKER"); len(values) != 0 {
			t.Fatalf("probe child %v received session-only values %#v", run.Args, values)
		}
	}

	requireNoShadowedHarness(t, record)
	requireCarrierRuns(t, carrierRuns(t, record), 0, map[string]carrier{
		"session-key": {bearer: "session-key", dir: sessionDir, marker: "amp-marker.exe"},
	})
}
