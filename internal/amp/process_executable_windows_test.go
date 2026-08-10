//go:build windows

package amp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPortableWindowsEnvironmentPrecedence(t *testing.T) {
	originalEntries := ordinaryEnvironmentEntries
	t.Cleanup(func() { ordinaryEnvironmentEntries = originalEntries })
	ordinaryEnvironmentEntries = func() []string {
		return []string{"Path=base", "PATH=override", "gotraceback=crash"}
	}
	captured := CaptureOrdinaryEnvironment()
	if captured["PATH"] != "override" || len(captured) != 1 {
		t.Fatalf("captured Windows environment = %#v", captured)
	}
	if _, err := BuildEnvWithIsolation(&ProcessIsolation{
		UID: 1, GID: 1, BaseEnvironment: map[string]string{"gotraceback": "crash"},
	}, nil, ""); err == nil || !strings.Contains(err.Error(), "forbidden key") {
		t.Fatalf("mixed-case forbidden isolation environment = %v", err)
	}

	harness, _ := portableHarness(t)
	baseDir := t.TempDir()
	harnessDir := t.TempDir()
	sessionDir := t.TempDir()
	resolved := filepath.Join(harnessDir, "amp.exe")

	data, err := os.ReadFile(harness)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resolved, data, 0o700); err != nil {
		t.Fatal(err)
	}
	// Neither of these may ever be selected: the base spelling is replaced by
	// the agent-scoped resolution phase, and the session PATH is a child
	// carrier rather than executable-selection authority.
	for _, shadow := range []string{filepath.Join(baseDir, "amp.com"), filepath.Join(sessionDir, "amp.exe")} {
		if err := os.WriteFile(shadow, []byte("unused"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	cwd := t.TempDir()
	client := NewClient(nil, Options{
		CLIPath: "amp",
		Cwd:     cwd,
		OrdinaryEnvironment: map[string]string{
			"Path":                            baseDir,
			"PathExt":                         ".COM",
			"pwd":                             "base",
			"gotraceback":                     "crash",
			"amp_disable_secret_redaction":    "1",
			"acp_go_amp_internal_test_secret": "secret",
		},
		ResolutionEnv: map[string]string{"PATH": harnessDir, "PATHEXT": ".EXE"},
		Env: map[string]string{
			"PATH":    sessionDir,
			"PATHEXT": ".EXE",
			"Pwd":     "override",
		},
	})

	environment, err := client.buildEnvironment(client.options.Env, cwd)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"PATH": sessionDir, "PATHEXT": ".EXE", "PWD": cwd} {
		values := windowsEnvironmentValues(environment, key)
		if len(values) != 1 || values[0] != want {
			t.Fatalf("%s values = %#v, want [%q]", key, values, want)
		}
	}
	for _, key := range []string{"GOTRACEBACK", "AMP_DISABLE_SECRET_REDACTION", "ACP_GO_AMP_INTERNAL_TEST_SECRET"} {
		if values := windowsEnvironmentValues(environment, key); len(values) != 0 {
			t.Fatalf("filtered %s values = %#v", key, values)
		}
	}

	executable, err := client.resolveExecutable(t.Context(), cwd)
	if err != nil || executable != resolved {
		t.Fatalf("Windows executable = %q, %v; want %q", executable, err, resolved)
	}
	out, err := client.outputWithArgs(t.Context(), "environment")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(out), sessionDir+"\n.EXE"; got != want {
		t.Fatalf("child environment = %q, want %q", got, want)
	}

	// A harness that already passed validation is launched directly. Pointing
	// resolution at the session directory — where a lookup would find the
	// unusable amp.exe — changes nothing, because no lookup runs.
	retained := NewClient(nil, Options{
		CLIPath:             "amp",
		Cwd:                 cwd,
		OrdinaryEnvironment: map[string]string{"Path": sessionDir, "PathExt": ".EXE"},
		ResolutionEnv:       map[string]string{"PATH": sessionDir, "PATHEXT": ".EXE"},
		Env:                 map[string]string{"PATH": sessionDir, "PATHEXT": ".EXE"},
		ResolvedExecutable:  resolved,
	})
	if executable, err := retained.resolveExecutable(t.Context(), cwd); err != nil || executable != resolved {
		t.Fatalf("retained Windows harness = %q, %v; want %q", executable, err, resolved)
	}
	if out, err := retained.outputWithArgs(t.Context(), "environment"); err != nil || string(out) != sessionDir+"\n.EXE" {
		t.Fatalf("retained child environment = %q, %v", out, err)
	}
}

func windowsEnvironmentValues(environment []string, name string) []string {
	values := make([]string, 0, 1)
	for _, entry := range environment {
		key, value, ok := strings.Cut(entry, "=")
		if ok && strings.EqualFold(key, name) {
			values = append(values, value)
		}
	}

	return values
}
