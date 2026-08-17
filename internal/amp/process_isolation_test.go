package amp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// requireRefusedEnvironmentPhases pins that a key which cannot name an
// environment variable is refused on every composition phase, not only on the
// base one.
func requireRefusedEnvironmentPhases(t *testing.T, cases ...[]map[string]string) {
	t.Helper()

	for _, phases := range cases {
		if _, err := buildEnvironment("", phases...); err == nil {
			t.Fatalf("invalid environment key accepted in %#v", phases)
		}
	}
}

func TestProcessIsolationEnvironmentIdentityAndLookup(t *testing.T) {
	t.Setenv("AMBIENT_ISOLATION_CANARY", "must-not-leak")
	dir := t.TempDir()
	executable := filepath.Join(dir, "amp")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	isolation := &ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{"PATH": dir, "BASE": "one"},
		StandaloneOwnerID: "process-isolation-test", StandaloneStateRoot: "/var/lib/acp-go-amp-test",
	}
	environment, err := BuildEnvWithIsolation(isolation, map[string]string{"BASE": "two", "EXPLICIT": "yes"}, "/work")
	if err != nil {
		t.Fatal(err)
	}
	env := environmentMap(environment)
	if env["BASE"] != "two" || env["EXPLICIT"] != "yes" || env["PWD"] != "/work" {
		t.Fatalf("environment = %#v", env)
	}
	if _, ok := env["AMBIENT_ISOLATION_CANARY"]; ok {
		t.Fatalf("ambient canary leaked: %#v", env)
	}
	resolved, err := Discover(t.Context(), "amp", environment)
	if err != nil || resolved != executable {
		t.Fatalf("policy lookup = %q, %v", resolved, err)
	}
	if resolvedDefault, defaultErr := Discover(t.Context(), "", environment); defaultErr != nil || resolvedDefault != executable {
		t.Fatalf("default policy lookup = %q, %v", resolvedDefault, defaultErr)
	}
	if _, lookupErr := Discover(t.Context(), "relative/amp", environment); lookupErr == nil {
		t.Fatal("relative executable accepted")
	}
	if _, buildErr := BuildEnvWithIsolation(&ProcessIsolation{UID: 1, GID: 1, BaseEnvironment: map[string]string{"BAD=KEY": "x"}}, nil, ""); buildErr == nil {
		t.Fatal("invalid base key accepted")
	}
	requireRefusedEnvironmentPhases(t,
		[]map[string]string{{"BAD=KEY": "x"}},
		[]map[string]string{nil, {"": "x"}},
	)
	for _, invalid := range []*ProcessIsolation{
		nil,
		{UID: 1, GID: 1},
		{UID: 0, GID: 1},
		{UID: 1, GID: 0},
		{UID: 1, GID: 1, BaseEnvironment: map[string]string{envIsolationUID: "1"}},
		{UID: 1, GID: 1, BaseEnvironment: map[string]string{"GOTRACEBACK": "crash"}},
	} {
		if validationErr := validateProcessIsolation(invalid); validationErr == nil {
			t.Fatalf("invalid isolation accepted: %#v", invalid)
		}
	}
	if got := environmentValue([]string{"invalid", "A=B"}, "A"); got != "B" {
		t.Fatalf("environment value = %q", got)
	}
	if got := environmentValue([]string{"A=B"}, "missing"); got != "" {
		t.Fatalf("missing environment value = %q", got)
	}
	for _, lookup := range []struct {
		file string
		env  []string
	}{
		{"", environment},
		{"amp", nil},
		{"amp", []string{"PATH=relative"}},
		{"missing", environment},
		{dir, environment},
	} {
		if _, lookupErr := lookPathInEnvironment(lookup.file, lookup.env); lookupErr == nil {
			t.Fatalf("invalid lookup accepted: %#v", lookup)
		}
	}
	nonExecutable := filepath.Join(dir, "non-executable")
	if writeErr := os.WriteFile(nonExecutable, []byte("x"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, executableErr := executableFile(nonExecutable); executableErr == nil {
		t.Fatal("non-executable accepted")
	}
	if _, supervisorErr := supervisorEnvironment(nil, nil, "mode"); supervisorErr == nil {
		t.Fatal("nil supervisor isolation accepted")
	}
	supervisorEnv, err := supervisorEnvironment(
		[]string{"A=B", adapterSupervisorModeEnv + "=old", envIsolationUID + "=old", envIsolationGID + "=old", envIsolationTest + "=old"},
		&ProcessIsolation{UID: 1, GID: 2, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}, "mode",
	)
	if err != nil {
		t.Fatal(err)
	}
	if values := environmentMap(supervisorEnv); values["A"] != "B" || values[adapterSupervisorModeEnv] != "mode" || values[envIsolationTest] != envValueTrue {
		t.Fatalf("supervisor environment = %#v", values)
	}
	if _, err := Discover(t.Context(), "amp"); err == nil {
		t.Fatal("discovery without a complete environment succeeded")
	}
	if _, err := Discover(t.Context(), "amp", []string{"PATH="}); err == nil {
		t.Fatal("discovery without policy PATH succeeded")
	}
	cancelledContext := func() context.Context {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		return ctx
	}()
	if _, err := Discover(cancelledContext, "amp", environment); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled discovery = %v", err)
	}
}

func TestOrdinaryEnvironmentAndPATHRemainPortable(t *testing.T) {
	original := ordinaryEnvironmentEntries
	t.Cleanup(func() { ordinaryEnvironmentEntries = original })
	privateKey := adapterPrivateEnvPrefix + "SECRET"
	privateKeyLower := strings.ToLower(privateKey)
	ordinaryEnvironmentEntries = func() []string {
		return []string{
			"MALFORMED",
			"PATH=.",
			"KEEP=value",
			privateKey + "=private",
			privateKeyLower + "=private",
			"GOTRACEBACK=crash",
			"AMP_DISABLE_SECRET_REDACTION=1",
		}
	}

	base := CaptureOrdinaryEnvironment()
	if base["KEEP"] != "value" || base["PATH"] != "." {
		t.Fatalf("ordinary capture = %#v", base)
	}
	for _, key := range []string{privateKey, privateKeyLower, "GOTRACEBACK", "AMP_DISABLE_SECRET_REDACTION"} {
		if _, ok := base[key]; ok {
			t.Fatalf("ordinary capture retained %q", key)
		}
	}

	dir := t.TempDir()
	executable := filepath.Join(dir, "amp")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	client := NewClient(nil, Options{Cwd: dir, OrdinaryEnvironment: base})
	environment, err := client.buildEnvironment(nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := client.discover(t.Context(), environment, dir); err != nil || got != executable {
		t.Fatalf("ordinary relative PATH lookup = %q, %v", got, err)
	}
	client.options.CLIPath = "./amp"
	if got, err := client.discover(t.Context(), environment, dir); err != nil || got != executable {
		t.Fatalf("ordinary relative executable lookup = %q, %v", got, err)
	}
}

// TestWindowsOrdinaryExecutableRulesAcceptGeneratedExecutables exercises the
// Windows rules on every host. Windows regular files have no Unix execute bit,
// PATH keys are case-insensitive, and a bare name is extended only according
// to the supplied PATHEXT value.
func TestWindowsOrdinaryExecutableRulesAcceptGeneratedExecutables(t *testing.T) {
	ignored := t.TempDir()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(ignored, "amp.cmd"), []byte("ignored"), 0o600); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(dir, "amp.exe")
	if err := os.WriteFile(executable, []byte("MZ"), 0o600); err != nil {
		t.Fatal(err)
	}

	environment := []string{"Path=" + ignored, "PathExt=.CMD", "PATH=" + dir, "PATHEXT=EXE;.CMD"}
	rules := windowsOrdinaryExecutableRules(environment)
	if got, err := lookPathInOrdinaryEnvironmentWithRules("amp", environment, dir, rules); err != nil || got != executable {
		t.Fatalf("Windows bare-name lookup = %q, %v", got, err)
	}
	if got, err := lookPathInOrdinaryEnvironmentWithRules(executable, environment, dir, rules); err != nil || got != executable {
		t.Fatalf("Windows qualified lookup = %q, %v", got, err)
	}

	cmdOnly := []string{"Path=" + dir, "PATHEXT=.CMD"}
	if _, err := lookPathInOrdinaryEnvironmentWithRules("amp", cmdOnly, dir, windowsOrdinaryExecutableRules(cmdOnly)); err == nil {
		t.Fatal("Windows lookup ignored the supplied PATHEXT")
	}

	if got := ordinaryWindowsExecutableExtensions(""); len(got) != 4 || got[1] != ".exe" {
		t.Fatalf("Windows default executable extensions = %#v", got)
	}
	if got := ordinaryWindowsExecutableExtensions(";"); len(got) != 0 {
		t.Fatalf("separator-only PATHEXT extensions = %#v", got)
	}
	if got := ordinaryEnvironmentValue(nil, "PATH", true); got != "" {
		t.Fatalf("missing ordinary environment value = %q", got)
	}
	if _, err := ordinaryExecutableFile(dir, unixOrdinaryExecutableRules()); err == nil {
		t.Fatal("ordinary executable lookup accepted a directory")
	}
}

func TestOrdinaryClientRunsWithoutProcessIsolation(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "amp")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nprintf 'ordinary:%s' \"$(id -u)\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	client := NewClient(nil, Options{
		CLIPath: executable,
		Cwd:     dir,
		OrdinaryEnvironment: map[string]string{
			"PATH": os.Getenv("PATH"),
		},
	})
	out, err := client.outputWithArgs(t.Context(), "version")
	if err != nil {
		t.Fatalf("ordinary launch: %v", err)
	}
	if got, want := string(out), fmt.Sprintf("ordinary:%d", os.Geteuid()); got != want {
		t.Fatalf("ordinary identity = %q, want %q", got, want)
	}
	if client.options.Isolation != nil {
		t.Fatalf("ordinary launch fabricated policy %#v", client.options.Isolation)
	}

	launch, err := client.prepareProcessLaunch(t.Context(), exec.Command(executable))
	if err != nil {
		t.Fatalf("prepare ordinary launch: %v", err)
	}
	defer launch.close()
	if launch.nativeIsolation || launch.control != nil || len(launch.inherited) != 0 {
		t.Fatalf("ordinary launch acquired isolation authority: %#v", launch)
	}
}

// TestOrdinaryLookupErrorBranches pins ordinary executable resolution: an
// empty name is refused, an unreadable working directory is reported, and an
// empty PATH component resolves against the process working directory rather
// than aborting the lookup.
func TestOrdinaryLookupErrorBranches(t *testing.T) {
	if _, err := lookPathInOrdinaryEnvironment("", nil, ""); err == nil {
		t.Fatal("empty ordinary executable succeeded")
	}

	originalGetwd := ordinaryEnvironmentGetwd
	ordinaryEnvironmentGetwd = func() (string, error) { return "", errors.New("getwd") }
	t.Cleanup(func() { ordinaryEnvironmentGetwd = originalGetwd })
	if _, err := lookPathInOrdinaryEnvironment("amp", nil, ""); err == nil || !strings.Contains(err.Error(), "get working directory") {
		t.Fatalf("ordinary getwd error = %v", err)
	}
	ordinaryEnvironmentGetwd = originalGetwd

	dir := t.TempDir()
	executable := filepath.Join(dir, "amp")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	client := NewClient(nil, Options{CLIPath: executable, Cwd: dir, OrdinaryEnvironment: map[string]string{"PATH": string(os.PathListSeparator) + "/bin"}})
	environment, err := client.buildEnvironment(nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lookPathInOrdinaryEnvironment("sh", environment, dir); err != nil {
		t.Fatalf("ordinary empty PATH component: %v", err)
	}
}
