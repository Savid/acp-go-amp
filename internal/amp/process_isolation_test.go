package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestProcessIsolationEnvironmentIdentityAndLookup(t *testing.T) {
	t.Setenv("AMBIENT_ISOLATION_CANARY", "must-not-leak")
	dir := t.TempDir()
	executable := filepath.Join(dir, "amp")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	isolation := &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{"PATH": dir, "BASE": "one"}}
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
	if _, buildErr := buildEnvironment(map[string]string{"BAD=KEY": "x"}, nil, ""); buildErr == nil {
		t.Fatal("low-level invalid base key accepted")
	}
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

func TestClientFailsClosedWithoutProcessIsolation(t *testing.T) {
	client := NewClient(nil, Options{})
	if client.AuthDeploymentSupported() {
		t.Fatal("auth deployment accepted without isolation")
	}
	if _, err := client.StartAuthLogin(t.Context()); err == nil {
		t.Fatal("auth login started without isolation")
	}
	if _, err := authLoginEnv(nil, nil, ""); err == nil {
		t.Fatal("auth environment built without isolation")
	}
	if err := client.DiscoveryProbe(t.Context()); err == nil {
		t.Fatal("discovery succeeded without isolation")
	}
	if _, err := client.startTurn(t.Context(), nil, nil); err == nil {
		t.Fatal("turn started without isolation")
	}
	if _, err := client.outputWithArgs(t.Context(), "--version"); err == nil {
		t.Fatal("command started without isolation")
	}
	if _, err := client.prepareProcessLaunch(t.Context(), exec.Command("/usr/bin/true")); err == nil {
		t.Fatal("launch prepared without isolation")
	}
}

func TestRepeatedEnvironmentBuildFailsClosed(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "amp")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	policy := &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{"PATH": dir}}

	originalCommand := commandContext
	t.Cleanup(func() { commandContext = originalCommand })

	client := NewClient(nil, Options{CLIPath: executable, Isolation: policy})
	commandContext = func(context.Context, string, ...string) *exec.Cmd {
		client.options.Isolation = nil

		return exec.Command("/usr/bin/true")
	}
	if _, err := client.startTurn(t.Context(), nil, nil); err == nil {
		t.Fatal("turn accepted isolation removed during construction")
	}

	client = NewClient(nil, Options{CLIPath: executable, Isolation: policy})
	if _, err := client.outputWithArgs(t.Context(), "--version"); err == nil {
		t.Fatal("command accepted isolation removed during construction")
	}

	failing := filepath.Join(dir, "failing-amp")
	if err := os.WriteFile(failing, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	commandContext = originalCommand
	client = newTestClient(t, nil, Options{CLIPath: failing})
	if _, _, err := client.discoverVersion(t.Context()); err == nil {
		t.Fatal("failed version probe accepted")
	}
}
