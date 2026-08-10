package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestExplicitClientPolicyErrorBranches(t *testing.T) {
	invalid := &ProcessIsolation{UID: 0, GID: 0, BaseEnvironment: map[string]string{}}
	root := t.TempDir()
	client := NewClient(nil, Options{
		CLIPath:       "/bin/true",
		Cwd:           root,
		WritableRoot:  root,
		SettingsFile:  filepath.Join(root, "xdg-config", "amp", "settings.json"),
		MCPConfigPath: filepath.Join(root, "mcp.json"),
		Env: map[string]string{
			envHome:          filepath.Join(root, "home"),
			envXDGConfigHome: filepath.Join(root, "xdg-config"),
			envXDGCacheHome:  filepath.Join(root, "xdg-cache"),
			dataHomeEnv:      filepath.Join(root, "xdg-data"),
			envXDGStateHome:  filepath.Join(root, "xdg-state"),
		},
		Isolation: invalid,
	})

	if _, err := client.StartAuthLogin(t.Context()); err == nil {
		t.Fatal("invalid explicit login environment succeeded")
	}
	if err := client.validateProbeResidence(); err == nil {
		t.Fatal("invalid explicit probe environment succeeded")
	}
	if err := client.DiscoveryProbe(t.Context()); err == nil {
		t.Fatal("invalid explicit discovery probe succeeded")
	}
	if _, _, err := client.discoverVersion(t.Context()); err == nil {
		t.Fatal("invalid explicit version environment succeeded")
	}
	if _, err := client.startTurn(t.Context(), nil, nil); err == nil {
		t.Fatal("invalid explicit turn environment succeeded")
	}
	if _, err := client.outputRaw(t.Context(), "version"); err == nil {
		t.Fatal("invalid explicit output environment succeeded")
	}
	if _, err := client.prepareProcessLaunch(t.Context(), exec.Command("/bin/true")); err == nil {
		t.Fatal("invalid explicit launch succeeded")
	}
}

func TestExplicitClientSelectionAndApplyBranches(t *testing.T) {
	truePath, err := exec.LookPath("true")
	if err != nil {
		t.Fatal(err)
	}
	valid := &ProcessIsolation{
		UID: 1, GID: 1,
		BaseEnvironment:      map[string]string{"PATH": filepath.Dir(truePath)},
		TestOnlyNoCredential: true,
	}
	client := NewClient(nil, Options{CLIPath: truePath, Isolation: valid})
	environment, err := client.buildEnvironment(nil, "")
	if err != nil {
		t.Fatal(err)
	}
	got, discoverErr := client.discover(t.Context(), environment, "")
	if discoverErr != nil || got != truePath {
		t.Fatalf("explicit discovery = %q, %v", got, discoverErr)
	}

	originalPrepare := prepareProcessTree
	t.Cleanup(func() { prepareProcessTree = originalPrepare })
	prepareProcessTree = func(cmd *exec.Cmd, _ processLaunchOptions) (*processTreeCommand, error) {
		return &processTreeCommand{cmd: cmd}, nil
	}
	launch, err := client.prepareProcessLaunch(t.Context(), exec.Command(truePath))
	if err != nil {
		t.Fatalf("explicit apply: %v", err)
	}
	_ = launch.close()

	prepareProcessTree = func(*exec.Cmd, processLaunchOptions) (*processTreeCommand, error) {
		return &processTreeCommand{}, nil
	}
	if _, err := client.prepareProcessLaunch(t.Context(), exec.Command(truePath)); err == nil || !strings.Contains(err.Error(), "apply Amp process isolation") {
		t.Fatalf("explicit apply error = %v", err)
	}

	if runtime.GOOS != "linux" {
		client = NewClient(nil, Options{
			CLIPath: truePath, ScratchParent: t.TempDir(), Isolation: valid,
			TestOnlyAuthLoginPlatform: linuxPlatform,
		})
		client.checkAuthLoginCompatibility = func(string) error { return nil }
		if _, err := client.StartAuthLogin(t.Context()); err == nil || !strings.Contains(err.Error(), "browser shim") {
			t.Fatalf("unsupported explicit browser shim handoff = %v", err)
		}
	}
}

func TestOrdinaryLookupAndVersionErrorBranches(t *testing.T) {
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
	if _, _, err := client.discoverVersion(context.Background()); err == nil {
		t.Fatal("failing ordinary version command succeeded")
	}
}
