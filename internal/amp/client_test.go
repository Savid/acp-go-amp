package amp

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

var testDarwinRuntimeID atomic.Uint64

func newTestClient(t *testing.T, logger *slog.Logger, options Options) *Client {
	t.Helper()
	if options.Isolation != nil {
		options.Isolation.TestOnlyIdentityLockRoot = t.TempDir()
	}
	if options.TestOnlyAuthLoginPlatform == "" {
		options.TestOnlyAuthLoginPlatform = "linux"
	}
	if runtime.GOOS == "darwin" {
		options.DarwinBestEffort = true
		options.NewDarwinGeneration = func(_ context.Context) (*DarwinGeneration, error) {
			return &DarwinGeneration{
				RuntimeID:   fmt.Sprintf("%032x", testDarwinRuntimeID.Add(1)),
				ScratchRoot: t.TempDir(),
			}, nil
		}
	}

	client := NewClient(logger, options)
	client.checkAuthLoginCompatibility = func(string) error { return nil }

	return client
}

func newTestProbeClient(t *testing.T, logger *slog.Logger, options Options) *Client {
	t.Helper()
	root := t.TempDir()
	settingsFile := filepath.Join(root, "xdg-config", "amp", "settings.json")
	for _, path := range []string{
		filepath.Join(root, "home"),
		filepath.Join(root, "xdg-config"),
		filepath.Join(root, "xdg-cache"),
		filepath.Join(root, "xdg-data"),
		filepath.Join(root, "xdg-state"),
		filepath.Dir(settingsFile),
	} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatalf("create probe residence: %v", err)
		}
	}
	if err := os.WriteFile(settingsFile, AuthSettingsDocument(), 0o600); err != nil {
		t.Fatalf("write probe settings: %v", err)
	}
	mcpFile := filepath.Join(root, "mcp.json")
	if err := os.WriteFile(mcpFile, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write probe MCP config: %v", err)
	}

	env := make(map[string]string, len(options.Env)+5)
	for key, value := range options.Env {
		env[key] = value
	}
	env[envHome] = filepath.Join(root, "home")
	env[envXDGConfigHome] = filepath.Join(root, "xdg-config")
	env[envXDGCacheHome] = filepath.Join(root, "xdg-cache")
	env[dataHomeEnv] = filepath.Join(root, "xdg-data")
	env[envXDGStateHome] = filepath.Join(root, "xdg-state")
	options.Env = env
	options.SettingsFile = settingsFile
	options.MCPConfigPath = mcpFile
	options.WritableRoot = root

	return newTestClient(t, logger, options)
}

func testProcessIsolation() *ProcessIsolation {
	uid, gid := os.Geteuid(), os.Getegid()
	if uid == 0 || gid == 0 {
		uid, gid = 65534, 65534
	}

	return &ProcessIsolation{
		UID: uint32(uid), GID: uint32(gid),
		BaseEnvironment:      map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")},
		TestOnlyNoCredential: true,
	}
}

func TestNewClientSelectsTheRequestedAuthCompatibilityPolicy(t *testing.T) {
	linuxClient := NewClient(nil, Options{TestOnlyAuthLoginPlatform: linuxPlatform})
	if err := linuxClient.checkAuthLoginCompatibility("ignored"); err != nil {
		t.Fatalf("Linux test compatibility = %v", err)
	}

	otherClient := NewClient(nil, Options{TestOnlyAuthLoginPlatform: "unsupported"})
	if err := otherClient.checkAuthLoginCompatibility("ignored"); err != nil {
		t.Fatalf("unsupported-platform compatibility = %v", err)
	}
}

func TestProbeResidenceValidation(t *testing.T) {
	if err := NewClient(nil, Options{}).validateProbeResidence(); err == nil {
		t.Fatal("empty probe residence accepted")
	}

	mismatch := newTestProbeClient(t, nil, Options{})
	mismatch.options.Env[envHome] = t.TempDir()
	if err := mismatch.validateProbeResidence(); err == nil {
		t.Fatal("probe residence accepted mismatched HOME")
	}

	valid := newTestProbeClient(t, nil, Options{})
	if err := valid.validateProbeResidence(); err != nil {
		t.Fatalf("valid probe residence rejected: %v", err)
	}
}

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

// TestOrdinaryVersionCommandFailure pins the ordinary version discovery
// failure: a harness that exits nonzero is reported, not silently accepted.
func TestOrdinaryVersionCommandFailure(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "amp")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	client := NewClient(nil, Options{CLIPath: executable, Cwd: dir, OrdinaryEnvironment: map[string]string{"PATH": string(os.PathListSeparator) + "/bin"}})
	if _, _, err := client.discoverVersion(context.Background()); err == nil {
		t.Fatal("failing ordinary version command succeeded")
	}
}
