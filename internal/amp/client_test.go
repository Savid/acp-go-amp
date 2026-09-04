package amp

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// testExecutableName spells a harness file name the way the host resolves
// executables. Windows honours PATHEXT, so a name with no extension is not an
// executable there however it is written to disk.
func testExecutableName(base string) string {
	if runtime.GOOS == "windows" {
		return base + ".exe"
	}

	return base
}

// absTestPath builds a host-absolute path from POSIX-looking segments, so a
// test states "an absolute path" rather than a spelling only one platform
// accepts.
func absTestPath(segments ...string) string {
	root := "/"
	if runtime.GOOS == "windows" {
		root = `C:\`
	}

	return filepath.Join(append([]string{root}, segments...)...)
}

func newTestClient(t *testing.T, logger *slog.Logger, options Options) *Client {
	t.Helper()
	if options.TestOnlyAuthLoginPlatform == "" {
		options.TestOnlyAuthLoginPlatform = "linux"
	}
	if options.BrowserShim == "" {
		shim, err := MaterializeBrowserShim(filepath.Join(t.TempDir(), "browser"))
		if err != nil {
			t.Fatalf("materialize browser shim: %v", err)
		}
		options.BrowserShim = shim
	}

	client := NewClient(logger, options)
	client.checkAuthLoginSafety = func(string) error { return nil }

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

func TestNewClientSelectsTheRequestedAuthSafetyPolicy(t *testing.T) {
	linuxClient := NewClient(nil, Options{TestOnlyAuthLoginPlatform: linuxPlatform})
	if err := linuxClient.checkAuthLoginSafety("ignored"); err != nil {
		t.Fatalf("Linux test safety policy = %v", err)
	}

	otherClient := NewClient(nil, Options{TestOnlyAuthLoginPlatform: "unsupported"})
	if err := otherClient.checkAuthLoginSafety("ignored"); err != nil {
		t.Fatalf("unsupported-platform safety policy = %v", err)
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
