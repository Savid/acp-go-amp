package amp

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
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
