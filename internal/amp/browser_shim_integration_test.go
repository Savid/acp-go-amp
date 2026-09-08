//go:build integration && browsercanary && !windows

package amp

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// installedAmpEnv is the private carrier the pinned-binary fixture uses to hand
// this probe the path of the real Amp it must drive. Only the Linux container
// fixture plants that binary and sets the carrier, so the name says so; an
// ordinary run anywhere else leaves it unset and the probe skips.
const installedAmpEnv = "ACP_GO_AMP_TEST_INSTALLED_LINUX_AMP"

// TestInstalledAmpLoginExecsOnlyShimLauncher is the real-native proof for
// brokered login: the installed Amp binary's account login must exec only the
// PATH-shadowed launcher its platform's audited branch names. The Linux
// integration caller runs it in a networkless, no-GUI container; on Darwin it
// runs natively against the supplied binary. AMP_URL points at unreachable
// loopback, so no provider authorization can be initiated.
func TestInstalledAmpLoginExecsOnlyShimLauncher(t *testing.T) {
	if os.Getenv("ACP_GO_AMP_RUN_INTEGRATION") != "1" {
		t.Skip("set ACP_GO_AMP_RUN_INTEGRATION=1 for the installed-native probe")
	}
	path := os.Getenv(installedAmpEnv)
	if path == "" {
		t.Fatalf("native browser probe requires %s to name a real installed Amp binary", installedAmpEnv)
	}

	launcher := "xdg-open"
	if runtime.GOOS == darwinPlatform {
		launcher = "open"
	}

	originalScript := browserShimScript
	browserShimScript = []byte("#!/bin/sh\nprintf '%s\\n' \"${0##*/}\" > \"$ACP_GO_AMP_TEST_BROWSER_MARKER\"\nexit 0\n")
	t.Cleanup(func() { browserShimScript = originalScript })
	marker := filepath.Join(t.TempDir(), "launcher")
	home := t.TempDir()
	settingsFile := filepath.Join(t.TempDir(), "settings.json")
	if writeErr := os.WriteFile(settingsFile, AuthSettingsDocument(), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	client := newTestClient(t, nil, Options{
		CLIPath:      path,
		Cwd:          t.TempDir(),
		SettingsFile: settingsFile,
		Env: map[string]string{
			AuthDeploymentEnv:                "http://127.0.0.1:1",
			"ACP_GO_AMP_TEST_BROWSER_MARKER": marker,
			envHome:                          home,
			envXDGCacheHome:                  filepath.Join(home, "cache"),
			envXDGConfigHome:                 filepath.Join(home, "config"),
			dataHomeEnv:                      t.TempDir(),
			envXDGStateHome:                  filepath.Join(home, "state"),
		},
	})

	login, err := client.StartAuthLogin(t.Context())
	if err != nil {
		t.Fatalf("start installed Amp login: %v", err)
	}
	t.Cleanup(func() { _ = login.Close() })

	deadline := time.Now().Add(20 * time.Second)
	for {
		launched, readErr := os.ReadFile(marker)
		if readErr == nil {
			if got := strings.TrimSpace(string(launched)); got != launcher {
				t.Fatalf("installed Amp executed shim %q, want %s", got, launcher)
			}

			break
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			t.Fatalf("read installed Amp browser marker: %v", readErr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("installed Amp never executed the PATH-shadowed %s shim", launcher)
		}

		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	if closeErr := login.Close(); closeErr != nil {
		t.Fatalf("close installed Amp login: %v", closeErr)
	}
}
