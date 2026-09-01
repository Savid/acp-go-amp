//go:build !windows

package amp

import (
	"errors"
	"os"
	"os/exec"
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

// browserProbeDir plants an executable for every name a launcher might exec,
// each of which records the invocation instead of opening anything. A recorded
// line is proof that something ran a browser launcher off PATH.
func browserProbeDir(t *testing.T, marker string) string {
	t.Helper()

	dir := t.TempDir()
	script := []byte("#!/bin/sh\necho \"$0 $*\" >> " + shellQuote(marker) + "\nexit 0\n")

	for _, name := range browserLauncherNames {
		if err := os.WriteFile(filepath.Join(dir, name), script, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

func TestMaterializeBrowserShimReportsFilesystemFailures(t *testing.T) {
	parentFile := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parentFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := MaterializeBrowserShim(filepath.Join(parentFile, "browser")); err == nil {
		t.Fatal("shim materialization ignored directory creation failure")
	}

	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, browserLauncherNames[0]), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := MaterializeBrowserShim(dir); err == nil {
		t.Fatal("shim materialization ignored launcher write failure")
	}
}

// TestLoginNeverExecsABrowserLauncher runs the adapter's login path with a
// deterministic harness and a recording launcher ahead of every other PATH
// entry. It proves the PATH interception used on Darwin and Linux;
// installed-binary safety is checked separately before this boundary
// is built.
func TestLoginNeverExecsABrowserLauncher(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "launched")
	probe := browserProbeDir(t, marker)

	t.Setenv("PATH", probe+string(os.PathListSeparator)+os.Getenv("PATH"))

	for _, name := range browserLauncherNames {
		resolved, lookErr := exec.LookPath(name)
		if lookErr != nil || resolved != filepath.Join(probe, name) {
			t.Fatalf("%s resolved to %q, %v; want the probe launcher", name, resolved, lookErr)
		}

		if runErr := exec.Command(resolved, "probe://control").Run(); runErr != nil {
			t.Fatalf("probe launcher %s control run: %v", name, runErr)
		}
	}

	control, err := os.ReadFile(marker)
	if err != nil || strings.Count(string(control), "\n") != len(browserLauncherNames) {
		t.Fatalf("the probe launchers recorded %q, %v; want one line per launcher", control, err)
	}

	if removeErr := os.Remove(marker); removeErr != nil {
		t.Fatal(removeErr)
	}

	dir := t.TempDir()
	resolvedFile := filepath.Join(dir, "resolved")
	path := filepath.Join(dir, "amp")
	// Every launcher name is exercised as a bare command so the harness resolves
	// it through the PATH it was handed.
	harness := "#!/bin/sh\n"

	for _, name := range browserLauncherNames {
		harness += "command -v " + name + " >> " + shellQuote(resolvedFile) + "\n" +
			name + " \"https://example.invalid/\"\n"
	}

	harness += "echo " + shellQuote(helperLoginURL) + "\nexit 0\n"

	if writeErr := os.WriteFile(path, []byte(harness), 0o700); writeErr != nil {
		t.Fatal(writeErr)
	}

	client := newTestClient(t, nil, Options{
		CLIPath: path,
		Cwd:     t.TempDir(),
		Env:     map[string]string{dataHomeEnv: t.TempDir()},
	})

	login, err := client.StartAuthLogin(t.Context())
	if err != nil {
		t.Fatalf("StartAuthLogin: %v", err)
	}

	t.Cleanup(func() { _ = login.Close() })

	if url, urlErr := login.URL(t.Context()); urlErr != nil || url != helperLoginURL {
		t.Fatalf("URL = %q, %v", url, urlErr)
	}

	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		recorded, _ := os.ReadFile(marker)
		t.Fatalf("the login child executed a browser launcher: %s", recorded)
	}

	// The recorder staying silent only means something if the child could reach
	// it, so every name it did resolve has to be the shim standing in front of it.
	shimmed, err := os.ReadFile(resolvedFile)
	if err != nil {
		t.Fatal(err)
	}

	resolved := strings.Fields(string(shimmed))
	if len(resolved) != len(browserLauncherNames) {
		t.Fatalf("the child resolved %q; want one path per launcher", shimmed)
	}

	for i, launcher := range resolved {
		if filepath.Dir(launcher) != client.options.BrowserShim || filepath.Base(launcher) != browserLauncherNames[i] {
			t.Fatalf("the child resolved %s to %q; want a shim launcher", browserLauncherNames[i], launcher)
		}
	}
}

// TestInstalledAmpLoginExecsOnlyShimLauncher is the real-native proof for
// brokered login: the installed Amp binary's account login must exec only the
// PATH-shadowed launcher its platform's audited branch names. The Linux
// integration caller runs it in a networkless, no-GUI container; on Darwin it
// runs natively against the supplied binary. AMP_URL points at unreachable
// loopback, so no provider authorization can be initiated.
func TestInstalledAmpLoginExecsOnlyShimLauncher(t *testing.T) {
	path := os.Getenv(installedAmpEnv)
	if path == "" {
		t.Skipf("set %s to a real installed Amp binary", installedAmpEnv)
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
