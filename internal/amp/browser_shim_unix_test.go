//go:build !windows

package amp

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const installedLinuxAmpEnv = "ACP_GO_AMP_TEST_INSTALLED_LINUX_AMP"

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

// TestLoginNeverExecsABrowserLauncher runs the adapter's login path with a
// deterministic harness and a recording launcher ahead of every other PATH
// entry. It proves the PATH interception used on Linux; installed-binary
// compatibility is checked separately before this boundary is built.
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
		// The shim is generated under the scratch parent and handed to the
		// isolated identity, so the parent has to be one that identity can
		// traverse rather than a t.TempDir leaf nested under a 0700 directory.
		ScratchParent: makeInstalledLinuxAmpScratchParent(t),
		Env:           map[string]string{dataHomeEnv: t.TempDir()},
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
		if !strings.Contains(launcher, browserShimPrefix) || filepath.Base(launcher) != browserLauncherNames[i] {
			t.Fatalf("the child resolved %s to %q; want a shim launcher", browserLauncherNames[i], launcher)
		}
	}
}

// TestInstalledLinuxAmpLoginExecsOnlyShimLauncher is the real-native proof for
// Linux brokered login. Its integration caller runs this test in a networkless,
// no-GUI container and supplies the installed Linux Amp binary. AMP_URL points
// at unreachable loopback, so no provider authorization can be initiated.
func TestInstalledLinuxAmpLoginExecsOnlyShimLauncher(t *testing.T) {
	path := os.Getenv(installedLinuxAmpEnv)
	if path == "" {
		t.Skipf("set %s to a real installed Linux Amp binary", installedLinuxAmpEnv)
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
	scratchParent := makeInstalledLinuxAmpScratchParent(t)

	client := newTestClient(t, nil, Options{
		CLIPath:       path,
		Cwd:           t.TempDir(),
		SettingsFile:  settingsFile,
		ScratchParent: scratchParent,
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
		t.Fatalf("start installed Linux Amp login: %v", err)
	}
	t.Cleanup(func() { _ = login.Close() })

	deadline := time.Now().Add(20 * time.Second)
	for {
		launched, readErr := os.ReadFile(marker)
		if readErr == nil {
			if got := strings.TrimSpace(string(launched)); got != "xdg-open" {
				t.Fatalf("installed Amp executed shim %q, want xdg-open", got)
			}

			break
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			t.Fatalf("read installed Amp browser marker: %v", readErr)
		}
		if time.Now().After(deadline) {
			t.Fatal("installed Amp never executed the PATH-shadowed xdg-open shim")
		}

		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	if closeErr := login.Close(); closeErr != nil {
		t.Fatalf("close installed Linux Amp login: %v", closeErr)
	}
}

func makeInstalledLinuxAmpScratchParent(t *testing.T) string {
	t.Helper()

	path, err := os.MkdirTemp("/tmp", "acp-go-amp-browser-scratch-") //nolint:usetesting // The isolated native identity must traverse the fixture root.
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(path) })
	if err = os.Chmod(path, 0o711); err != nil {
		t.Fatal(err)
	}
	if err = os.Chown(path, os.Geteuid(), os.Getegid()); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestBrowserShimLeavesNothingBehind(t *testing.T) {
	parent := t.TempDir()

	shim, err := newBrowserShim(parent)
	if err != nil {
		t.Fatalf("newBrowserShim: %v", err)
	}

	info, err := os.Stat(shim.dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("shim directory mode = %v, %v; want 0700", info, err)
	}

	for _, name := range browserLauncherNames {
		launcher, statErr := os.Stat(filepath.Join(shim.dir, name))
		if statErr != nil || launcher.Mode().Perm()&0o100 == 0 {
			t.Fatalf("%s launcher = %v, %v; want an executable no-op", name, launcher, statErr)
		}
	}

	if removeErr := shim.remove(); removeErr != nil {
		t.Fatalf("remove: %v", removeErr)
	}

	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch parent after remove = %#v, %v; want empty", entries, err)
	}

	if removeErr := (*browserShim)(nil).remove(); removeErr != nil {
		t.Fatalf("remove on an unbuilt shim = %v", removeErr)
	}
}

// TestStartAuthLoginRefusesAnUnbuildableShim keeps the leg fail-closed: a login
// that cannot prove the browser launch is neutralised never starts a child.
func TestStartAuthLoginRefusesAnUnbuildableShim(t *testing.T) {
	want := errors.New("no scratch")
	original := browserShimMkdirTemp
	browserShimMkdirTemp = func(string, string) (string, error) { return "", want }

	t.Cleanup(func() { browserShimMkdirTemp = original })

	path, state := fakeAmpPath(t, "login")

	client := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir(), ScratchParent: t.TempDir()})
	if _, err := client.StartAuthLogin(t.Context()); !errors.Is(err, want) {
		t.Fatalf("StartAuthLogin = %v, want %v", err, want)
	}

	if _, err := os.Stat(filepath.Join(state, "args.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused login still launched the harness: %v", err)
	}
}

func TestNewBrowserShimReportsAnUnusableScratch(t *testing.T) {
	want := errors.New("no scratch")
	originalMkdirTemp := browserShimMkdirTemp
	browserShimMkdirTemp = func(string, string) (string, error) { return "", want }

	t.Cleanup(func() { browserShimMkdirTemp = originalMkdirTemp })

	if _, err := newBrowserShim(t.TempDir()); !errors.Is(err, want) {
		t.Fatalf("newBrowserShim = %v, want %v", err, want)
	}

	browserShimMkdirTemp = originalMkdirTemp

	wantWrite := errors.New("no launcher")
	originalWriteFile := browserShimWriteFile
	browserShimWriteFile = func(string, []byte, os.FileMode) error { return wantWrite }

	t.Cleanup(func() { browserShimWriteFile = originalWriteFile })

	parent := t.TempDir()
	if _, err := newBrowserShim(parent); !errors.Is(err, wantWrite) {
		t.Fatalf("newBrowserShim = %v, want %v", err, wantWrite)
	}

	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch parent after a failed build = %#v, %v; want empty", entries, err)
	}
}
