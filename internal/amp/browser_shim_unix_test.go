//go:build !windows

package amp

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

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

	originalWriteFile := browserShimWriteFile
	t.Cleanup(func() { browserShimWriteFile = originalWriteFile })
	browserShimWriteFile = func(string, []byte, os.FileMode) error {
		return errors.New("write fault")
	}
	if _, err := MaterializeBrowserShim(filepath.Join(t.TempDir(), "browser")); err == nil {
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
	var harness strings.Builder
	harness.WriteString("#!/bin/sh\n")

	for _, name := range browserLauncherNames {
		harness.WriteString("command -v " + name + " >> " + shellQuote(resolvedFile) + "\n" +
			name + " \"https://example.invalid/\"\n")
	}

	harness.WriteString("echo " + shellQuote(helperLoginURL) + "\nexit 0\n")

	if writeErr := os.WriteFile(path, []byte(harness.String()), 0o700); writeErr != nil {
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
