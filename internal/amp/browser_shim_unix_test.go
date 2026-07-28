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

	for _, name := range []string{"open", "xdg-open"} {
		if err := os.WriteFile(filepath.Join(dir, name), script, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

// TestLoginNeverExecsABrowserLauncher runs the real login path with a recording
// browser launcher ahead of every other PATH entry the child inherits. The
// control run proves the recorder fires when something execs it; the login run
// then has to leave the recorder untouched.
func TestLoginNeverExecsABrowserLauncher(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "launched")
	probe := browserProbeDir(t, marker)

	t.Setenv("PATH", probe+string(os.PathListSeparator)+os.Getenv("PATH"))

	resolved, err := exec.LookPath("open")
	if err != nil || resolved != filepath.Join(probe, "open") {
		t.Fatalf("open resolved to %q, %v; want the probe launcher", resolved, err)
	}

	if runErr := exec.Command(resolved, "probe://control").Run(); runErr != nil {
		t.Fatalf("probe launcher control run: %v", runErr)
	}

	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("the probe launcher recorded nothing when executed directly: %v", statErr)
	}

	if removeErr := os.Remove(marker); removeErr != nil {
		t.Fatal(removeErr)
	}

	dir := t.TempDir()
	resolvedFile := filepath.Join(dir, "resolved")
	path := filepath.Join(dir, "amp")
	// The launcher name is bare so the child resolves it through the PATH it was
	// handed, which is exactly how the native binary opens a URL.
	harness := "#!/bin/sh\ncommand -v open > " + shellQuote(resolvedFile) + "\n" +
		"open \"https://example.invalid/\"\nxdg-open \"https://example.invalid/\"\n" +
		"echo " + shellQuote(helperLoginURL) + "\nexit 0\n"

	if writeErr := os.WriteFile(path, []byte(harness), 0o700); writeErr != nil {
		t.Fatal(writeErr)
	}

	client := newTestClient(t, nil, Options{
		CLIPath:       path,
		Cwd:           t.TempDir(),
		ScratchParent: t.TempDir(),
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
	// it, so the name it did resolve has to be the shim standing in front of it.
	shimmed, err := os.ReadFile(resolvedFile)
	if err != nil || !strings.Contains(string(shimmed), browserShimPrefix) {
		t.Fatalf("the child resolved open to %q, %v; want a shim launcher", shimmed, err)
	}
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
