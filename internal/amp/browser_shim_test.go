package amp

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestBrowserShimEnvironOwnsBothMechanisms(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shim")
	env := browserShimEnviron([]string{
		"MALFORMED",
		browserShimPathEnv + "=/usr/bin:/bin",
		browserShimBrowserEnv + "=/opt/real-browser",
		"XDG_DATA_HOME=/data",
	}, dir)

	wantPath := browserShimPathEnv + "=" + dir + string(os.PathListSeparator) + "/usr/bin:/bin"
	if !slices.Contains(env, wantPath) {
		t.Fatalf("environ did not lead PATH with the shim: %v", env)
	}

	if !slices.Contains(env, browserShimBrowserEnv+"="+filepath.Join(dir, "open")) {
		t.Fatalf("environ did not point BROWSER at a no-op: %v", env)
	}

	for _, entry := range env {
		if entry == browserShimBrowserEnv+"=/opt/real-browser" {
			t.Fatalf("environ kept the operator's browser: %v", env)
		}
	}

	if !slices.Contains(env, "MALFORMED") || !slices.Contains(env, "XDG_DATA_HOME=/data") {
		t.Fatalf("environ dropped an unrelated entry: %v", env)
	}

	// An environment with no PATH at all still gets one naming only the shim.
	bare := (&browserShim{dir: dir}).environ(nil)
	if !slices.Contains(bare, browserShimPathEnv+"="+dir) {
		t.Fatalf("environ without a PATH = %v", bare)
	}
}
