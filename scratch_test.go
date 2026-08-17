package ampacp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScratchParent(t *testing.T) {
	if got := scratchParent(""); got != os.TempDir() {
		t.Fatalf("scratchParent(empty) = %q, want %q", got, os.TempDir())
	}

	if got := scratchParent("/custom/scratch"); got != "/custom/scratch" {
		t.Fatalf("scratchParent set = %q, want /custom/scratch", got)
	}
}

func TestEnsureScratchParent(t *testing.T) {
	got, err := ensureScratchParent("")
	if err != nil {
		t.Fatalf("ensureScratchParent(empty): %v", err)
	}
	if got != os.TempDir() {
		t.Fatalf("ensureScratchParent(empty) = %q, want %q", got, os.TempDir())
	}

	nested := filepath.Join(t.TempDir(), "a", "b", "c")
	got, err = ensureScratchParent(nested)
	if err != nil {
		t.Fatalf("ensureScratchParent(nested): %v", err)
	}
	if got != nested {
		t.Fatalf("ensureScratchParent(nested) = %q, want %q", got, nested)
	}
	info, err := os.Stat(nested)
	if err != nil || !info.IsDir() {
		t.Fatalf("nested scratch not created as dir: info=%#v err=%v", info, err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Fatalf("nested scratch perm = %o, want 700", perm)
	}

	fileParent := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(fileParent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ensureScratchParent(fileParent); err == nil {
		t.Fatal("ensureScratchParent accepted a regular-file parent")
	}
}
