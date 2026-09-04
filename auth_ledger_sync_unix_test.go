//go:build !windows

package ampacp

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestSyncAuthLedgerDirectoryUnix proves the POSIX flush both reaches a real
// directory and reports a root it cannot open, which is the branch the ledger
// write turns into a commit failure.
func TestSyncAuthLedgerDirectoryUnix(t *testing.T) {
	root := t.TempDir()
	if err := syncAuthLedgerDirectory(root); err != nil {
		t.Fatalf("sync real directory: %v", err)
	}

	original := ledgerOpen
	t.Cleanup(func() { ledgerOpen = original })

	want := errors.New("open")
	ledgerOpen = func(string) (*os.File, error) { return nil, want }

	if err := syncAuthLedgerDirectory(filepath.Join(root, "missing")); !errors.Is(err, want) {
		t.Fatalf("open failure = %v", err)
	}
}
