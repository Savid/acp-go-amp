//go:build !linux

package ampacp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUnsupportedNativePathOwnership(t *testing.T) {
	if err := handoffGeneratedNativeTree("unused", nil); err != nil {
		t.Fatal(err)
	}
	current := &ProcessIsolation{UID: uint32(os.Geteuid()), GID: uint32(os.Getegid())}
	if err := handoffGeneratedNativeTree("unused", current); err != nil {
		t.Fatal(err)
	}
	different := &ProcessIsolation{UID: current.UID + 1, GID: current.GID + 1}
	if err := handoffGeneratedNativeTree("unused", different); err == nil {
		t.Fatal("unsupported ownership handoff succeeded")
	}

	path := filepath.Join(t.TempDir(), "native-owned")
	if err := writeNativeOwnedFile(path, []byte("ignored"), nil); err != nil {
		t.Fatal(err)
	}
	if err := writeNativeOwnedFile(path, []byte("contents"), current); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "contents" {
		t.Fatalf("native-owned file = %q, %v", contents, err)
	}
	if err := writeNativeOwnedFile(path, []byte("rejected"), different); err == nil {
		t.Fatal("unsupported native-owned write succeeded")
	}
}
