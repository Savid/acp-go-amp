//go:build !linux

package amp

import (
	"os"
	"testing"
)

func TestUnsupportedGeneratedNativeTreeHandoff(t *testing.T) {
	if err := handoffGeneratedNativeTree("unused", nil); err != nil {
		t.Fatal(err)
	}
	if err := handoffGeneratedNativeTree("unused", &ProcessIsolation{TestOnlyNoCredential: true}); err != nil {
		t.Fatal(err)
	}
	current := &ProcessIsolation{UID: uint32(os.Geteuid()), GID: uint32(os.Getegid())}
	if err := handoffGeneratedNativeTree("unused", current); err != nil {
		t.Fatal(err)
	}
	if err := handoffGeneratedNativeTree("unused", &ProcessIsolation{UID: current.UID + 1, GID: current.GID + 1}); err == nil {
		t.Fatal("unsupported generated-tree handoff succeeded")
	}
}
