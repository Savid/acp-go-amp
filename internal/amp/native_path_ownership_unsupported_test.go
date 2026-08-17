//go:build !linux

package amp

import (
	"testing"
)

func TestUnsupportedGeneratedNativeTreeHandoff(t *testing.T) {
	if err := handoffGeneratedNativeTree("unused", nil); err != nil {
		t.Fatal(err)
	}
	if err := handoffGeneratedNativeTree("unused", &ProcessIsolation{TestOnlyNoCredential: true}); err == nil {
		t.Fatal("test-only explicit handoff succeeded")
	}
	if err := handoffGeneratedNativeTree("unused", &ProcessIsolation{UID: 1, GID: 1}); err == nil {
		t.Fatal("unsupported generated-tree handoff succeeded")
	}
}
