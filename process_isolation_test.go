package ampacp

import (
	"testing"
)

func TestProcessIsolationOptionClonesAndFailsClosed(t *testing.T) {
	if processIsolationBase(nil) != nil {
		t.Fatal("nil isolation exposed a base environment")
	}
	if err := NewAgent().validateSessionStartOptions(AmpOptions{}); err == nil {
		t.Fatal("session start accepted missing isolation")
	}
	base := map[string]string{"PATH": "/policy/bin", "CANARY": "base"}
	opts := applyOptions([]Option{WithProcessIsolation(ProcessIsolation{UID: 10, GID: 20, BaseEnvironment: base})})
	base["CANARY"] = "mutated"
	if got := opts.ProcessIsolation.BaseEnvironment["CANARY"]; got != "base" {
		t.Fatalf("cloned base = %q", got)
	}
	internal := nativeProcessIsolation(opts.ProcessIsolation, false)
	opts.ProcessIsolation.BaseEnvironment["CANARY"] = "later"
	if got := internal.BaseEnvironment["CANARY"]; got != "base" {
		t.Fatalf("internal cloned base = %q", got)
	}
	if nativeProcessIsolation(nil, false) != nil {
		t.Fatal("nil isolation mapped non-nil")
	}
	for _, isolation := range []*ProcessIsolation{nil, {UID: 0, GID: 1}, {UID: 1, GID: 0}} {
		if validateProcessIsolationOption(isolation) == nil {
			t.Fatalf("invalid isolation accepted: %#v", isolation)
		}
	}
	original := runtimeGOOS
	runtimeGOOS = platformWindows
	t.Cleanup(func() { runtimeGOOS = original })
	if validateProcessIsolationOption(&ProcessIsolation{UID: 1, GID: 1}) == nil {
		t.Fatal("windows isolation accepted")
	}
}
