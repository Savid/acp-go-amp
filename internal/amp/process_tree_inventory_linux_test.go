//go:build linux

package amp

import (
	"errors"
	"testing"
)

// TestDescendantCountEnumeratesOnlyASupervisedTree pins the one configuration
// whose descendant set is exactly enumerable. Ordinary execution owns no
// subreaper root, so it reports no inventory rather than an empty set, and an
// enumeration that fails reports no inventory rather than a guess.
func TestDescendantCountEnumeratesOnlyASupervisedTree(t *testing.T) {
	original := turnSupervisorDescendants
	t.Cleanup(func() { turnSupervisorDescendants = original })

	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) {
		t.Fatal("an unsupervised tree enumerates nothing")

		return nil, nil
	}

	if count, available := (&processTree{pgid: 4242}).descendantCount(); available || count != 0 {
		t.Fatalf("ordinary inventory = (%d, %t), want unavailable", count, available)
	}

	supervised := &processTree{pgid: 4242, supervised: true}

	turnSupervisorDescendants = func(root int) ([]linuxProcessIdentity, error) {
		if root != 4242 {
			t.Fatalf("enumerated root %d, want the supervised root 4242", root)
		}

		return nil, errors.New("proc read failed")
	}

	if count, available := supervised.descendantCount(); available || count != 0 {
		t.Fatalf("failed enumeration = (%d, %t), want unavailable", count, available)
	}

	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) {
		return []linuxProcessIdentity{{pid: 1}, {pid: 2}}, nil
	}

	if count, available := supervised.descendantCount(); !available || count != 2 {
		t.Fatalf("enumerated inventory = (%d, %t), want (2, true)", count, available)
	}

	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) { return nil, nil }

	if count, available := supervised.descendantCount(); !available || count != 0 {
		t.Fatalf("vacant inventory = (%d, %t), want (0, true)", count, available)
	}
}
