package amp

import (
	"os"
	"testing"
)

// TestContainmentProofReportsOnlyWhatTheBoundaryEnumerated pins that vacancy is
// an observation: a turn with no contained tree, and a boundary that publishes no
// inventory, both report an unproven proof rather than an empty descendant set.
func TestContainmentProofReportsOnlyWhatTheBoundaryEnumerated(t *testing.T) {
	if proof := (&Turn{}).ContainmentProof(); proof.Proven || proof.Vacant() {
		t.Fatalf("treeless proof = %#v, want unproven", proof)
	}

	turn := &Turn{tree: &processTree{process: &os.Process{Pid: 4242}}}
	if proof := turn.ContainmentProof(); proof.Proven || proof.Vacant() || proof.Root != 4242 {
		t.Fatalf("uninstrumented proof = %#v, want unproven root 4242", proof)
	}

	original := processTreeDescendantCount
	t.Cleanup(func() { processTreeDescendantCount = original })

	processTreeDescendantCount = func(*processTree) (int, bool) { return 0, true }

	if proof := turn.ContainmentProof(); !proof.Vacant() {
		t.Fatalf("enumerated empty proof = %#v, want vacant", proof)
	}

	processTreeDescendantCount = func(*processTree) (int, bool) { return 2, true }

	if proof := turn.ContainmentProof(); proof.Vacant() || proof.Descendants != 2 {
		t.Fatalf("enumerated live proof = %#v, want two descendants and no vacancy", proof)
	}
}
