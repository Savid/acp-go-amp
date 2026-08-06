package ampacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProcessIsolationOptionIsRefusedOnWindows proves the option is rejected on
// Windows rather than silently accepted and ignored. Process isolation is the
// mechanism that drops the native agent to a separate account; a wrapper that
// took the option, reported no error, and then launched the agent under its own
// identity would hand a caller who asked for containment a completely
// uncontained agent.
func TestProcessIsolationOptionIsRefusedOnWindows(t *testing.T) {
	original := runtimeGOOS
	t.Cleanup(func() { runtimeGOOS = original })

	isolation := &ProcessIsolation{
		UID: 65534, GID: 65534,
		StandaloneOwnerID:   "windows-refusal",
		StandaloneStateRoot: "/srv/amp/state",
	}

	runtimeGOOS = platformWindows
	require.ErrorContains(
		t,
		validateProcessIsolationOption(isolation),
		"process isolation is unsupported on windows",
	)

	runtimeGOOS = platformDarwin
	require.NoError(t, validateProcessIsolationOption(isolation))
}
