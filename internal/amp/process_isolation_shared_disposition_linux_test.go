//go:build linux

package amp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSharedIdentityIsolationCarriesNoStandaloneOwnerFields proves the
// disposition validator treats no capabilities and no standalone fields as the
// canonical shared shape, refuses fields that promise a durable record this arm
// never writes, and leaves the isolated refusals exactly as they were.
func TestSharedIdentityIsolationCarriesNoStandaloneOwnerFields(t *testing.T) {
	restoreSharedIdentitySeams(t)
	processIsolationGeteuid = func() int { return 1000 }

	shared := func() *ProcessIsolation {
		return &ProcessIsolation{UID: 1000, GID: 1000, BaseEnvironment: map[string]string{}}
	}

	require.NoError(t, validateProcessIsolation(shared()))

	withOwner := shared()
	withOwner.StandaloneOwnerID = "acp-go-amp-shared"
	require.ErrorContains(t, validateProcessIsolation(withOwner),
		"standalone owner fields describe an identity the supervisor already holds; "+
			sharedIdentitySupervisorRemedy)

	withStateRoot := shared()
	withStateRoot.StandaloneStateRoot = "/var/tmp/acp-go-amp-shared"
	require.ErrorContains(t, validateProcessIsolation(withStateRoot),
		"standalone owner fields describe an identity the supervisor already holds")

	isolated := shared()
	isolated.UID = 1001
	require.ErrorContains(t, validateProcessIsolation(isolated),
		"standalone owner id must match [A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}")

	borrowed := shared()
	placeholder := &agentIdentityLock{}
	borrowed.IdentityLock, borrowed.AuthorityDomain = placeholder, placeholder
	require.NoError(t, validateProcessIsolation(borrowed))
}
