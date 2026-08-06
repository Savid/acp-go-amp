//go:build linux

package amp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProcessIsolationCovStandaloneStateRootMustBeACleanAbsolutePath proves
// that the launch policy validator refuses every standalone state root that
// is not a plain, clean, absolute, control-character-free path. Each of these
// shapes is a way to smuggle a different directory past the later
// path-ownership checks, so the refusal has to happen before the policy is
// ever handed to a supervisor.
func TestProcessIsolationCovStandaloneStateRootMustBeACleanAbsolutePath(t *testing.T) {
	for name, stateRoot := range map[string]string{
		"empty":            "",
		"relative":         "state/root",
		"unclean":          "/var/tmp/../tmp/state",
		"trailing_slash":   "/var/tmp/state/",
		"filesystem_root":  "/",
		"embedded_nul":     "/var/tmp/state\x00root",
		"invalid_utf8":     "/var/tmp/state\xffroot",
		"too_long":         "/var/tmp/" + strings.Repeat("s", 4096),
		"control_byte":     "/var/tmp/state\x01root",
		"control_delete":   "/var/tmp/state\x7froot",
		"control_unicode":  "/var/tmp/state\u0085root",
		"authority_root":   "/var/lib/acp-go/agent-identities",
		"authority_subdir": "/var/lib/acp-go/agent-identities/11.lock",
	} {
		t.Run(name, func(t *testing.T) {
			err := validateProcessIsolation(&ProcessIsolation{
				UID: 11, GID: 22, BaseEnvironment: map[string]string{},
				StandaloneOwnerID: "cov-owner", StandaloneStateRoot: stateRoot,
			})
			require.ErrorContains(t, err, "standalone state root must be a clean absolute path")
			require.False(t, validStandaloneStateRootPath(stateRoot))
		})
	}

	require.NoError(t, validateProcessIsolation(&ProcessIsolation{
		UID: 11, GID: 22, BaseEnvironment: map[string]string{},
		StandaloneOwnerID: "cov-owner", StandaloneStateRoot: "/var/tmp/acp-go-amp-cov-state",
	}))
}

// TestProcessIsolationCovStandaloneOwnerIDRejectsOutOfAlphabetBytes proves
// that the launch policy validator refuses a standalone owner id containing
// any byte outside the accepted alphabet, including in the tail after a valid
// leading character. The owner id names a durable on-disk authority binding,
// so a whitespace, shell or path-traversal byte must never reach it.
func TestProcessIsolationCovStandaloneOwnerIDRejectsOutOfAlphabetBytes(t *testing.T) {
	for name, ownerID := range map[string]string{
		"trailing_bang":    "cov-owner!",
		"embedded_space":   "cov owner",
		"embedded_newline": "cov\nowner",
		"embedded_nul":     "cov\x00owner",
		"percent":          "cov%owner",
		"backslash":        `cov\owner`,
	} {
		t.Run(name, func(t *testing.T) {
			err := validateProcessIsolation(&ProcessIsolation{
				UID: 11, GID: 22, BaseEnvironment: map[string]string{},
				StandaloneOwnerID: ownerID, StandaloneStateRoot: "/var/tmp/acp-go-amp-cov-state",
			})
			require.ErrorContains(t, err, "standalone owner id must match [A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}")
			require.False(t, validStandaloneOwnerID(ownerID))
		})
	}

	require.True(t, validStandaloneOwnerID("cov-owner._:@/-0"))
}
