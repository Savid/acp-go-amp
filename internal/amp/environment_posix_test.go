//go:build !windows

package amp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLookPathInOrdinaryEnvironmentWithUnixRules pins the two Unix-only search
// facts the shared platform-rules case cannot state on every host: an empty
// PATH element means the working directory, and the execute bit is what makes a
// file a candidate. Windows records no execute bit, so a file that satisfies
// this rule set cannot exist there and the case is built only where it can.
func TestLookPathInOrdinaryEnvironmentWithUnixRules(t *testing.T) {
	bin := t.TempDir()
	executable := filepath.Join(bin, "tool")
	require.NoError(t, os.WriteFile(executable, []byte("x"), 0o700))

	for _, search := range []string{"PATH=.:missing", "PATH=:missing"} {
		path, err := lookPathInOrdinaryEnvironmentWithRules("tool", []string{search}, bin, unixOrdinaryExecutableRules())
		require.NoError(t, err)
		require.Equal(t, executable, path)
	}
}
