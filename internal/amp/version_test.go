package amp

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeHarness writes an executable that prints script and exits with code.
func fakeHarness(t *testing.T, script string, code int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "amp")
	contents := "#!/bin/sh\nprintf '" + script + "'\nexit " + strconv.Itoa(code) + "\n"
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o700))

	return path
}

func TestProbeVersionReadsTheFirstField(t *testing.T) {
	t.Parallel()

	version, err := ProbeVersion(context.Background(), fakeHarness(t, "0.0.1789344113-g6e4515 2026-09-14T00:01:53Z\\n", 0), os.Environ())
	require.NoError(t, err)
	require.Equal(t, "0.0.1789344113-g6e4515", version)
}

func TestProbeVersionRefusesAnUnusableHarness(t *testing.T) {
	t.Parallel()

	_, err := ProbeVersion(context.Background(), fakeHarness(t, "", 1), os.Environ())
	require.Error(t, err)

	_, err = ProbeVersion(context.Background(), fakeHarness(t, "", 0), os.Environ())
	require.Error(t, err)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = ProbeVersion(cancelled, fakeHarness(t, "1.0.0\\n", 0), os.Environ())
	require.Error(t, err)
}
