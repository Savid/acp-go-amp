package ampacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

func TestStartupProbeUsesOrdinaryCurrentIdentityResidence(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "probe-residence")
	scratch := testScratchDir(t)
	forbidden := map[string]string{}
	for _, name := range []string{envHome, envXDGConfigHome, envXDGCacheHome, envXDGDataHome, envXDGStateHome} {
		dir := filepath.Join(t.TempDir(), strings.ToLower(name))
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "sentinel"), []byte("unchanged"), 0o600))
		forbidden[name] = dir
	}

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(scratch),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	for key, value := range forbidden {
		agent.ordinaryEnvironment[key] = value
	}
	wantUID := strconv.Itoa(os.Geteuid())
	wantGID := strconv.Itoa(os.Getegid())

	_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	records := readHelperJSON[map[string]string](t, filepath.Join(state, "probe-residence.jsonl"))
	require.Len(t, records, 5)
	probeRoots := map[string]bool{}
	for _, record := range records {
		probeRoot := filepath.Dir(record["home"])
		probeRoots[probeRoot] = true
		require.True(t, filepath.IsAbs(probeRoot))
		require.Equal(t, scratch, filepath.Dir(probeRoot))
		if runtime.GOOS == "darwin" {
			require.Contains(t, filepath.Base(probeRoot), "acp-go-amp-command-")
		} else {
			require.Contains(t, filepath.Base(probeRoot), "acp-go-amp-startup-")
		}
		expectedDirs := map[string]string{
			"home":   filepath.Join(probeRoot, "home"),
			"config": filepath.Join(probeRoot, "xdg-config"),
			"cache":  filepath.Join(probeRoot, "xdg-cache"),
			"data":   filepath.Join(probeRoot, "xdg-data"),
			"state":  filepath.Join(probeRoot, "xdg-state"),
		}
		for name, want := range expectedDirs {
			require.Equal(t, want, record[name])
			require.Empty(t, record[name+"Error"])
			require.Empty(t, record[name+"WriteError"])
			require.Equal(t, "700", record[name+"Mode"])
			require.Equal(t, wantUID, record[name+"UID"])
			require.Equal(t, wantGID, record[name+"GID"])
		}
		if record["settings"] != "" {
			require.Equal(t, filepath.Join(probeRoot, "xdg-config", "amp", "settings.json"), record["settings"])
			require.Equal(t, filepath.Join(probeRoot, "mcp.json"), record["mcp"])
			for _, name := range []string{"settings", "mcp"} {
				require.Empty(t, record[name+"Error"])
				require.Equal(t, "600", record[name+"Mode"])
				require.Equal(t, wantUID, record[name+"UID"])
				require.Equal(t, wantGID, record[name+"GID"])
			}
		}
	}
	if runtime.GOOS != "darwin" {
		require.Len(t, probeRoots, 1)
	}

	for probeRoot := range probeRoots {
		_, statErr := os.Stat(probeRoot)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
	for _, dir := range forbidden {
		entries, readErr := os.ReadDir(dir)
		require.NoError(t, readErr)
		require.Len(t, entries, 1)
		require.Equal(t, "sentinel", entries[0].Name())
	}

	require.NoError(t, agent.Close())
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	requireNoTransientProbeRoots(t, entries)
}

func TestDiscoveryProbeUsesAndRemovesIsolatedResidence(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "probe-residence")
	scratch := testScratchDir(t)
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(scratch))
	err := agent.ensureStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	})
	require.NoError(t, err)

	records := readHelperJSON[map[string]string](t, filepath.Join(state, "probe-residence.jsonl"))
	require.Len(t, records, 1)
	require.Equal(t, scratch, filepath.Dir(filepath.Dir(records[0]["home"])))
	require.Empty(t, records[0]["settings"])
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	requireNoTransientProbeRoots(t, entries)
}

// TestStartupProbeRetainsTheValidatedHarness pins that the agent keeps the
// exact absolute harness the probe validated, and that a probe answering
// anything else is a failure rather than a reason to resolve again.
func TestStartupProbeRetainsTheValidatedHarness(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "record-env")
	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	t.Cleanup(func() { _ = agent.Close() })

	require.Empty(t, agent.retainedHarnessPath())

	_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	require.Equal(t, path, agent.retainedHarnessPath())

	broken := newTestAgent(WithScratchDir(testScratchDir(t)))
	t.Cleanup(func() { _ = broken.Close() })

	err = broken.runStartupWithProbe(t.Context(), t.TempDir(), nil,
		func(context.Context, *nativeamp.Client) (string, error) { return "amp", nil },
	)
	require.ErrorContains(t, err, "unusable harness path")
	require.Empty(t, broken.retainedHarnessPath())
}

func requireNoTransientProbeRoots(t *testing.T, entries []os.DirEntry) {
	t.Helper()
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "acp-go-amp-startup-") ||
			strings.HasPrefix(entry.Name(), "acp-go-amp-command-") ||
			strings.HasPrefix(entry.Name(), "acp-go-amp-session-") {
			t.Fatalf("transient probe/session root retained: %s", entry.Name())
		}
	}
}

func TestStartupProbeResidenceCleanupRequiresContainmentProof(t *testing.T) {
	scratch := testScratchDir(t)
	releases := 0
	agent := newTestAgent(
		WithScratchDir(scratch),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { releases++ }, nil
			},
		}),
	)

	wantErr := errors.New("probe failed")
	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) {
		return "", wantErr
	})
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, releases)
	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Empty(t, entries)

	err = agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) {
		return "", nativeamp.ErrProcessContainmentIncomplete
	})
	require.ErrorIs(t, err, nativeamp.ErrProcessContainmentIncomplete)
	require.Equal(t, 1, releases)
	entries, readErr = os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Len(t, entries, 1)
	require.Contains(t, entries[0].Name(), "acp-go-amp-startup-")
}

func TestStartupProbeResidenceMaterializationAndRemovalFailures(t *testing.T) {
	t.Run("scratch parent", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
		releases := 0
		agent := newTestAgent(WithScratchDir(file), WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { releases++ }, nil
			},
		}))
		err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) { return testHarnessPath(t), nil })
		require.Error(t, err)
		require.Equal(t, 1, releases)
	})

	t.Run("mkdir temp", func(t *testing.T) {
		original := mkdirTemp
		t.Cleanup(func() { mkdirTemp = original })
		mkdirTemp = func(string, string) (string, error) { return "", errors.New("mkdir temp") }
		err := newTestAgent(WithScratchDir(testScratchDir(t))).runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) { return testHarnessPath(t), nil })
		require.ErrorContains(t, err, "create Amp startup probe residence")
	})

	t.Run("mkdir isolated home", func(t *testing.T) {
		original := mkdirAll
		t.Cleanup(func() { mkdirAll = original })
		mkdirAll = func(string, os.FileMode) error { return errors.New("mkdir home") }
		err := newTestAgent(WithScratchDir(testScratchDir(t))).runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) { return testHarnessPath(t), nil })
		require.ErrorContains(t, err, "create Amp startup probe isolated home")
	})

	t.Run("write settings", func(t *testing.T) {
		original := writeFile
		t.Cleanup(func() { writeFile = original })
		writeFile = func(string, []byte, os.FileMode) error { return errors.New("write settings") }
		err := newTestAgent(WithScratchDir(testScratchDir(t))).runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) { return testHarnessPath(t), nil })
		require.ErrorContains(t, err, "write Amp startup probe settings")
	})

	t.Run("write MCP", func(t *testing.T) {
		original := writeFile
		t.Cleanup(func() { writeFile = original })
		writeFile = func(path string, data []byte, mode os.FileMode) error {
			if filepath.Base(path) == "mcp.json" {
				return errors.New("write MCP")
			}

			return original(path, data, mode)
		}
		err := newTestAgent(WithScratchDir(testScratchDir(t))).runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) { return testHarnessPath(t), nil })
		require.ErrorContains(t, err, "write Amp startup probe MCP config")
	})

	t.Run("remove", func(t *testing.T) {
		original := removeSessionDir
		t.Cleanup(func() { removeSessionDir = original })
		removeSessionDir = func(string) error { return errors.New("remove residence") }
		releases := 0
		agent := newTestAgent(WithScratchDir(testScratchDir(t)), WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { releases++ }, nil
			},
		}))
		err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) { return testHarnessPath(t), nil })
		require.ErrorContains(t, err, "remove residence")
		require.Zero(t, releases)
	})
}

func TestStartupProbeResidenceReservationAndOwnershipFailures(t *testing.T) {
	wantErr := errors.New("resource exhausted")
	agent := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) { return testHarnessPath(t), nil })
	require.ErrorIs(t, err, wantErr)

	isolationAgent := NewAgent(
		WithScratchDir(t.TempDir()),
		WithProcessIsolation(ProcessIsolation{
			UID: uint32(os.Geteuid()) + 1, GID: uint32(os.Getegid()) + 1,
			BaseEnvironment: map[string]string{},
		}),
	)
	err = isolationAgent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) { return testHarnessPath(t), nil })
	require.ErrorContains(t, err, "handoff Amp startup probe residence")
}
