package ampacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

func TestDiscoveryProbeUsesAndRemovesDedicatedResidence(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	scratch := testScratchDir(t)
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(scratch))

	err := agent.ensureStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	})
	require.NoError(t, err)
	requireNoTransientProbeRoots(t, scratch)
}

func TestStartupProbeRetainsTheValidatedHarness(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))

	require.Empty(t, agent.retainedHarnessPath())
	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) {
		return path, nil
	})
	require.NoError(t, err)
	require.Equal(t, path, agent.retainedHarnessPath())

	broken := newTestAgent(WithScratchDir(testScratchDir(t)))
	err = broken.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(context.Context, *nativeamp.Client) (string, error) {
		return "amp", nil
	})
	require.ErrorContains(t, err, "unusable harness path")
	require.Empty(t, broken.retainedHarnessPath())
}

func TestStartupProbeResidenceMaterializationFailures(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	probe := func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	}

	t.Run("mkdir temp", func(t *testing.T) {
		original := mkdirTemp
		t.Cleanup(func() { mkdirTemp = original })
		mkdirTemp = func(string, string) (string, error) { return "", errors.New("mkdir temp") }
		agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
		err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, probe)
		require.ErrorContains(t, err, "create Amp startup probe residence")
		require.Empty(t, agent.cleanupResidences)
	})

	t.Run("mkdir home", func(t *testing.T) {
		original := mkdirAll
		t.Cleanup(func() { mkdirAll = original })
		mkdirAll = func(string, os.FileMode) error { return errors.New("mkdir home") }
		agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
		err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, probe)
		require.ErrorContains(t, err, "create Amp startup probe isolated home")
		require.Empty(t, agent.cleanupResidences)
	})

	t.Run("write settings", func(t *testing.T) {
		original := writeFile
		t.Cleanup(func() { writeFile = original })
		writeFile = func(string, []byte, os.FileMode) error { return errors.New("write settings") }
		agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
		err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, probe)
		require.ErrorContains(t, err, "write Amp startup probe settings")
		require.Empty(t, agent.cleanupResidences)
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
		agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
		err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, probe)
		require.ErrorContains(t, err, "write Amp startup probe MCP config")
		require.Empty(t, agent.cleanupResidences)
	})
}

func TestStartupProbeRemovalRetriesDuringNormalOperation(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	original := removeSessionDir
	t.Cleanup(func() { removeSessionDir = original })
	removeSessionDir = func(string) error { return errors.New("temporary removal refusal") }
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	probe := func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	}

	require.ErrorContains(t, agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, probe), "temporary removal refusal")
	require.Len(t, agent.cleanupResidences, 1)

	removeSessionDir = original
	require.NoError(t, agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, probe))
	require.Empty(t, agent.cleanupResidences)
}

func requireNoTransientProbeRoots(t *testing.T, scratch string) {
	t.Helper()
	entries, err := os.ReadDir(scratch)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), "acp-go-amp-startup-"), entry.Name())
	}
}

func TestAgentCleanupResidenceResidualBranches(t *testing.T) {
	closed := NewAgent()
	closed.closed = true
	_, err := closed.reserveCleanupResidence()
	require.Error(t, err)

	agent := NewAgent()
	opaque := &agentCleanupResidence{agent: agent, id: 1, opaque: true, retryable: true}
	agent.cleanupResidences[opaque.id] = opaque
	agent.retryCleanupResidences(t.Context())
	require.Contains(t, agent.cleanupResidences, opaque.id)

	agent = NewAgent()
	root := t.TempDir()
	residence := &agentCleanupResidence{agent: agent, id: 1, root: root, retryable: true}
	agent.cleanupResidences[residence.id] = residence
	original := removeSessionDir
	panicked := false
	removeSessionDir = func(path string) error {
		if !panicked {
			panicked = true
			panic("cleanup panic")
		}

		return original(path)
	}
	t.Cleanup(func() { removeSessionDir = original })
	require.ErrorIs(t, agent.closeAttempt(), errAgentGoroutinePanic)
	require.Empty(t, agent.cleanupResidences)
}

func TestCleanupResidenceContextReclaimRetainsPreparedRetry(t *testing.T) {
	authority := &contextReclaimAuthority{
		residualAuthority: residualAuthority{environment: nativeamp.CaptureOrdinaryEnvironment()},
		started:           make(chan struct{}),
		exited:            make(chan struct{}),
	}
	agent := NewAgent(WithHostAuthority(authority))
	residence := &agentCleanupResidence{
		agent: agent, id: 1, root: t.TempDir(), prepared: true,
	}
	agent.cleanupResidences[residence.id] = residence

	reclaimCtx, cancelReclaim := context.WithCancel(t.Context())
	reclaimDone := make(chan error, 1)
	go func() { reclaimDone <- residence.reclaimWithContext(reclaimCtx) }()
	<-authority.started
	cancelReclaim()

	require.ErrorIs(t, <-reclaimDone, context.Canceled)
	<-authority.exited
	require.True(t, residence.prepared)
	require.True(t, residence.retryable)
	require.Same(t, residence, agent.cleanupResidences[residence.id])
}
