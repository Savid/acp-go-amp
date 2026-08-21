package ampacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

type startupProbeResidence struct {
	root         string
	home         string
	config       string
	cache        string
	data         string
	state        string
	settingsFile string
	mcpFile      string
}

type agentCleanupResidence struct {
	agent        *Agent
	id           uint64
	root         string
	release      func()
	releaseTaken bool
	boundaryErr  error
}

func (a *Agent) reserveCleanupResidence() (*agentCleanupResidence, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	if len(a.cleanupResidences) >= a.maxActiveSessions() {
		return nil, backpressureError("cleanup_residences")
	}

	a.nextCleanupResidence++
	residence := &agentCleanupResidence{agent: a, id: a.nextCleanupResidence}
	a.cleanupResidences[residence.id] = residence

	return residence, nil
}

func (r *agentCleanupResidence) setRelease(release func()) {
	r.agent.mu.Lock()
	if r.agent.cleanupResidences[r.id] == r {
		r.release = release
	}
	r.agent.mu.Unlock()
}

func (r *agentCleanupResidence) setRoot(root string) {
	r.agent.mu.Lock()
	if r.agent.cleanupResidences[r.id] == r {
		r.root = root
	}
	r.agent.mu.Unlock()
}

func (r *agentCleanupResidence) recordBoundary(err error) {
	r.agent.mu.Lock()
	if r.agent.cleanupResidences[r.id] == r {
		r.boundaryErr = errors.Join(r.boundaryErr, err)
	}
	r.agent.mu.Unlock()
}

func (r *agentCleanupResidence) finalize() error {
	r.agent.mu.Lock()
	root := r.root
	boundaryErr := r.boundaryErr
	releaseTaken := r.releaseTaken
	r.agent.mu.Unlock()

	if !amp.ProcessContainmentComplete(boundaryErr) {
		return boundaryErr
	}

	var removeErr error
	if root != "" {
		removeErr = removeSessionDir(root)
	}

	if removeErr != nil || releaseTaken {
		return removeErr
	}

	r.agent.mu.Lock()
	if r.agent.cleanupResidences[r.id] != r || r.releaseTaken {
		r.agent.mu.Unlock()

		return nil
	}

	r.releaseTaken = true
	release := r.release
	r.release = nil
	r.agent.mu.Unlock()

	if release != nil {
		release()
	}

	return nil
}

func (a *Agent) clearCleanupResidence(expect *agentCleanupResidence) {
	a.mu.Lock()
	if a.cleanupResidences[expect.id] == expect {
		delete(a.cleanupResidences, expect.id)
	}
	a.mu.Unlock()
}

func (a *Agent) retryCleanupResidences(ctx context.Context) {
	a.mu.Lock()

	residences := make([]*agentCleanupResidence, 0, len(a.cleanupResidences))
	for _, residence := range a.cleanupResidences {
		residences = append(residences, residence)
	}
	a.mu.Unlock()

	for _, residence := range residences {
		cleanupErr := invokeShutdownStep(residence.finalize)
		if cleanupErr == nil {
			a.clearCleanupResidence(residence)

			continue
		}

		a.log.DebugContext(ctx, "retry amp startup cleanup failed", slog.String("failure", cleanupFailureClass(cleanupErr)))
	}
}

// ensureStartupWithProbe validates the harness before a session exists.
// sessionEnv is the caller's session environment, and only the named operation
// values are lifted out of it: the probe children run on the static base, so
// neither the version probe nor the method probes can be pointed at a different
// binary or a different helper by a session's raw PATH.
func (a *Agent) ensureStartupWithProbe(
	ctx context.Context,
	cwd string,
	sessionEnv map[string]string,
	probe func(context.Context, *amp.Client) (string, error),
) error {
	if err := a.runStartupWithProbe(ctx, cwd, sessionEnv, probe); err != nil {
		return nativeInternalError(err)
	}

	return nil
}

func (a *Agent) runStartupWithProbe(
	ctx context.Context,
	cwd string,
	sessionEnv map[string]string,
	probe func(context.Context, *amp.Client) (string, error),
) (returnErr error) {
	a.retryCleanupResidences(ctx)

	cleanupResidence, err := a.reserveCleanupResidence()
	if err != nil {
		return err
	}

	scratchRelease, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
	if err != nil {
		a.clearCleanupResidence(cleanupResidence)

		return err
	}

	cleanupResidence.setRelease(scratchRelease)

	var residence startupProbeResidence

	defer func() {
		if !amp.ProcessContainmentComplete(returnErr) {
			cleanupResidence.recordBoundary(returnErr)

			return
		}

		cleanupErr := cleanupResidence.finalize()
		if cleanupErr == nil {
			a.clearCleanupResidence(cleanupResidence)
		}

		returnErr = errors.Join(returnErr, cleanupErr)
	}()

	parent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return err
	}

	residence, err = materializeStartupProbeResidence(parent)
	if err != nil {
		cleanupResidence.setRoot(residence.root)

		return err
	}

	cleanupResidence.setRoot(residence.root)

	if err := handoffGeneratedNativeTree(residence.root, a.options.ProcessIsolation); err != nil {
		return fmt.Errorf("handoff Amp startup probe residence: %w", err)
	}

	// The probe environment is the static one: the ordinary or isolation base
	// the native boundary supplies, the agent-scoped WithEnv phase, the named
	// operation values the authenticated method probes need, and the
	// adapter-managed probe residence last. The session's raw PATH is absent.
	probeEnv := composeEnv(
		a.options.Env,
		operationSessionEnv(sessionEnv),
		managedSessionEnv(residence.home, residence.config, residence.cache, residence.data, residence.state),
	)

	options := amp.Options{
		CLIPath:                    a.options.ExecutablePath,
		Cwd:                        cwd,
		SettingsFile:               residence.settingsFile,
		Env:                        probeEnv,
		ResolutionEnv:              composeEnv(a.options.Env),
		MCPConfigPath:              residence.mcpFile,
		MaxLineBytes:               a.options.runtime.maxJSONLineBytes,
		OnGoroutinePanic:           a.onNativeGoroutinePanic,
		NewProcessSnapshotObserver: a.newProcessSnapshotObserver,
		WritableRoot:               residence.root,
	}
	a.configureNativeClient(&options, RuntimeResourceDiscovery)

	callbackCtx := withExactCallbackGeneration(ctx, "native:startup_probe")

	path, probeErr := invokeOwnedPair(func() (string, error) {
		return probe(callbackCtx, amp.NewClient(a.log, options))
	})
	if probeErr != nil {
		return probeErr
	}

	// The harness that just passed version and startup validation is the one
	// every later launch runs, so resolution never happens again.
	return a.retainHarnessPath(path)
}

func materializeStartupProbeResidence(parent string) (startupProbeResidence, error) {
	root, err := mkdirTemp(parent, "acp-go-amp-startup-*")
	if err != nil {
		return startupProbeResidence{}, fmt.Errorf("create Amp startup probe residence: %w", err)
	}

	residence := startupProbeResidence{
		root:         root,
		home:         filepath.Join(root, "home"),
		config:       filepath.Join(root, "xdg-config"),
		cache:        filepath.Join(root, "xdg-cache"),
		data:         filepath.Join(root, "xdg-data"),
		state:        filepath.Join(root, "xdg-state"),
		settingsFile: filepath.Join(root, "xdg-config", "amp", "settings.json"),
		mcpFile:      filepath.Join(root, "mcp.json"),
	}

	for _, path := range []string{
		residence.home,
		residence.config,
		residence.cache,
		residence.data,
		residence.state,
		filepath.Dir(residence.settingsFile),
	} {
		if err := mkdirAll(path, 0o700); err != nil {
			return residence, fmt.Errorf("create Amp startup probe isolated home: %w", err)
		}
	}

	if err := writeFile(residence.settingsFile, amp.AuthSettingsDocument(), 0o600); err != nil {
		return residence, fmt.Errorf("write Amp startup probe settings: %w", err)
	}

	if err := writeFile(residence.mcpFile, []byte("{}\n"), 0o600); err != nil {
		return residence, fmt.Errorf("write Amp startup probe MCP config: %w", err)
	}

	return residence, nil
}
