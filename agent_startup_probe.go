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
	agent     *Agent
	id        uint64
	root      string
	prepared  bool
	opaque    bool
	retryable bool
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

func (r *agentCleanupResidence) setRoot(root string) {
	r.agent.mu.Lock()
	if r.agent.cleanupResidences[r.id] == r {
		r.root = root
	}
	r.agent.mu.Unlock()
}

func (r *agentCleanupResidence) beginPrepare() {
	r.agent.mu.Lock()
	if r.agent.cleanupResidences[r.id] == r {
		r.opaque = true
	}
	r.agent.mu.Unlock()
}

func (r *agentCleanupResidence) setPrepared() {
	r.agent.mu.Lock()
	if r.agent.cleanupResidences[r.id] == r {
		r.prepared = true
		r.opaque = false
	}
	r.agent.mu.Unlock()
}

func (r *agentCleanupResidence) setRetryable() {
	r.agent.mu.Lock()
	if r.agent.cleanupResidences[r.id] == r && !r.opaque {
		r.retryable = true
	}
	r.agent.mu.Unlock()
}

func (r *agentCleanupResidence) retainsOpaqueTree() bool {
	r.agent.mu.Lock()
	defer r.agent.mu.Unlock()

	return r.agent.cleanupResidences[r.id] == r && r.opaque
}

func (r *agentCleanupResidence) finalize() error {
	r.agent.mu.Lock()
	root := r.root
	opaque := r.opaque
	r.agent.mu.Unlock()

	if opaque {
		return ErrContainmentIncomplete
	}

	if reclaimErr := r.reclaim(); reclaimErr != nil {
		return reclaimErr
	}

	var removeErr error
	if root != "" {
		removeErr = removeSessionDir(root)
	}

	if removeErr != nil {
		return removeErr
	}

	return nil
}

func (r *agentCleanupResidence) reclaim() error {
	reclaimCtx, cancel := context.WithTimeout(context.Background(), defaultNativeCommandTimeout)
	reclaimErr := r.reclaimWithContext(reclaimCtx)

	cancel()

	return reclaimErr
}

func (r *agentCleanupResidence) reclaimWithContext(ctx context.Context) error {
	r.agent.mu.Lock()
	root := r.root
	prepared := r.prepared
	r.agent.mu.Unlock()

	if !prepared || root == "" {
		return nil
	}

	reclaimErr := r.agent.reclaimNativeTree(ctx, root)
	if reclaimErr != nil {
		r.setRetryable()

		return reclaimErr
	}

	r.agent.mu.Lock()
	r.prepared = false
	r.agent.mu.Unlock()

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
		if residence.retryable {
			residences = append(residences, residence)
		}
	}
	a.mu.Unlock()

	for _, residence := range residences {
		cleanupErr := invokeShutdownStep(residence.finalize)
		if cleanupErr == nil && !residence.retainsOpaqueTree() {
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
	options := amp.Options{
		CLIPath:       a.options.ExecutablePath,
		Cwd:           cwd,
		Env:           composeEnv(a.options.Env, operationSessionEnv(sessionEnv)),
		ResolutionEnv: composeEnv(a.options.Env),
		NewProbeClient: func(probeCtx context.Context) (*amp.Client, func() error, error) {
			return a.newPreparedProbeClient(probeCtx, cwd, sessionEnv)
		},
	}
	a.configureNativeClient(&options)

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

func (a *Agent) newPreparedProbeClient(ctx context.Context, cwd string, sessionEnv map[string]string) (*amp.Client, func() error, error) {
	cleanupResidence, err := a.reserveCleanupResidence()
	if err != nil {
		return nil, nil, err
	}

	fail := func(cause error) (*amp.Client, func() error, error) {
		cleanupResidence.setRetryable()

		cleanupErr := cleanupResidence.finalize()
		if cleanupErr == nil && !cleanupResidence.retainsOpaqueTree() {
			a.clearCleanupResidence(cleanupResidence)
		}

		return nil, nil, errors.Join(cause, cleanupErr)
	}

	parent, err := a.ensureScratchParent()
	if err != nil {
		return fail(err)
	}

	residence, err := materializeStartupProbeResidence(parent)
	cleanupResidence.setRoot(residence.root)

	if err != nil {
		return fail(err)
	}

	if a.options.hostAuthoritySupplied {
		cleanupResidence.beginPrepare()
	}

	if err := a.prepareNativeTree(ctx, residence.root); err != nil {
		return fail(fmt.Errorf("prepare Amp startup probe residence: %w", err))
	}

	if a.options.hostAuthoritySupplied {
		cleanupResidence.setPrepared()
	}

	probeEnv := composeEnv(
		a.options.Env,
		operationSessionEnv(sessionEnv),
		managedSessionEnv(residence.home, residence.config, residence.cache, residence.data, residence.state),
	)
	options := amp.Options{
		CLIPath: a.options.ExecutablePath, Cwd: cwd, SettingsFile: residence.settingsFile,
		Env: probeEnv, ResolutionEnv: composeEnv(a.options.Env), MCPConfigPath: residence.mcpFile,
		MaxLineBytes: a.options.runtime.maxJSONLineBytes, OnGoroutinePanic: a.onNativeGoroutinePanic,
		WritableRoot: residence.root,
	}
	a.configureNativeClient(&options)

	cleanup := func() error {
		cleanupResidence.setRetryable()

		cleanupErr := cleanupResidence.finalize()
		if cleanupErr == nil {
			a.clearCleanupResidence(cleanupResidence)
		}

		return cleanupErr
	}

	return amp.NewClient(a.log, options), cleanup, nil
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
