//nolint:goconst,wsl_v5,nlreturn,govet // compact scaffold keeps contract strings close to their use sites.
package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

type Agent struct {
	options Options
	log     *slog.Logger
	store   SessionStore

	mu                   sync.Mutex
	closed               bool
	conn                 *acp.AgentSideConnection
	sessions             map[acp.SessionId]*agentSession
	deleted              map[acp.SessionId]struct{}
	pendingNativeDeletes map[acp.SessionId]struct{}
	pending              int
	rawSeq               atomic.Int64
	clientCalls          chan struct{}

	activeLimitErr error
}

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)
	log := options.Logger
	if log == nil {
		log = slog.Default()
	}
	store := options.SessionStore
	if store == nil {
		store = NewInMemorySessionStore()
	}
	return &Agent{
		options:              options,
		log:                  log,
		store:                store,
		sessions:             make(map[acp.SessionId]*agentSession),
		deleted:              make(map[acp.SessionId]struct{}),
		pendingNativeDeletes: make(map[acp.SessionId]struct{}),
		clientCalls:          make(chan struct{}, maxConcurrentClientCalls(options.ConcurrencyLimits)),
		activeLimitErr:       validateConcurrencyLimits(options.ConcurrencyLimits),
	}
}

func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) error {
	agent := NewAgent(opts...)
	defer func() { _ = agent.Close() }()
	conn := acp.NewAgentSideConnection(agent, output, input)
	agent.setConnection(conn)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

func (a *Agent) Close() error {
	a.mu.Lock()
	sessions := make([]*agentSession, 0, len(a.sessions))
	for _, session := range a.sessions {
		sessions = append(sessions, session)
	}
	a.sessions = map[acp.SessionId]*agentSession{}
	a.closed = true
	a.conn = nil
	a.mu.Unlock()

	var err error
	for _, session := range sessions {
		err = errors.Join(err, session.Close(context.Background()))
	}
	return err
}

func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (acp.InitializeResponse, error) {
	if a.activeLimitErr != nil {
		return acp.InitializeResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldError: a.activeLimitErr.Error()})
	}
	title := a.options.AgentTitle
	position := selectPositionEncoding(params.ClientCapabilities.PositionEncodings)
	return acp.InitializeResponse{
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			LoadSession:      true,
			McpCapabilities:  acp.McpCapabilities{Http: true},
			PositionEncoding: &position,
			PromptCapabilities: acp.PromptCapabilities{
				EmbeddedContext: true,
				Image:           true,
			},
			SessionCapabilities: acp.SessionCapabilities{
				AdditionalDirectories: &acp.SessionAdditionalDirectoriesCapabilities{},
				Close:                 &acp.SessionCloseCapabilities{},
				Delete:                &acp.SessionDeleteCapabilities{},
				List:                  &acp.SessionListCapabilities{},
				Resume:                &acp.SessionResumeCapabilities{},
			},
			Meta: map[string]any{
				ampMetaKey: map[string]any{
					"rawEvent": map[string]any{
						"method":         RawEventMethod,
						"enabledBy":      "_meta.amp.rawEvent.enabled",
						"maxBytes":       rawEventMaxBytes,
						"defaultEnabled": false,
					},
					"sessionStore": map[string]any{
						"format": SessionStoreFormat,
						"key":    []string{jsonFieldSessionID, "subpath"},
					},
				},
			},
		},
	}, nil
}

func (a *Agent) Authenticate(ctx context.Context, params acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

func (a *Agent) Logout(ctx context.Context, params acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, nil
}

func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	meta, err := parseSessionMeta(params.Meta)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}
	if err := a.validateSessionStartOptions(meta.options); err != nil {
		return acp.NewSessionResponse{}, err
	}
	if err := validateSessionPaths(params.Cwd, params.AdditionalDirectories); err != nil {
		return acp.NewSessionResponse{}, err
	}
	mcpConfig, err := mcpConfigJSON(params.McpServers)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}
	if err := a.ensureStartup(ctx, params.Cwd, meta); err != nil {
		return acp.NewSessionResponse{}, err
	}
	if err := a.reserveSessionSlot(); err != nil {
		return acp.NewSessionResponse{}, err
	}

	probeSession, err := newAgentSession(a, "", params.Cwd, meta, mcpConfig, params.AdditionalDirectories)
	if err != nil {
		a.releaseSessionSlot("")
		return acp.NewSessionResponse{}, err
	}
	threadID, err := probeSession.client().NewThread(ctx)
	if err != nil {
		a.releaseSessionSlot("")
		_ = probeSession.Close(context.Background())
		return acp.NewSessionResponse{}, acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	}
	probeSession.id = acp.SessionId(threadID)
	if err := probeSession.persistAfterTurn(ctx, nil); err != nil {
		a.releaseSessionSlot("")
		_ = probeSession.Delete(context.Background())
		return acp.NewSessionResponse{}, err
	}

	a.mu.Lock()
	a.sessions[probeSession.id] = probeSession
	a.pending--
	a.mu.Unlock()
	return acp.NewSessionResponse{SessionId: probeSession.id, ConfigOptions: probeSession.configOptions()}, nil
}

func (a *Agent) LoadSession(ctx context.Context, params acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	session, err := a.loadOrResume(ctx, params.SessionId, params.Cwd, params.McpServers, params.AdditionalDirectories, params.Meta)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}
	if err := session.replayTranscript(ctx); err != nil {
		return acp.LoadSessionResponse{}, err
	}
	return acp.LoadSessionResponse{ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	session, err := a.loadOrResume(ctx, params.SessionId, params.Cwd, params.McpServers, params.AdditionalDirectories, params.Meta)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}
	return acp.ResumeSessionResponse{ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	a.retryPendingNativeDeletes(ctx)
	summaries, err := a.store.ListSessions(ctx)
	if err != nil {
		return acp.ListSessionsResponse{}, err
	}
	infos := make([]acp.SessionInfo, 0, len(summaries))
	for _, summary := range summaries {
		if _, deleted := a.isDeleted(acp.SessionId(summary.SessionID)); deleted {
			continue
		}
		if params.Cwd != nil && *params.Cwd != "" && summary.Cwd != *params.Cwd {
			continue
		}
		updated := millisToRFC3339(summary.UpdatedAtUnixMilli)
		title := summary.Title
		infos = append(infos, acp.SessionInfo{
			SessionId: acp.SessionId(summary.SessionID),
			Cwd:       summary.Cwd,
			Title:     &title,
			UpdatedAt: &updated,
			Meta:      summary.Meta,
		})
	}
	return acp.ListSessionsResponse{Sessions: infos}, nil
}

func (a *Agent) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	return session.Prompt(ctx, params)
}

func (a *Agent) Cancel(ctx context.Context, params acp.CancelNotification) error {
	session, err := a.session(params.SessionId)
	if err != nil {
		if errors.Is(err, errSessionDeleted) {
			return nil
		}
		return err
	}
	return session.Cancel(ctx)
}

func (a *Agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	session, err := a.session(params.SessionId)
	if err != nil {
		if errors.Is(err, errSessionDeleted) {
			return acp.CloseSessionResponse{}, nil
		}
		return acp.CloseSessionResponse{}, err
	}
	err = session.Close(ctx)
	a.mu.Lock()
	delete(a.sessions, params.SessionId)
	a.mu.Unlock()
	return acp.CloseSessionResponse{}, err
}

func (a *Agent) UnstableDeleteSession(ctx context.Context, params acp.UnstableDeleteSessionRequest) (acp.UnstableDeleteSessionResponse, error) {
	if params.SessionId == "" {
		return acp.UnstableDeleteSessionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldField: jsonFieldSessionID})
	}
	a.retryPendingNativeDeletes(ctx)
	if a.store != nil {
		if err := a.store.Delete(ctx, SessionKey{SessionID: string(params.SessionId), Subpath: SessionStoreMainSubpath}); err != nil {
			return acp.UnstableDeleteSessionResponse{}, err
		}
	}
	a.markDeleted(params.SessionId)
	a.mu.Lock()
	session := a.sessions[params.SessionId]
	delete(a.sessions, params.SessionId)
	a.mu.Unlock()
	if err := a.deleteNativeThread(ctx, params.SessionId, session); err != nil {
		a.markPendingNativeDelete(params.SessionId)
		return acp.UnstableDeleteSessionResponse{}, err
	}
	a.clearPendingNativeDelete(params.SessionId)
	return acp.UnstableDeleteSessionResponse{}, nil
}

func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	if params.Boolean != nil {
		return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldField: "value", jsonFieldError: "boolean config options are unsupported"})
	}
	if params.ValueId == nil {
		return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldField: "value"})
	}
	session, err := a.session(params.ValueId.SessionId)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}
	if err := session.setConfig(ctx, params.ValueId.ConfigId, params.ValueId.Value); err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}
	return acp.SetSessionConfigOptionResponse{ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) SetSessionMode(ctx context.Context, params acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case ForkSessionMethod:
		return acp.UnstableForkSessionResponse{}, acp.NewInvalidParams(map[string]any{
			jsonFieldError: "unsupported",
			jsonFieldField: ForkSessionMethod,
		})
	default:
		return nil, acp.NewMethodNotFound(method)
	}
}

func (a *Agent) loadOrResume(ctx context.Context, sessionID acp.SessionId, cwd string, mcpServers []acp.McpServer, additionalDirs []string, rawMeta map[string]any) (*agentSession, error) {
	a.retryPendingNativeDeletes(ctx)
	if _, deleted := a.isDeleted(sessionID); deleted {
		return nil, errSessionDeleted
	}

	// X1: validate the full request identically to the cold path FIRST, so an
	// already-active session cannot bypass strict _meta, cwd/additional-dir,
	// MCP transport, and model/mode/effort validation. Only after validation
	// succeeds may an active session be reused.
	meta, err := parseSessionMeta(rawMeta)
	if err != nil {
		return nil, err
	}
	if err := a.validateSessionStartOptions(meta.options); err != nil {
		return nil, err
	}
	if err := validateSessionPaths(cwd, additionalDirs); err != nil {
		return nil, err
	}
	mcpConfig, err := mcpConfigJSON(mcpServers)
	if err != nil {
		return nil, err
	}
	if err := a.ensureStartup(ctx, cwd, meta); err != nil {
		return nil, err
	}

	a.mu.Lock()
	if session := a.sessions[sessionID]; session != nil {
		a.mu.Unlock()
		if err := session.applyActiveRequest(meta, cwd, mcpConfig, additionalDirs); err != nil {
			return nil, err
		}
		if err := session.ensureMirrorSynced(ctx); err != nil {
			return nil, err
		}
		if err := session.verifyContinuable(ctx); err != nil {
			return nil, err
		}
		return session, nil
	}
	a.mu.Unlock()

	manifest, err := a.loadManifest(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if meta.options.Mode == "" {
		meta.options.Mode = manifest.Mode
	}
	if meta.options.Effort == "" {
		meta.options.Effort = manifest.Effort
	}
	session, err := newAgentSession(a, sessionID, cwd, meta, mcpConfig, additionalDirs)
	if err != nil {
		return nil, err
	}
	session.title = manifest.Title
	session.createdUnix = manifest.CreatedAtUnixMilli
	session.updatedUnix = manifest.UpdatedAtUnixMilli
	if err := session.verifyContinuable(ctx); err != nil {
		_ = session.Close(context.Background())
		return nil, err
	}

	a.mu.Lock()
	if len(a.sessions) >= a.maxActiveSessions() {
		a.mu.Unlock()
		_ = session.Close(context.Background())
		return nil, backpressureError("active_sessions")
	}
	a.sessions[sessionID] = session
	a.mu.Unlock()
	return session, nil
}

func (a *Agent) loadManifest(ctx context.Context, sessionID acp.SessionId) (ampManifest, error) {
	entries, err := a.store.Load(ctx, SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	if err != nil {
		return ampManifest{}, err
	}
	if len(entries) == 0 {
		return ampManifest{}, acp.NewInvalidParams(map[string]any{jsonFieldField: jsonFieldSessionID, jsonFieldError: "unknown session"})
	}
	var manifest ampManifest
	if err := json.Unmarshal(entries[len(entries)-1], &manifest); err != nil {
		return ampManifest{}, acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	}
	if manifest.Format != SessionStoreFormat || manifest.ThreadID != string(sessionID) {
		return ampManifest{}, acp.NewInternalError(map[string]any{jsonFieldError: "invalid amp session manifest"})
	}
	return manifest, nil
}

func (s *agentSession) applyActiveRequest(meta parsedSessionMeta, cwd string, mcpConfig string, additionalDirs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cwd != cwd {
		return mismatchField("cwd")
	}
	if !slices.Equal(s.additionalDirectories, additionalDirs) {
		return mismatchField("additionalDirectories")
	}
	if s.mcpConfigJSON != mcpConfig {
		return mismatchField("mcpServers")
	}
	if !maps.Equal(activeRequestEnv(s.env), activeRequestEnv(mergeEnv(s.agent.options.Env, meta.options.Env))) {
		return mismatchField("env")
	}
	if meta.optionFields.mode && s.mode != meta.options.Mode {
		return mismatchField("mode")
	}
	if meta.optionFields.effort && s.effort != meta.options.Effort {
		return mismatchField("effort")
	}
	if meta.rawEventField {
		s.rawEvents = meta.rawEvent
	}
	return nil
}

func activeRequestEnv(env map[string]string) map[string]string {
	out := cloneStringMap(env)
	for _, key := range []string{"HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"} {
		delete(out, key)
	}
	return out
}

func mismatchField(field string) error {
	return acp.NewInvalidParams(map[string]any{jsonFieldError: "mismatch", jsonFieldField: field})
}

func (a *Agent) validateSessionStartOptions(options AmpOptions) error {
	if a.options.DefaultModel != "" {
		return unsupportedField("model")
	}
	if options.Model != "" {
		return unsupportedField("model")
	}
	if options.OutputSchema != nil {
		return unsupportedField("outputSchema")
	}
	if options.Mode != "" && !slices.Contains(validModes(), options.Mode) {
		return acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options.mode"})
	}
	if options.Effort != "" && !slices.Contains(validEfforts(), options.Effort) {
		return acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options.effort"})
	}
	return nil
}

func (a *Agent) reserveSessionSlot() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: "agent closed"})
	}
	if len(a.sessions)+a.pending >= a.maxActiveSessions() {
		return backpressureError("active_sessions")
	}
	a.pending++
	return nil
}

func (a *Agent) releaseSessionSlot(acp.SessionId) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pending > 0 {
		a.pending--
	}
}

func (a *Agent) session(id acp.SessionId) (*agentSession, error) {
	if _, deleted := a.isDeleted(id); deleted {
		return nil, errSessionDeleted
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	session := a.sessions[id]
	if session == nil {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldField: jsonFieldSessionID, jsonFieldError: "unknown session"})
	}
	return session, nil
}

func (a *Agent) markDeleted(id acp.SessionId) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deleted[id] = struct{}{}
}

func (a *Agent) isDeleted(id acp.SessionId) (struct{}, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	value, ok := a.deleted[id]
	return value, ok
}

func (a *Agent) markPendingNativeDelete(id acp.SessionId) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pendingNativeDeletes[id] = struct{}{}
}

func (a *Agent) clearPendingNativeDelete(id acp.SessionId) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.pendingNativeDeletes, id)
}

func (a *Agent) pendingNativeDeleteIDs() []acp.SessionId {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]acp.SessionId, 0, len(a.pendingNativeDeletes))
	for id := range a.pendingNativeDeletes {
		ids = append(ids, id)
	}
	return ids
}

func (a *Agent) retryPendingNativeDeletes(ctx context.Context) {
	for _, id := range a.pendingNativeDeleteIDs() {
		if err := a.deleteNativeThread(ctx, id, nil); err != nil {
			a.log.DebugContext(ctx, "retry amp native delete failed", slog.String(jsonFieldSessionID, string(id)), slog.String(jsonFieldError, err.Error()))
			continue
		}
		a.clearPendingNativeDelete(id)
	}
}

func (a *Agent) deleteNativeThread(ctx context.Context, id acp.SessionId, session *agentSession) error {
	if session != nil {
		return session.Delete(ctx)
	}
	tmp, err := newAgentSession(a, id, "", parsedSessionMeta{}, "", nil)
	if err != nil {
		return err
	}
	defer func() { _ = tmp.Close(context.Background()) }()
	return tmp.client().DeleteThread(ctx, string(id))
}

func (a *Agent) setConnection(conn *acp.AgentSideConnection) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.conn = conn
}

func (a *Agent) connection() *acp.AgentSideConnection {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.conn
}

func (a *Agent) acquireClientCall(ctx context.Context) (func(), error) {
	select {
	case a.clientCalls <- struct{}{}:
		return func() { <-a.clientCalls }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, backpressureError("client_calls")
	}
}

func (a *Agent) nextRawEventSequence() int64 {
	return a.rawSeq.Add(1)
}

func (a *Agent) settingsParent() string {
	if a.options.Home != "" {
		return a.options.Home
	}
	return ""
}

func (a *Agent) ensureStartup(ctx context.Context, cwd string, meta parsedSessionMeta) error {
	env := mergeEnv(a.options.Env, meta.options.Env)
	client := amp.NewClient(a.log, amp.Options{
		CLIPath:      a.options.ExecutablePath,
		Cwd:          cwd,
		Env:          env,
		MaxLineBytes: a.options.runtime.maxJSONLineBytes,
	})
	if err := client.StartupProbe(ctx); err != nil {
		return acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	}
	return nil
}

func (a *Agent) maxActiveSessions() int {
	if a.options.ConcurrencyLimits.MaxActiveSessions > 0 {
		return a.options.ConcurrencyLimits.MaxActiveSessions
	}
	return defaultMaxActiveSessions
}

func (a *Agent) maxConcurrentPrompts() int {
	if a.options.ConcurrencyLimits.MaxConcurrentPrompts > 0 {
		return a.options.ConcurrencyLimits.MaxConcurrentPrompts
	}
	return defaultMaxConcurrentPrompts
}

func maxConcurrentClientCalls(limits ConcurrencyLimits) int {
	if limits.MaxConcurrentClientCalls > 0 {
		return limits.MaxConcurrentClientCalls
	}
	return defaultMaxConcurrentCalls
}

func parseSessionMeta(meta map[string]any) (parsedSessionMeta, error) {
	result := parsedSessionMeta{}
	for key, value := range meta {
		switch key {
		case ampMetaKey:
			ampMeta, ok := value.(map[string]any)
			if !ok {
				return result, unsupportedField("_meta.amp")
			}
			for ampKey, ampValue := range ampMeta {
				switch ampKey {
				case "options":
					options, fields, err := parseAmpOptionsWithPresence(ampValue)
					if err != nil {
						return result, err
					}
					result.options = options
					result.optionFields = fields
				case "rawEvent":
					enabled, err := parseRawEventMeta(ampValue)
					if err != nil {
						return result, err
					}
					result.rawEvent = enabled
					result.rawEventField = true
				default:
					return result, unsupportedField("_meta.amp." + ampKey)
				}
			}
		case "traceparent", "tracestate", "baggage":
		default:
		}
	}
	return result, nil
}

func parseAmpOptions(value any) (AmpOptions, error) {
	options, _, err := parseAmpOptionsWithPresence(value)
	return options, err
}

func parseAmpOptionsWithPresence(value any) (AmpOptions, ampOptionFields, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return AmpOptions{}, ampOptionFields{}, unsupportedField("_meta.amp.options")
	}
	options := AmpOptions{}
	fields := ampOptionFields{}
	for key, value := range raw {
		switch key {
		case "model":
			model, ok := value.(string)
			if !ok {
				return options, fields, unsupportedField("_meta.amp.options.model")
			}
			options.Model = model
		case "env":
			fields.env = true
			switch env := value.(type) {
			case map[string]any:
				options.Env = map[string]string{}
				for k, v := range env {
					str, ok := v.(string)
					if !ok {
						return options, fields, unsupportedField("_meta.amp.options.env." + k)
					}
					options.Env[k] = str
				}
			case map[string]string:
				options.Env = cloneStringMap(env)
			default:
				return options, fields, unsupportedField("_meta.amp.options.env")
			}
		case "outputSchema":
			return options, fields, unsupportedField("_meta.amp.options.outputSchema")
		case "mode":
			fields.mode = true
			mode, ok := value.(string)
			if !ok {
				return options, fields, unsupportedField("_meta.amp.options.mode")
			}
			options.Mode = mode
		case "effort":
			fields.effort = true
			effort, ok := value.(string)
			if !ok {
				return options, fields, unsupportedField("_meta.amp.options.effort")
			}
			options.Effort = effort
		default:
			return options, fields, unsupportedField("_meta.amp.options." + key)
		}
	}
	return options, fields, nil
}

func parseRawEventMeta(value any) (bool, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return false, unsupportedField("_meta.amp.rawEvent")
	}
	enabled := false
	for key, value := range raw {
		switch key {
		case "enabled":
			parsed, ok := value.(bool)
			if !ok {
				return false, unsupportedField("_meta.amp.rawEvent.enabled")
			}
			enabled = parsed
		default:
			return false, unsupportedField("_meta.amp.rawEvent." + key)
		}
	}
	return enabled, nil
}

func unsupportedField(path string) error {
	return acp.NewInvalidParams(map[string]any{jsonFieldError: "unsupported", jsonFieldField: path})
}

func validateSessionPaths(cwd string, additionalDirs []string) error {
	if cwd == "" || !filepath.IsAbs(cwd) {
		return acp.NewInvalidParams(map[string]any{jsonFieldField: "cwd"})
	}
	for i, dir := range additionalDirs {
		if dir == "" || !filepath.IsAbs(dir) {
			return acp.NewInvalidParams(map[string]any{jsonFieldField: fmt.Sprintf("additionalDirectories[%d]", i)})
		}
	}
	return nil
}

func mcpConfigJSON(servers []acp.McpServer) (string, error) {
	if len(servers) == 0 {
		return "", nil
	}
	payload := map[string]any{}
	for i, server := range servers {
		switch {
		case server.Stdio != nil:
			if server.Stdio.Name == "" {
				return "", acp.NewInvalidParams(map[string]any{jsonFieldField: fmt.Sprintf("mcpServers[%d].name", i)})
			}
			spec := map[string]any{"command": server.Stdio.Command}
			if len(server.Stdio.Args) > 0 {
				spec["args"] = server.Stdio.Args
			}
			if len(server.Stdio.Env) > 0 {
				env := map[string]string{}
				for _, item := range server.Stdio.Env {
					env[item.Name] = item.Value
				}
				spec["env"] = env
			}
			payload[server.Stdio.Name] = spec
		case server.Http != nil:
			if server.Http.Name == "" {
				return "", acp.NewInvalidParams(map[string]any{jsonFieldField: fmt.Sprintf("mcpServers[%d].name", i)})
			}
			spec := map[string]any{"url": server.Http.Url}
			if len(server.Http.Headers) > 0 {
				headers := map[string]string{}
				for _, item := range server.Http.Headers {
					headers[item.Name] = item.Value
				}
				spec["headers"] = headers
			}
			payload[server.Http.Name] = spec
		case server.Sse != nil:
			return "", acp.NewInvalidParams(map[string]any{jsonFieldField: fmt.Sprintf("mcpServers[%d]", i), jsonFieldError: "unsupported mcp transport: sse"})
		case server.Acp != nil:
			return "", acp.NewInvalidParams(map[string]any{jsonFieldField: fmt.Sprintf("mcpServers[%d]", i), jsonFieldError: "unsupported mcp transport: acp"})
		default:
			return "", acp.NewInvalidParams(map[string]any{jsonFieldField: fmt.Sprintf("mcpServers[%d]", i)})
		}
	}
	data, _ := json.Marshal(payload)
	return string(data), nil
}

func (s *agentSession) replayTranscript(ctx context.Context) error {
	if s.agent.store == nil {
		return nil
	}
	entries, err := s.agent.store.Load(ctx, SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		msg, err := amp.ParseJSONLine(entry)
		if err != nil {
			return err
		}
		if err := s.emitMessage(ctx, msg); err != nil {
			return err
		}
		if err := s.emitRawEvent(ctx, "store-replay", msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *agentSession) configOptions() []acp.SessionConfigOption {
	modeCategory := acp.SessionConfigOptionCategoryMode
	effortCategory := acp.SessionConfigOptionCategoryThoughtLevel
	return []acp.SessionConfigOption{
		selectConfig(configMode, "Mode", modeCategory, s.mode, validModes()),
		selectConfig(configEffort, "Effort", effortCategory, s.effort, validEfforts()),
	}
}

func (s *agentSession) setConfig(ctx context.Context, id acp.SessionConfigId, value acp.SessionConfigValueId) error {
	s.mu.Lock()
	switch id {
	case configMode:
		if !slices.Contains(validModes(), string(value)) {
			s.mu.Unlock()
			return acp.NewInvalidParams(map[string]any{jsonFieldField: "value"})
		}
		s.mode = string(value)
	case configEffort:
		if !slices.Contains(validEfforts(), string(value)) {
			s.mu.Unlock()
			return acp.NewInvalidParams(map[string]any{jsonFieldField: "value"})
		}
		s.effort = string(value)
	default:
		s.mu.Unlock()
		return acp.NewInvalidParams(map[string]any{jsonFieldField: "configId"})
	}
	s.updatedUnix = time.Now().UnixMilli()
	s.mu.Unlock()
	if err := s.persistAfterTurn(ctx, nil); err != nil {
		return err
	}
	return s.emitUpdate(ctx, acp.SessionUpdate{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{
		SessionUpdate: "config_option_update",
		ConfigOptions: s.configOptions(),
	}})
}

func selectConfig(id acp.SessionConfigId, name string, category acp.SessionConfigOptionCategory, current string, values []string) acp.SessionConfigOption {
	opts := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(values))
	for _, value := range values {
		opts = append(opts, acp.SessionConfigSelectOption{Name: value, Value: acp.SessionConfigValueId(value)})
	}
	return acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
		Id:           id,
		Name:         name,
		Type:         configTypeSelect,
		Category:     &category,
		CurrentValue: acp.SessionConfigValueId(current),
		Options:      acp.SessionConfigSelectOptions{Ungrouped: &opts},
	}}
}

func validModes() []string {
	return []string{"smart", "deep", "rush"}
}

func validEfforts() []string {
	return []string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}
}

func validateConcurrencyLimits(limits ConcurrencyLimits) error {
	switch {
	case limits.MaxActiveSessions < 0:
		return errors.New("max active sessions must be non-negative")
	case limits.MaxConcurrentPrompts < 0:
		return errors.New("max concurrent prompts must be non-negative")
	case limits.MaxConcurrentPrompts > 1:
		return errors.New("MaxConcurrentPrompts must be 1: amp thread turns are inherently serial server-side")
	case limits.MaxConcurrentClientCalls < 0:
		return errors.New("max concurrent client calls must be non-negative")
	default:
		return nil
	}
}

func backpressureError(limit string) error {
	return acp.NewInvalidRequest(map[string]any{jsonFieldError: "backpressure", "limit": limit})
}

func selectPositionEncoding(values []acp.PositionEncodingKind) acp.PositionEncodingKind {
	if slices.Contains(values, acp.PositionEncodingKindUtf8) {
		return acp.PositionEncodingKindUtf8
	}
	return acp.PositionEncodingKindUtf16
}

func millisToRFC3339(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}
