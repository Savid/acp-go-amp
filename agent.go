//nolint:goconst,wsl_v5,nlreturn,govet // compact scaffold keeps contract strings close to their use sites.
package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

	mu       sync.Mutex
	closed   bool
	conn     *acp.AgentSideConnection
	sessions map[acp.SessionId]*agentSession
	deleted  map[acp.SessionId]struct{}
	pending  int
	rawSeq   atomic.Int64

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
		options:        options,
		log:            log,
		store:          store,
		sessions:       make(map[acp.SessionId]*agentSession),
		deleted:        make(map[acp.SessionId]struct{}),
		activeLimitErr: validateConcurrencyLimits(options.ConcurrencyLimits),
	}
}

func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) error {
	agent := NewAgent(opts...)
	defer func() { _ = agent.Close() }()
	conn := acp.NewAgentSideConnection(agent, output, input)
	conn.SetLogger(agent.log)
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
	mcpConfig, err := mcpConfigJSON(params.McpServers)
	if err != nil {
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
	a.markDeleted(params.SessionId)
	if a.store != nil {
		if err := a.store.Delete(ctx, SessionKey{SessionID: string(params.SessionId), Subpath: SessionStoreMainSubpath}); err != nil {
			return acp.UnstableDeleteSessionResponse{}, err
		}
	}
	a.mu.Lock()
	session := a.sessions[params.SessionId]
	delete(a.sessions, params.SessionId)
	a.mu.Unlock()
	var err error
	if session != nil {
		err = session.Delete(ctx)
	} else {
		tmp, tmpErr := newAgentSession(a, params.SessionId, "", parsedSessionMeta{}, "", nil)
		if tmpErr == nil {
			err = tmp.client().DeleteThread(ctx, string(params.SessionId))
			_ = tmp.Close(context.Background())
		}
	}
	return acp.UnstableDeleteSessionResponse{}, err
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
	return acp.SetSessionConfigOptionResponse{}, nil
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
	if _, deleted := a.isDeleted(sessionID); deleted {
		return nil, errSessionDeleted
	}
	a.mu.Lock()
	if session := a.sessions[sessionID]; session != nil {
		a.mu.Unlock()
		return session, nil
	}
	a.mu.Unlock()

	manifest, err := a.loadManifest(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	meta, err := parseSessionMeta(rawMeta)
	if err != nil {
		return nil, err
	}
	if err := a.validateSessionStartOptions(meta.options); err != nil {
		return nil, err
	}
	if cwd == "" {
		cwd = manifest.Cwd
	}
	if len(additionalDirs) == 0 {
		additionalDirs = manifest.AdditionalDirs
	}
	mcpConfig, err := mcpConfigJSON(mcpServers)
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

func (a *Agent) validateSessionStartOptions(options AmpOptions) error {
	if a.options.DefaultModel != "" {
		return acp.NewInvalidParams(map[string]any{jsonFieldError: "unsupported", jsonFieldField: "model"})
	}
	if options.Model != "" {
		return acp.NewInvalidParams(map[string]any{jsonFieldError: "unsupported", jsonFieldField: "model"})
	}
	if len(options.OutputSchema) > 0 {
		return acp.NewInvalidParams(map[string]any{jsonFieldError: "unsupported", jsonFieldField: "outputSchema"})
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

func (a *Agent) nextRawEventSequence() int64 {
	return a.rawSeq.Add(1)
}

func (a *Agent) settingsParent() string {
	if a.options.Home != "" {
		return a.options.Home
	}
	return ""
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

func parseSessionMeta(meta map[string]any) (parsedSessionMeta, error) {
	result := parsedSessionMeta{}
	for key, value := range meta {
		switch key {
		case ampMetaKey:
			ampMeta, ok := value.(map[string]any)
			if !ok {
				return result, acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp"})
			}
			for ampKey, ampValue := range ampMeta {
				switch ampKey {
				case "options":
					options, err := parseAmpOptions(ampValue)
					if err != nil {
						return result, err
					}
					result.options = options
				case "rawEvent":
					raw, _ := ampValue.(map[string]any)
					enabled, _ := raw["enabled"].(bool)
					result.rawEvent = enabled
				default:
					return result, acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp." + ampKey})
				}
			}
		case "traceparent", "tracestate", "baggage":
		default:
		}
	}
	return result, nil
}

func parseAmpOptions(value any) (AmpOptions, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return AmpOptions{}, acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options"})
	}
	options := AmpOptions{}
	for key, value := range raw {
		switch key {
		case "model":
			options.Model, _ = value.(string)
		case "env":
			env, ok := value.(map[string]any)
			if !ok {
				return options, acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options.env"})
			}
			options.Env = map[string]string{}
			for k, v := range env {
				str, ok := v.(string)
				if !ok {
					return options, acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options.env." + k})
				}
				options.Env[k] = str
			}
		case "outputSchema":
			options.OutputSchema, _ = value.(map[string]any)
		case "mode":
			options.Mode, _ = value.(string)
		case "effort":
			options.Effort, _ = value.(string)
		default:
			return options, acp.NewInvalidParams(map[string]any{jsonFieldField: "_meta.amp.options." + key})
		}
	}
	return options, nil
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
