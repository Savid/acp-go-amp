package ampacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (resp acp.NewSessionResponse, err error) {
	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionNew)
	defer func() { finish(err) }()

	ctx, finishLifecycle, err := a.beginLifecycleOperation(ctx)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}
	defer func() { finishLifecycle(err) }()

	ctx = a.observe.Extract(ctx, params.Meta)

	meta, err := parseSessionMeta(params.Meta)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	if optErr := a.validateSessionStartOptions(meta.options); optErr != nil {
		return acp.NewSessionResponse{}, optErr
	}

	if pathErr := validateSessionPaths(params.Cwd, params.AdditionalDirectories); pathErr != nil {
		return acp.NewSessionResponse{}, pathErr
	}

	mcpConfig, err := mcpConfigJSON(params.McpServers)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	discoveryRelease, err := acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	discoveryScratchRelease, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
	if err != nil {
		discoveryRelease()

		return acp.NewSessionResponse{}, err
	}

	readinessStarted := time.Now()
	startErr := a.ensureStartup(ctx, params.Cwd, meta)
	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery, RuntimeStartupReadiness, readinessStarted, startErr)

	releaseNativeRootWhenQuiescent(discoveryRelease, startErr)
	releaseScratchRootWhenQuiescent(discoveryScratchRelease, startErr)

	if startErr != nil {
		return acp.NewSessionResponse{}, startErr
	}

	if slotErr := a.reserveSessionSlot(); slotErr != nil {
		return acp.NewSessionResponse{}, slotErr
	}

	probeSession, err := newAgentSession(ctx, a, "", params.Cwd, meta, mcpConfig, params.AdditionalDirectories)
	if err != nil {
		a.releaseSessionSlot("")

		return acp.NewSessionResponse{}, err
	}

	nativeRelease, err := acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession)
	if err != nil {
		a.releaseSessionSlot("")

		closeErr := probeSession.Close(context.Background())

		return acp.NewSessionResponse{}, errors.Join(err, closeErr)
	}

	// Amp thread creation is an authenticated remote operation and has been
	// observed taking close to a minute during healthy startup. Give it its own
	// generous hard bound so an unbounded ACP request can never leave an
	// interactive or stalled `threads new` process alive indefinitely.
	sessionCtx, cancelSession := context.WithTimeout(ctx, a.options.runtime.nativeSessionTimeout)
	sessionStarted := time.Now()
	threadID, err := a.options.runtime.newThread(sessionCtx, probeSession.client())

	cancelSession()
	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceSession, RuntimeStartupSession, sessionStarted, err)

	releaseNativeRootWhenQuiescent(nativeRelease, err)

	if err != nil {
		a.releaseSessionSlot("")
		probeSession.recordScratchQuiescence(err)
		closeErr := probeSession.Close(context.Background())

		return acp.NewSessionResponse{}, errors.Join(nativeInternalError(err), closeErr)
	}

	probeSession.id = acp.SessionId(threadID)
	if persistErr := probeSession.persistAfterTurn(ctx, nil); persistErr != nil {
		a.releaseSessionSlot("")

		deleteErr := probeSession.Delete(context.Background())

		return acp.NewSessionResponse{}, errors.Join(persistErr, deleteErr)
	}

	a.mu.Lock()
	a.sessions[probeSession.id] = probeSession
	a.pending--
	a.mu.Unlock()
	a.observe.AddActiveSession(ctx, 1)

	return acp.NewSessionResponse{SessionId: probeSession.id, ConfigOptions: probeSession.configOptions()}, nil
}

func (a *Agent) LoadSession(ctx context.Context, params acp.LoadSessionRequest) (resp acp.LoadSessionResponse, err error) {
	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionLoad)
	defer func() { finish(err) }()

	ctx, finishLifecycle, err := a.beginLifecycleOperation(ctx)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}
	defer func() { finishLifecycle(err) }()

	ctx = a.observe.Extract(ctx, params.Meta)

	session, transcript, started, err := a.loadOrResume(ctx, params.SessionId, params.Cwd, params.McpServers, params.AdditionalDirectories, params.Meta)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}

	if replayErr := session.replayTranscriptEntries(ctx, transcript); replayErr != nil {
		var cleanupErr error
		if started {
			cleanupErr = a.removeSession(ctx, params.SessionId, session)
		}

		return acp.LoadSessionResponse{}, errors.Join(replayErr, cleanupErr)
	}

	return acp.LoadSessionResponse{ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (resp acp.ResumeSessionResponse, err error) {
	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionResume)
	defer func() { finish(err) }()

	ctx, finishLifecycle, err := a.beginLifecycleOperation(ctx)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}
	defer func() { finishLifecycle(err) }()

	ctx = a.observe.Extract(ctx, params.Meta)

	session, transcript, started, err := a.loadOrResume(ctx, params.SessionId, params.Cwd, params.McpServers, params.AdditionalDirectories, params.Meta)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	messageID, identityErr := terminalAssistantMessageIdentity(params.SessionId, transcript)
	if identityErr != nil {
		var cleanupErr error
		if started {
			cleanupErr = a.removeSession(ctx, params.SessionId, session)
		}

		return acp.ResumeSessionResponse{}, errors.Join(identityErr, cleanupErr)
	}

	if emitErr := session.emitNativeMessageIdentity(ctx, messageID); emitErr != nil {
		var cleanupErr error
		if started {
			cleanupErr = a.removeSession(ctx, params.SessionId, session)
		}

		return acp.ResumeSessionResponse{}, errors.Join(emitErr, cleanupErr)
	}

	return acp.ResumeSessionResponse{ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (resp acp.ListSessionsResponse, err error) {
	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionList)
	defer func() { finish(err) }()

	ctx, finishLifecycle, err := a.beginLifecycleOperation(ctx)
	if err != nil {
		return acp.ListSessionsResponse{}, err
	}
	defer func() { finishLifecycle(err) }()

	if pathErr := validateOptionalAbsolutePath("cwd", params.Cwd); pathErr != nil {
		return acp.ListSessionsResponse{}, pathErr
	}

	if retryErr := a.retryPendingNativeDeletes(ctx); retryErr != nil {
		return acp.ListSessionsResponse{}, retryErr
	}

	a.mu.Lock()

	active := make([]*agentSession, 0, len(a.sessions))
	for _, session := range a.sessions {
		if params.Cwd != nil && *params.Cwd != "" && session.cwd != *params.Cwd {
			continue
		}

		active = append(active, session)
	}
	a.mu.Unlock()

	infos := make([]acp.SessionInfo, 0, len(active))
	seen := make(map[acp.SessionId]struct{}, len(active))

	for _, session := range active {
		info := session.sessionInfo()
		infos = append(infos, info)
		seen[info.SessionId] = struct{}{}
	}

	listCtx, cancelList := a.sessionStoreLoadContext(ctx)
	summaries, err := a.store.ListSessions(listCtx)

	cancelList()

	if err != nil {
		return acp.ListSessionsResponse{}, err
	}

	for _, summary := range summaries {
		id := acp.SessionId(summary.SessionID)
		if _, ok := seen[id]; ok {
			continue
		}

		if _, deleted := a.isDeleted(id); deleted {
			continue
		}
		// A summary without a recorded cwd survives the cwd filter: dropping it
		// would hide restorable sessions from hosts that always filter.
		if params.Cwd != nil && *params.Cwd != "" && summary.Cwd != "" && summary.Cwd != *params.Cwd {
			continue
		}

		updated := millisToRFC3339(summary.UpdatedAtUnixMilli)
		title := summary.Title
		infos = append(infos, acp.SessionInfo{
			SessionId: id,
			Cwd:       summary.Cwd,
			Title:     &title,
			UpdatedAt: &updated,
			Meta:      summary.Meta,
		})
		seen[id] = struct{}{}
	}

	slices.SortStableFunc(infos, compareSessionInfos)

	paged, next, pageErr := paginateSessionInfos(infos, params.Cursor)
	if pageErr != nil {
		return acp.ListSessionsResponse{}, pageErr
	}

	return acp.ListSessionsResponse{Sessions: paged, NextCursor: next}, nil
}

// compareSessionInfos orders merged session infos newest UpdatedAt first, then
// by SessionId, so cursor pagination walks a deterministic sequence.
func compareSessionInfos(left, right acp.SessionInfo) int {
	l := ""
	if left.UpdatedAt != nil {
		l = *left.UpdatedAt
	}

	r := ""
	if right.UpdatedAt != nil {
		r = *right.UpdatedAt
	}

	if byTime := strings.Compare(r, l); byTime != 0 {
		return byTime
	}

	return strings.Compare(string(left.SessionId), string(right.SessionId))
}

// listSessionsPageSize is the fixed session/list page size; a page that fills
// completely emits a NextCursor for the next offset.
const listSessionsPageSize = 50

// paginateSessionInfos applies the opaque offset cursor: an undecodable cursor
// or one pointing past the end is invalid params, and a full page emits the
// next offset as a base64 RawURL cursor.
func paginateSessionInfos(sessions []acp.SessionInfo, cursor *string) ([]acp.SessionInfo, *string, error) {
	offset, err := decodeListCursor(cursor)
	if err != nil {
		return nil, nil, acp.NewInvalidParams(map[string]any{fieldCursor: "invalid cursor"})
	}

	if offset > len(sessions) {
		return nil, nil, acp.NewInvalidParams(map[string]any{fieldCursor: "cursor is past end"})
	}

	end := offset + listSessionsPageSize
	if end >= len(sessions) {
		return sessions[offset:], nil, nil
	}

	next := encodeListCursor(end)

	return sessions[offset:end], &next, nil
}

func decodeListCursor(cursor *string) (int, error) {
	if cursor == nil || *cursor == "" {
		return 0, nil
	}

	data, err := base64.RawURLEncoding.DecodeString(*cursor)
	if err != nil {
		return 0, err
	}

	offset, err := strconv.Atoi(string(data))
	if err != nil || offset < 0 {
		return 0, strconv.ErrSyntax
	}

	return offset, nil
}

func encodeListCursor(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func (a *Agent) Prompt(ctx context.Context, params acp.PromptRequest) (resp acp.PromptResponse, err error) {
	ctx, finishReq := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionPrompt)
	defer func() { finishReq(err) }()

	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	ctx, finish := a.observe.StartPrompt(ctx, params.Meta, a.options.DefaultModel)
	defer func() { finish(promptResultForObserver(resp, err, a.options.DefaultModel)) }()

	resp, err = session.Prompt(ctx, params)

	return resp, err
}

func (a *Agent) Cancel(ctx context.Context, params acp.CancelNotification) (err error) {
	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionCancel)
	defer func() { finish(err) }()

	session, err := a.session(params.SessionId)
	if err != nil {
		return err
	}

	return session.Cancel(ctx)
}

func (a *Agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (resp acp.CloseSessionResponse, err error) {
	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionClose)
	defer func() { finish(err) }()

	ctx, finishLifecycle, err := a.beginLifecycleOperation(ctx)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}
	defer func() { finishLifecycle(err) }()

	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	err = session.Close(ctx)

	a.mu.Lock()
	delete(a.sessions, params.SessionId)
	a.mu.Unlock()
	a.observe.AddActiveSession(ctx, -1)

	return acp.CloseSessionResponse{}, err
}

func (a *Agent) UnstableDeleteSession(ctx context.Context, params acp.UnstableDeleteSessionRequest) (resp acp.UnstableDeleteSessionResponse, err error) {
	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionDelete)
	defer func() { finish(err) }()

	ctx, finishLifecycle, err := a.beginLifecycleOperation(ctx)
	if err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}
	defer func() { finishLifecycle(err) }()

	ctx = a.observe.Extract(ctx, params.Meta)
	if params.SessionId == "" {
		return acp.UnstableDeleteSessionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldField: jsonFieldSessionID})
	}

	if a.isPendingNativeDelete(params.SessionId) {
		if retryErr := a.retryPendingNativeDelete(ctx, params.SessionId); retryErr != nil {
			return acp.UnstableDeleteSessionResponse{}, retryErr
		}

		return acp.UnstableDeleteSessionResponse{}, nil
	}

	if retryErr := a.retryPendingNativeDeletes(ctx); retryErr != nil {
		return acp.UnstableDeleteSessionResponse{}, retryErr
	}

	stored, err := a.storedSessionExists(ctx, params.SessionId)
	if err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}

	if a.store != nil {
		deleteCtx, cancelDelete := a.sessionStoreWriteContext(ctx)
		deleteErr := a.store.Delete(deleteCtx, SessionKey{SessionID: string(params.SessionId), Subpath: SessionStoreMainSubpath})

		cancelDelete()

		if deleteErr != nil {
			return acp.UnstableDeleteSessionResponse{}, deleteErr
		}
	}

	a.markDeleted(params.SessionId)
	a.mu.Lock()
	session := a.sessions[params.SessionId]
	delete(a.sessions, params.SessionId)
	a.mu.Unlock()

	if session != nil {
		a.observe.AddActiveSession(ctx, -1)
	}

	if !stored {
		if session == nil {
			return acp.UnstableDeleteSessionResponse{}, nil
		}

		return acp.UnstableDeleteSessionResponse{}, session.Close(ctx)
	}

	if err := a.deleteNativeThread(ctx, params.SessionId, session); err != nil {
		a.markPendingNativeDelete(params.SessionId)

		return acp.UnstableDeleteSessionResponse{}, err
	}

	a.clearPendingNativeDelete(params.SessionId)

	return acp.UnstableDeleteSessionResponse{}, nil
}

func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (resp acp.SetSessionConfigOptionResponse, err error) {
	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionSetConfigOption)
	defer func() { finish(err) }()

	if params.Boolean != nil {
		return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldField: fieldValue, jsonFieldError: "boolean config options are unsupported"})
	}

	if params.ValueId == nil {
		return acp.SetSessionConfigOptionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldField: fieldValue})
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

func (a *Agent) SetSessionMode(ctx context.Context, params acp.SetSessionModeRequest) (resp acp.SetSessionModeResponse, err error) {
	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionSetMode)
	defer func() { finish(err) }()

	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

func (a *Agent) loadOrResume(ctx context.Context, sessionID acp.SessionId, cwd string, mcpServers []acp.McpServer, additionalDirs []string, rawMeta map[string]any) (*agentSession, []SessionStoreEntry, bool, error) {
	if retryErr := a.retryPendingNativeDeletes(ctx); retryErr != nil {
		return nil, nil, false, retryErr
	}

	if _, deleted := a.isDeleted(sessionID); deleted {
		return nil, nil, false, unknownSessionError()
	}

	// X1: validate the full request identically to the cold path FIRST, so an
	// already-active session cannot bypass strict _meta, cwd/additional-dir,
	// MCP transport, and model/mode validation. Only after validation
	// succeeds may an active session be reused.
	meta, err := parseSessionMeta(rawMeta)
	if err != nil {
		return nil, nil, false, err
	}

	if optErr := a.validateSessionStartOptions(meta.options); optErr != nil {
		return nil, nil, false, optErr
	}

	if pathErr := validateSessionPaths(cwd, additionalDirs); pathErr != nil {
		return nil, nil, false, pathErr
	}

	mcpConfig, err := mcpConfigJSON(mcpServers)
	if err != nil {
		return nil, nil, false, err
	}

	discoveryRelease, err := acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
	if err != nil {
		return nil, nil, false, err
	}

	discoveryScratchRelease, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery)
	if err != nil {
		discoveryRelease()

		return nil, nil, false, err
	}

	readinessStarted := time.Now()
	startErr := a.ensureStartup(ctx, cwd, meta)
	observeRuntimeStartupStage(ctx, a.options.RuntimeResourceHooks, RuntimeResourceDiscovery, RuntimeStartupReadiness, readinessStarted, startErr)

	releaseNativeRootWhenQuiescent(discoveryRelease, startErr)
	releaseScratchRootWhenQuiescent(discoveryScratchRelease, startErr)

	if startErr != nil {
		return nil, nil, false, startErr
	}

	a.mu.Lock()
	if session := a.sessions[sessionID]; session != nil {
		a.mu.Unlock()

		if applyErr := session.applyActiveRequest(meta, cwd, mcpConfig, additionalDirs); applyErr != nil {
			return nil, nil, false, applyErr
		}

		if syncErr := session.ensureMirrorSynced(ctx); syncErr != nil {
			return nil, nil, false, syncErr
		}

		if verifyErr := session.verifyContinuable(ctx); verifyErr != nil {
			return nil, nil, false, verifyErr
		}

		transcript, loadErr := session.loadTranscript(ctx)
		if loadErr != nil {
			return nil, nil, false, loadErr
		}

		session.setTranscriptFrameCount(len(transcript))

		return session, transcript, false, nil
	}
	a.mu.Unlock()

	manifest, err := a.loadManifest(ctx, sessionID)
	if err != nil {
		return nil, nil, false, err
	}

	if meta.options.Mode == "" {
		meta.options.Mode = manifest.Mode
	}

	session, err := newAgentSession(ctx, a, sessionID, cwd, meta, mcpConfig, additionalDirs)
	if err != nil {
		return nil, nil, false, err
	}

	session.title = manifest.Title
	session.createdUnix = manifest.CreatedAtUnixMilli

	session.updatedUnix = manifest.UpdatedAtUnixMilli

	transcript, err := session.loadTranscript(ctx)
	if err != nil {
		closeErr := session.Close(context.Background())

		return nil, nil, false, errors.Join(err, closeErr)
	}

	session.setTranscriptFrameCount(len(transcript))

	if err = session.verifyContinuable(ctx); err != nil {
		closeErr := session.Close(context.Background())

		return nil, nil, false, errors.Join(err, closeErr)
	}

	a.mu.Lock()
	if len(a.sessions) >= a.maxActiveSessions() {
		a.mu.Unlock()

		closeErr := session.Close(context.Background())

		return nil, nil, false, errors.Join(backpressureError("active_sessions"), closeErr)
	}

	a.sessions[sessionID] = session
	a.mu.Unlock()
	a.observe.AddActiveSession(ctx, 1)

	return session, transcript, true, nil
}

func (a *Agent) removeSession(ctx context.Context, sessionID acp.SessionId, session *agentSession) error {
	a.mu.Lock()

	removed := a.sessions[sessionID] == session
	if removed {
		delete(a.sessions, sessionID)
	}
	a.mu.Unlock()

	if removed {
		a.observe.AddActiveSession(ctx, -1)

		return session.Close(context.Background())
	}

	return nil
}

// sessionStoreLoadTimeout resolves the WithSessionStoreLoadTimeout bound for
// store reads, falling back to the package default.
func (a *Agent) sessionStoreLoadTimeout() time.Duration {
	if a.options.SessionStoreLoadTimeout > 0 {
		return a.options.SessionStoreLoadTimeout
	}

	return defaultSessionStoreTimeout
}

// sessionStoreLoadContext bounds a store READ (Load, ListSessions, ListSubkeys)
// with WithSessionStoreLoadTimeout so a stalled store cannot hang the request.
func (a *Agent) sessionStoreLoadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.sessionStoreLoadTimeout())
}

// sessionStoreWriteContext bounds a store WRITE (Replace, Delete) with the
// fixed write bound; WithSessionStoreLoadTimeout never governs writes.
func (a *Agent) sessionStoreWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, sessionStoreWriteTimeout)
}

func (a *Agent) loadManifest(ctx context.Context, sessionID acp.SessionId) (ampManifest, error) {
	loadCtx, cancel := a.sessionStoreLoadContext(ctx)
	defer cancel()

	entries, err := a.store.Load(loadCtx, SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	if err != nil {
		return ampManifest{}, err
	}

	if len(entries) == 0 {
		return ampManifest{}, unknownSessionError()
	}

	var manifest ampManifest
	if err := json.Unmarshal(entries[len(entries)-1], &manifest); err != nil {
		return ampManifest{}, acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	}

	if manifest.Format != SessionStoreFormat || manifest.ThreadID != string(sessionID) || amp.ValidateThreadID(manifest.ThreadID) != nil {
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
		return mismatchField(optionEnvKey)
	}

	if meta.optionFields.mode && s.mode != meta.options.Mode {
		return mismatchField(optionModeKey)
	}

	if meta.rawEventField {
		s.rawEvents = meta.rawEvent
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
	// A tombstoned session is wire-indistinguishable from one that never
	// existed: both resolve to the uniform unknown-session error.
	if _, deleted := a.isDeleted(id); deleted {
		return nil, unknownSessionError()
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	session := a.sessions[id]
	if session == nil {
		return nil, unknownSessionError()
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

func (a *Agent) isPendingNativeDelete(id acp.SessionId) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, ok := a.pendingNativeDeletes[id]

	return ok
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

func (a *Agent) retryPendingNativeDeletes(ctx context.Context) error {
	var proofErr error

	for _, id := range a.pendingNativeDeleteIDs() {
		if err := a.retryPendingNativeDelete(ctx, id); err != nil {
			a.log.DebugContext(ctx, "retry amp native delete failed", slog.String(jsonFieldSessionID, string(id)), slog.String(jsonFieldError, err.Error()))

			if errors.Is(err, ErrProcessTreeUnproven) {
				proofErr = errors.Join(proofErr, err)
			}
		}
	}

	return proofErr
}

func (a *Agent) retryPendingNativeDelete(ctx context.Context, id acp.SessionId) error {
	if err := a.deleteNativeThread(ctx, id, nil); err != nil {
		return err
	}

	a.clearPendingNativeDelete(id)

	return nil
}

func (a *Agent) storedSessionExists(ctx context.Context, id acp.SessionId) (bool, error) {
	if a.store == nil {
		return false, nil
	}

	loadCtx, cancelLoad := a.sessionStoreLoadContext(ctx)
	entries, err := a.store.Load(loadCtx, SessionKey{SessionID: string(id), Subpath: SessionStoreMainSubpath})

	cancelLoad()

	if err != nil {
		return false, err
	}

	return len(entries) != 0, nil
}

func (a *Agent) deleteNativeThread(ctx context.Context, id acp.SessionId, session *agentSession) error {
	if session != nil {
		return session.Delete(ctx)
	}

	tmp, err := newAgentSession(ctx, a, id, "", parsedSessionMeta{}, "", nil)
	if err != nil {
		return err
	}

	return tmp.Delete(ctx)
}

// missingAPIKeyMessage explains the session-start fail-fast: session commands
// run inside an isolated home, so `amp login` credentials on the host are
// invisible and the amp CLI would otherwise block forever on its interactive
// login flow.
const missingAPIKeyMessage = "AMP_API_KEY is not set: amp sessions run in an " +
	"isolated home where amp login credentials are unavailable; set AMP_API_KEY " +
	"in the process environment, WithEnv, or session env options"

func (a *Agent) ensureStartup(ctx context.Context, cwd string, meta parsedSessionMeta) error {
	env := mergeEnv(a.options.Env, meta.options.Env)
	if !amp.HasAPIKey(env) {
		return acp.NewInternalError(map[string]any{jsonFieldError: missingAPIKeyMessage})
	}

	scratchParent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	}

	client := amp.NewClient(a.log, amp.Options{
		CLIPath:                    a.options.ExecutablePath,
		Cwd:                        cwd,
		Env:                        env,
		ScratchParent:              scratchParent,
		MaxLineBytes:               a.options.runtime.maxJSONLineBytes,
		OnGoroutinePanic:           a.onNativeGoroutinePanic,
		NewProcessSnapshotObserver: a.newProcessSnapshotObserver,
	})
	if err := a.options.runtime.startupProbe(ctx, client); err != nil {
		return nativeInternalError(err)
	}

	return nil
}

func (a *Agent) maxActiveSessions() int {
	if a.options.ConcurrencyLimits.MaxActiveSessions > 0 {
		return a.options.ConcurrencyLimits.MaxActiveSessions
	}

	return defaultMaxActiveSessions
}

func millisToRFC3339(ms int64) string {
	if ms <= 0 {
		return ""
	}

	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}

// sessionInfo renders the live session's current identity for session/list.
func (s *agentSession) sessionInfo() acp.SessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	title := s.title
	updated := millisToRFC3339(s.updatedUnix)

	return acp.SessionInfo{
		SessionId: s.id,
		Cwd:       s.cwd,
		Title:     &title,
		UpdatedAt: &updated,
	}
}

func (s *agentSession) replayTranscript(ctx context.Context) error {
	if s.agent.store == nil {
		return nil
	}

	entries, err := s.loadTranscript(ctx)
	if err != nil {
		return err
	}

	return s.replayTranscriptEntries(ctx, entries)
}

func (s *agentSession) replayTranscriptEntries(ctx context.Context, entries []SessionStoreEntry) error {
	// Authoritative session/load replay emits session/update frames only. Raw
	// events are live-turn only and are never replayed from the store.
	for index, entry := range entries {
		msg, err := amp.ParseJSONLine(entry)
		if err != nil {
			return err
		}

		messageID := assistantMessageIdentity(s.id, index+1, msg)
		if err := s.emitMessage(ctx, msg, false, messageID); err != nil {
			return err
		}
	}

	s.setTranscriptFrameCount(len(entries))

	return nil
}

func (s *agentSession) loadTranscript(ctx context.Context) ([]SessionStoreEntry, error) {
	if s.agent.store == nil {
		return nil, nil
	}

	loadCtx, cancel := s.agent.sessionStoreLoadContext(ctx)
	defer cancel()

	return s.agent.store.Load(loadCtx, SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath})
}

func (s *agentSession) configOptions() []acp.SessionConfigOption {
	modeCategory := acp.SessionConfigOptionCategoryMode

	return []acp.SessionConfigOption{selectConfig(configMode, "Mode", modeCategory, s.mode, validModes())}
}

func (s *agentSession) setConfig(ctx context.Context, id acp.SessionConfigId, value acp.SessionConfigValueId) error {
	s.mu.Lock()
	switch id {
	case configMode:
		if !slices.Contains(validModes(), string(value)) {
			s.mu.Unlock()

			return acp.NewInvalidParams(map[string]any{jsonFieldField: fieldValue})
		}

		s.mode = string(value)
	default:
		s.mu.Unlock()

		return acp.NewInvalidParams(map[string]any{jsonFieldField: "configId"})
	}

	s.updatedUnix = time.Now().UnixMilli()
	s.mu.Unlock()

	if err := s.persistAfterTurn(ctx, nil); err != nil {
		return err
	}

	return s.emitUpdate(ctx, s.configUpdate())
}

// reconcileNativeConfig aligns the session's advertised mode with the value
// amp actually used, as reported in the stream-json init frame. A
// native-reported value wins over the host-requested one once observed; a field
// amp does not report leaves the host-requested value in place. When the
// reconciled state differs from what was last advertised, a config_option_update
// is emitted so the host reads back amp's truth rather than its own request. The
// reconciled state is persisted with the transcript at turn end.
func (s *agentSession) reconcileNativeConfig(ctx context.Context, sys *amp.SystemMessage) error {
	s.mu.Lock()
	changed := false

	if sys.AgentMode != "" && sys.AgentMode != s.mode {
		s.mode = sys.AgentMode
		changed = true
	}

	s.mu.Unlock()

	if !changed {
		return nil
	}

	return s.emitUpdate(ctx, s.configUpdate())
}

// configUpdate builds the config_option_update notification carrying the
// session's current mode advert.
func (s *agentSession) configUpdate() acp.SessionUpdate {
	return acp.SessionUpdate{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{
		SessionUpdate: "config_option_update",
		ConfigOptions: s.configOptions(),
	}}
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
