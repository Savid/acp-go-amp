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

var newLifecycleAgentSession = newAgentSession

func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (resp acp.NewSessionResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionNew)
	defer func() { finish(err) }()

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

	if retryErr := a.retryCleanupOwners(ctx); retryErr != nil {
		return acp.NewSessionResponse{}, retryErr
	}

	startErr := a.ensureNewSessionStartup(ctx, params.Cwd, meta)
	if startErr != nil {
		return acp.NewSessionResponse{}, startErr
	}

	if slotErr := a.reserveSessionSlot(); slotErr != nil {
		return acp.NewSessionResponse{}, slotErr
	}

	sessionID, err := newSessionID()
	if err != nil {
		a.releaseSessionSlot("")

		return acp.NewSessionResponse{}, acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	}

	session, err := newLifecycleAgentSession(ctx, a, acp.SessionId(sessionID), params.Cwd, meta, mcpConfig, params.AdditionalDirectories)
	if err != nil {
		a.releaseSessionSlot("")

		return acp.NewSessionResponse{}, err
	}

	if persistErr := session.persistAfterTurn(ctx, nil); persistErr != nil {
		a.releaseSessionSlot("")

		closeErr := session.Close(context.Background())
		if closeErr == nil {
			a.clearCleanupOwner(session.id, session)
		}

		return acp.NewSessionResponse{}, errors.Join(persistErr, closeErr)
	}

	a.mu.Lock()
	a.activateSessionLocked(session)
	a.pending--
	a.mu.Unlock()
	a.observe.AddActiveSession(ctx, 1)

	return acp.NewSessionResponse{SessionId: session.id, ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) LoadSession(ctx context.Context, params acp.LoadSessionRequest) (resp acp.LoadSessionResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx, params.SessionId)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionLoad)
	defer func() { finish(err) }()

	ctx = a.observe.Extract(ctx, params.Meta)

	session, transcript, started, use, err := a.loadOrResume(ctx, params.SessionId, params.Cwd, params.McpServers, params.AdditionalDirectories, params.Meta)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}

	ctx = withCallbackProvenance(ctx, a, use)
	defer func() { a.finishSessionUse(params.SessionId, use) }()

	if replayErr := session.replayTranscriptEntries(ctx, transcript); replayErr != nil {
		var cleanupErr error

		if started {
			a.finishSessionUse(params.SessionId, use)
			use = nil
			cleanupErr = a.removeSession(ctx, params.SessionId, session)
		}

		return acp.LoadSessionResponse{}, errors.Join(replayErr, cleanupErr)
	}

	return acp.LoadSessionResponse{ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (resp acp.ResumeSessionResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx, params.SessionId)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionResume)
	defer func() { finish(err) }()

	ctx = a.observe.Extract(ctx, params.Meta)

	session, transcript, started, use, err := a.loadOrResume(ctx, params.SessionId, params.Cwd, params.McpServers, params.AdditionalDirectories, params.Meta)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	ctx = withCallbackProvenance(ctx, a, use)
	defer func() { a.finishSessionUse(params.SessionId, use) }()

	messageID, identityErr := terminalAssistantMessageIdentity(params.SessionId, transcript)
	if identityErr != nil {
		var cleanupErr error

		if started {
			a.finishSessionUse(params.SessionId, use)
			use = nil
			cleanupErr = a.removeSession(ctx, params.SessionId, session)
		}

		return acp.ResumeSessionResponse{}, errors.Join(identityErr, cleanupErr)
	}

	if emitErr := session.emitNativeMessageIdentity(ctx, messageID); emitErr != nil {
		var cleanupErr error

		if started {
			a.finishSessionUse(params.SessionId, use)
			use = nil
			cleanupErr = a.removeSession(ctx, params.SessionId, session)
		}

		return acp.ResumeSessionResponse{}, errors.Join(emitErr, cleanupErr)
	}

	return acp.ResumeSessionResponse{ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (resp acp.ListSessionsResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx)
	if err != nil {
		return acp.ListSessionsResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionList)
	defer func() { finish(err) }()

	if refusal := rejectLifecycleMeta(params.Meta); refusal != nil {
		return acp.ListSessionsResponse{}, refusal
	}

	if pathErr := validateOptionalAbsolutePath("cwd", params.Cwd); pathErr != nil {
		return acp.ListSessionsResponse{}, pathErr
	}

	if retryErr := a.retryCleanupOwners(ctx); retryErr != nil {
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
	ctx, finishCall, err := a.beginAgentCall(ctx, params.SessionId)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finishReq := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionPrompt)
	defer func() { finishReq(err) }()

	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	ctx, finish := a.observe.StartPrompt(ctx, params.Meta, a.options.DefaultModel)
	defer func() { finish(promptResultForObserver(resp, err, a.options.DefaultModel)) }()

	resp, err = session.Prompt(ctx, params)

	// The turn's own settlement decides how it ended, and the terminal identity
	// it recorded is the one the v1 response states. A cancel that lands while a
	// turn is already failing does not convert that failure into a clean cancel:
	// the lifecycle idle recorded `failed`, and a response saying `cancelled`
	// would contradict the boundary the host was just shown.
	//
	// A settlement failure keeps the failure's own wire shape rather than the
	// wrapper the prompt marked it as unsettled with.
	var unsettled unsettledPromptError
	if errors.As(err, &unsettled) {
		return acp.PromptResponse{}, unsettled.err
	}

	return resp, err
}

func (a *Agent) Cancel(ctx context.Context, params acp.CancelNotification) (err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx, params.SessionId)
	if err != nil {
		return err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionCancel)
	defer func() { finish(err) }()

	// A cancel carrying the lifecycle key fails closed before native interrupt,
	// and the cancel is never applied. Being a notification it carries no response
	// frame, so the refusal is this method own internal error and is wire-silent.
	if refusal := rejectLifecycleMeta(params.Meta); refusal != nil {
		return refusal
	}

	session, err := a.sessionForCancel(params.SessionId)
	if err != nil {
		return err
	}

	return session.Cancel(ctx)
}

func (a *Agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (resp acp.CloseSessionResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx, params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionClose)
	defer func() { finish(err) }()

	if refusal := rejectLifecycleMeta(params.Meta); refusal != nil {
		return acp.CloseSessionResponse{}, refusal
	}

	session, err := a.session(params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	return acp.CloseSessionResponse{}, a.removeSession(ctx, params.SessionId, session)
}

func (a *Agent) UnstableDeleteSession(ctx context.Context, params acp.UnstableDeleteSessionRequest) (resp acp.UnstableDeleteSessionResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx, params.SessionId)
	if err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionDelete)
	defer func() { finish(err) }()

	if refusal := rejectLifecycleMeta(params.Meta); refusal != nil {
		return acp.UnstableDeleteSessionResponse{}, refusal
	}

	ctx = a.observe.Extract(ctx, params.Meta)
	if params.SessionId == "" {
		return acp.UnstableDeleteSessionResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldField: jsonFieldSessionID})
	}

	ctx, flight, err := a.beginSessionFlight(ctx, params.SessionId, agentSessionDeleteFlight, nil)
	if err != nil {
		return acp.UnstableDeleteSessionResponse{}, err
	}
	defer a.finishSessionFlightOnReturn(params.SessionId, flight)

	if retryErr := a.retryCleanupOwnersExcept(ctx, params.SessionId); retryErr != nil {
		return acp.UnstableDeleteSessionResponse{}, retryErr
	}

	return acp.UnstableDeleteSessionResponse{}, a.deleteSession(ctx, params.SessionId, flight)
}

func (a *Agent) deleteSession(ctx context.Context, id acp.SessionId, flight *agentSessionFlight) error {
	owner, tombstoned, active, err := a.deleteOwnerSnapshot(id, flight)
	if err != nil {
		return err
	}

	if tombstoned {
		return a.retryTombstonedDelete(ctx, id, flight, owner)
	}

	var (
		persistenceFence sessionPersistenceFence
		tombstoneLanded  bool
	)

	defer func() {
		if !tombstoneLanded && owner != nil {
			owner.rollbackPersistenceFence(persistenceFence)

			if active {
				owner.rollbackDeleteAdmission()
			}
		}
	}()

	// Publish the delete fence before the first Store callback. An active
	// wrapper is already the flight's single-assignment owner, so any ordinary
	// writer admitted after this point must be refused rather than slipping in
	// while manifest discovery is in progress.
	if owner != nil {
		persistenceFence, err = owner.fencePersistenceForDeleteRollback(ctx)
		if err != nil {
			return err
		}
	}

	manifest, stored, err := a.storedManifest(ctx, id)
	if err != nil {
		return err
	}

	if validationErr := a.validateSessionFlight(id, flight, owner); validationErr != nil {
		return validationErr
	}

	owner, active, err = a.prepareDeleteOwner(ctx, id, flight, owner, active, manifest, stored)
	if err != nil {
		return err
	}

	if owner != nil && !active {
		// A cold wrapper was constructed only after the store read and therefore
		// did not exist when the early active-owner fence was installed.
		persistenceFence, err = owner.fencePersistenceForDeleteRollback(ctx)
		if err != nil {
			return errors.Join(err, a.recoverRefusedDelete(id, owner, active, persistenceFence))
		}
	}

	if err := a.writeDeleteTombstone(ctx, id); err != nil {
		return errors.Join(err, a.recoverRefusedDelete(id, owner, active, persistenceFence))
	}

	tombstoneLanded = true

	if err := a.validateSessionFlight(id, flight, owner); err != nil {
		return err
	}

	removed := a.publishDeletedOwner(id, flight, owner)
	if removed {
		a.observe.AddActiveSession(ctx, -1)
	}

	if owner == nil {
		return a.reclaimConstructionOwners(ctx, id, nil)
	}

	if err := owner.Delete(ctx); err != nil {
		return err
	}

	if err := a.validateSessionFlight(id, flight, owner); err != nil {
		return err
	}

	a.clearCleanupOwner(id, owner)

	return a.reclaimConstructionOwners(ctx, id, owner)
}

func (a *Agent) deleteOwnerSnapshot(id acp.SessionId, flight *agentSessionFlight) (*agentSession, bool, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.sessionFlights[id] != flight || a.sessionFlights[id].generation != flight.generation {
		return nil, false, false, acp.NewInternalError(map[string]any{jsonFieldError: deleteOwnershipChanged})
	}

	_, tombstoned := a.deleted[id]

	owner := flight.session
	if tombstoned {
		if retained, ok := a.cleanupOwnerOfKindLocked(id, agentCleanupDeleted); ok && owner == nil {
			owner = retained.session
			flight.session = owner
		}
	}

	active := owner != nil && a.sessions[id] == owner

	return owner, tombstoned, active, nil
}

func (a *Agent) retryTombstonedDelete(ctx context.Context, id acp.SessionId, flight *agentSessionFlight, owner *agentSession) error {
	if owner == nil {
		return a.reclaimConstructionOwners(ctx, id, nil)
	}

	if err := owner.Delete(ctx); err != nil {
		return err
	}

	if err := a.validateSessionFlight(id, flight, owner); err != nil {
		return err
	}

	a.clearCleanupOwner(id, owner)

	return a.reclaimConstructionOwners(ctx, id, owner)
}

func (a *Agent) reclaimConstructionOwners(ctx context.Context, id acp.SessionId, except *agentSession) error {
	a.mu.Lock()
	owners := append([]agentCleanupOwner(nil), a.cleanupOwners[id]...)
	a.mu.Unlock()

	var cleanupErr error

	for _, owner := range owners {
		if owner.session == except || owner.kind != agentCleanupConstructing {
			continue
		}

		if err := owner.session.Close(ctx); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)

			continue
		}

		a.clearCleanupOwner(id, owner.session)
	}

	return cleanupErr
}

func (a *Agent) prepareDeleteOwner(
	ctx context.Context,
	id acp.SessionId,
	flight *agentSessionFlight,
	owner *agentSession,
	active bool,
	manifest ampManifest,
	stored bool,
) (*agentSession, bool, error) {
	if owner != nil || !stored || manifest.NativeSessionID == "" {
		return owner, active, nil
	}

	if !validSessionCwd(manifest.Cwd) {
		return owner, active, nil
	}

	prepared, err := newAgentSession(
		ctx,
		a,
		id,
		manifest.Cwd,
		parsedSessionMeta{options: AmpOptions{Mode: manifest.Mode, Env: cloneStringMap(manifest.Env)}},
		"",
		nil,
	)
	if err != nil {
		return nil, false, err
	}

	prepared.nativeID = manifest.NativeSessionID
	prepared.title = manifest.Title
	prepared.createdUnix = manifest.CreatedAtUnixMilli
	prepared.updatedUnix = manifest.UpdatedAtUnixMilli

	a.mu.Lock()

	current := a.sessionFlights[id]
	if current == flight && current.generation == flight.generation && current.session == nil {
		current.session = prepared
		a.mu.Unlock()

		return prepared, false, nil
	}

	var winnerSession *agentSession
	if current != nil && current == flight && current.generation == flight.generation {
		winnerSession = current.session
	}
	a.mu.Unlock()

	cleanupErr := prepared.Close(context.Background())
	if cleanupErr == nil {
		a.clearCleanupOwner(id, prepared)
	}

	if winnerSession != nil {
		return winnerSession, false, cleanupErr
	}

	return nil, false, errors.Join(
		acp.NewInternalError(map[string]any{jsonFieldError: deleteOwnershipChanged}),
		cleanupErr,
	)
}

func (a *Agent) writeDeleteTombstone(ctx context.Context, id acp.SessionId) error {
	if a.store == nil {
		return nil
	}

	if beforeDelete := a.options.runtime.beforeDeleteTombstone; beforeDelete != nil {
		beforeDelete()
	}

	deleteCtx, cancelDelete := a.sessionStoreWriteContext(ctx)
	defer cancelDelete()

	return a.store.Delete(deleteCtx, SessionKey{SessionID: string(id), Subpath: SessionStoreMainSubpath})
}

func (a *Agent) recoverRefusedDelete(id acp.SessionId, owner *agentSession, active bool, fence sessionPersistenceFence) error {
	if owner == nil {
		return nil
	}

	owner.rollbackPersistenceFence(fence)

	if active {
		return nil
	}

	cleanupErr := owner.Close(context.Background())
	if cleanupErr == nil {
		a.clearCleanupOwner(id, owner)
	}

	return cleanupErr
}

func (a *Agent) publishDeletedOwner(id acp.SessionId, flight *agentSessionFlight, owner *agentSession) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.deleted[id] = struct{}{}

	removed := owner != nil && a.sessions[id] == owner
	if removed {
		delete(a.sessions, id)
	}

	if owner != nil {
		a.retainCleanupOwnerLocked(id, owner, agentCleanupDeleted)
	}

	flight.session = owner

	return removed
}

func (a *Agent) validateSessionFlight(id acp.SessionId, flight *agentSessionFlight, owner *agentSession) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	current := a.sessionFlights[id]
	if current != flight || current.generation != flight.generation || current.session != owner {
		return acp.NewInternalError(map[string]any{jsonFieldError: deleteOwnershipChanged})
	}

	return nil
}

func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (resp acp.SetSessionConfigOptionResponse, err error) {
	var sessionID acp.SessionId
	if params.ValueId != nil {
		sessionID = params.ValueId.SessionId
	} else if params.Boolean != nil {
		sessionID = params.Boolean.SessionId
	}

	ctx, finishCall, err := a.beginAgentCall(ctx, sessionID)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	ctx, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionSetConfigOption)
	defer func() { finish(err) }()

	// The family literal is refused by name before the discriminator is judged.
	// Both variants carry their own `_meta`, and "this adapter takes no boolean
	// option" and "the key is not read here" are different answers: a reserved
	// literal is never ignored because the request was going to be refused anyway.
	if refusal := rejectLifecycleConfigOptionMeta(params); refusal != nil {
		return acp.SetSessionConfigOptionResponse{}, refusal
	}

	// Amp advertises select options only, so the boolean variant is refused on
	// the discriminator that chose it rather than on the value it carried.
	if params.Boolean != nil {
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(fieldType)
	}

	if params.ValueId == nil {
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(fieldValue)
	}

	// A value member that carries nothing is the same defect as no value member
	// at all, so it is refused on the same field. This judges the request's
	// shape, not the mode: nothing here holds a list of modes to measure a value
	// against, and every real value travels to amp unchanged for amp to answer
	// for. The empty string is not one of those values — it names no mode, and
	// forwarding it would drop `-m` from the native argv entirely and run the
	// account default while the host reads back a mode it believes it selected.
	if params.ValueId.Value == "" {
		return acp.SetSessionConfigOptionResponse{}, unsupportedField(fieldValue)
	}

	ctx, use, err := a.beginSessionUse(ctx, params.ValueId.SessionId)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}
	defer a.finishSessionUse(params.ValueId.SessionId, use)

	session := use.session
	if session == nil {
		return acp.SetSessionConfigOptionResponse{}, unknownSessionError()
	}

	if err := session.setConfig(ctx, params.ValueId.ConfigId, params.ValueId.Value); err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	return acp.SetSessionConfigOptionResponse{ConfigOptions: session.configOptions()}, nil
}

func (a *Agent) SetSessionMode(ctx context.Context, params acp.SetSessionModeRequest) (resp acp.SetSessionModeResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx, params.SessionId)
	if err != nil {
		return acp.SetSessionModeResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodSessionSetMode)
	defer func() { finish(err) }()

	// The family literal is refused by name before the method's own answer. A
	// method this adapter does not route is still an inbound surface, and a
	// reserved key on it is rejected rather than swallowed with the method.
	if refusal := rejectLifecycleMeta(params.Meta); refusal != nil {
		return acp.SetSessionModeResponse{}, refusal
	}

	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

func (a *Agent) loadOrResume(ctx context.Context, sessionID acp.SessionId, cwd string, mcpServers []acp.McpServer, additionalDirs []string, rawMeta map[string]any) (_ *agentSession, _ []SessionStoreEntry, _ bool, use *agentSessionUse, err error) {
	if _, deleted := a.isDeleted(sessionID); deleted {
		return nil, nil, false, nil, unknownSessionError()
	}

	if retryErr := a.retryCleanupOwner(ctx, sessionID); retryErr != nil {
		return nil, nil, false, nil, retryErr
	}

	if retryErr := a.retryCleanupOwnersExcept(ctx, sessionID); retryErr != nil {
		return nil, nil, false, nil, retryErr
	}

	ctx, use, err = a.beginSessionUse(ctx, sessionID)
	if err != nil {
		return nil, nil, false, nil, err
	}

	if admitted := a.options.runtime.afterSessionUseAdmitted; admitted != nil {
		admitted(use)
	}

	lease := use
	keepUse := false

	defer func() {
		if !keepUse {
			a.finishSessionUse(sessionID, lease)
		}
	}()

	meta, mcpConfig, err := a.validateLoadRequest(cwd, mcpServers, additionalDirs, rawMeta)
	if err != nil {
		return nil, nil, false, nil, err
	}

	startErr := a.ensureStartup(ctx, cwd, meta)
	if startErr != nil {
		return nil, nil, false, nil, startErr
	}

	if useErr := a.validateSessionUse(sessionID, use, use.session); useErr != nil {
		return nil, nil, false, nil, useErr
	}

	if session := use.session; session != nil {
		transcript, activeErr := a.loadActiveSession(ctx, sessionID, use, session, meta, cwd, mcpConfig, additionalDirs)
		if activeErr != nil {
			return nil, nil, false, nil, activeErr
		}

		keepUse = true

		return session, transcript, false, use, nil
	}

	session, transcript, coldErr := a.loadColdSession(ctx, sessionID, use, cwd, meta, mcpConfig, additionalDirs)
	if coldErr != nil {
		return nil, nil, false, nil, coldErr
	}

	keepUse = true

	return session, transcript, true, use, nil
}

// validateLoadRequest runs before active/cold selection so an installed session
// cannot bypass the strict request shape applied to a stored one.
func (a *Agent) validateLoadRequest(cwd string, mcpServers []acp.McpServer, additionalDirs []string, rawMeta map[string]any) (parsedSessionMeta, string, error) {
	meta, err := parseSessionMeta(rawMeta)
	if err != nil {
		return parsedSessionMeta{}, "", err
	}

	if optErr := a.validateSessionStartOptions(meta.options); optErr != nil {
		return parsedSessionMeta{}, "", optErr
	}

	if pathErr := validateSessionPaths(cwd, additionalDirs); pathErr != nil {
		return parsedSessionMeta{}, "", pathErr
	}

	mcpConfig, err := mcpConfigJSON(mcpServers)
	if err != nil {
		return parsedSessionMeta{}, "", err
	}

	return meta, mcpConfig, nil
}

func (a *Agent) loadActiveSession(ctx context.Context, id acp.SessionId, use *agentSessionUse, session *agentSession, meta parsedSessionMeta, cwd, mcpConfig string, additionalDirs []string) ([]SessionStoreEntry, error) {
	if err := session.applyActiveRequest(meta, cwd, mcpConfig, additionalDirs); err != nil {
		return nil, err
	}

	if err := session.ensureMirrorSynced(ctx); err != nil {
		return nil, err
	}

	if err := a.validateSessionUse(id, use, session); err != nil {
		return nil, err
	}

	if err := session.verifyContinuable(ctx); err != nil {
		return nil, err
	}

	if err := a.validateSessionUse(id, use, session); err != nil {
		return nil, err
	}

	transcript, err := session.loadTranscript(ctx)
	if err != nil {
		return nil, err
	}

	if err := a.validateSessionUse(id, use, session); err != nil {
		return nil, err
	}

	session.setTranscriptFrameCount(len(transcript))

	return transcript, nil
}

func (a *Agent) loadColdSession(ctx context.Context, sessionID acp.SessionId, use *agentSessionUse, cwd string, meta parsedSessionMeta, mcpConfig string, additionalDirs []string) (*agentSession, []SessionStoreEntry, error) {
	manifest, err := a.loadManifest(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}

	if useErr := a.validateSessionUse(sessionID, use, nil); useErr != nil {
		return nil, nil, useErr
	}

	if meta.options.Mode == "" {
		meta.options.Mode = manifest.Mode
	}

	if meta.optionFields.env {
		if !maps.Equal(composeEnv(meta.options.Env), manifest.Env) {
			return nil, nil, mismatchField(optionEnvKey)
		}
	} else {
		meta.options.Env = cloneStringMap(manifest.Env)
	}

	session, err := newLifecycleAgentSession(ctx, a, sessionID, cwd, meta, mcpConfig, additionalDirs)
	if err != nil {
		return nil, nil, err
	}

	session.nativeID = manifest.NativeSessionID
	session.title = manifest.Title
	session.createdUnix = manifest.CreatedAtUnixMilli

	session.updatedUnix = manifest.UpdatedAtUnixMilli

	if !a.bindSessionUse(sessionID, use, session) {
		return nil, nil, a.failPreparedLoad(sessionID, use, session, unknownSessionError())
	}

	if prepared := a.options.runtime.afterColdSessionPrepared; prepared != nil {
		prepared(session)
	}

	transcript, err := session.loadTranscript(ctx)
	if err != nil {
		return nil, nil, a.failPreparedLoad(sessionID, use, session, err)
	}

	if useErr := a.validateSessionUse(sessionID, use, session); useErr != nil {
		return nil, nil, a.failPreparedLoad(sessionID, use, session, useErr)
	}

	session.setTranscriptFrameCount(len(transcript))

	if err := session.verifyContinuable(ctx); err != nil {
		return nil, nil, a.failPreparedLoad(sessionID, use, session, err)
	}

	if useErr := a.validateSessionUse(sessionID, use, session); useErr != nil {
		return nil, nil, a.failPreparedLoad(sessionID, use, session, useErr)
	}

	// The entry check is not the last word. Preparation reads the store, starts
	// the runtime and builds a settings and scratch home, and a delete can land
	// anywhere in that window: the tombstone is re-read under the same hold that
	// publishes the session, so a delete that completed while this replacement
	// was being prepared wins however far the preparation got. Installing behind
	// it would name a live session with an id every door already answers unknown
	// for — unreachable, never torn down, holding its slot and its directories
	// for the rest of the agent's life.
	if publishErr := a.publishColdSession(sessionID, use, session); publishErr != nil {
		return nil, nil, a.failPreparedLoad(sessionID, use, session, publishErr)
	}

	a.reopenProviderAuth(sessionID)
	a.observe.AddActiveSession(ctx, 1)

	return session, transcript, nil
}

func (a *Agent) publishColdSession(sessionID acp.SessionId, use *agentSessionUse, session *agentSession) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	currentUse := a.sessionUses[sessionID]
	flight := a.sessionFlights[sessionID]

	if currentUse != use || currentUse.generation != use.generation || a.isDeletedLocked(sessionID) || flight != nil {
		if flight != nil && flight.use == use {
			if flight.session == nil {
				flight.session = session
			} else if flight.session != session {
				return acp.NewInternalError(map[string]any{jsonFieldError: wrapperOwnershipChanged})
			}
		}

		return unknownSessionError()
	}

	cleanupOwners := a.cleanupOwnerCountLocked()
	if _, owned := a.cleanupOwnerForSessionLocked(sessionID, session); owned {
		cleanupOwners--
	}

	if len(a.sessions)+cleanupOwners >= a.maxActiveSessions() {
		return backpressureError("active_sessions")
	}

	a.activateSessionLocked(session)

	return nil
}

func (a *Agent) failPreparedLoad(id acp.SessionId, use *agentSessionUse, session *agentSession, cause error) error {
	return errors.Join(cause, a.cleanupUninstalledSession(id, use, session))
}

func (a *Agent) beginSessionUse(ctx context.Context, id acp.SessionId) (context.Context, *agentSessionUse, error) {
	for {
		a.mu.Lock()
		if _, deleted := a.deleted[id]; deleted {
			a.mu.Unlock()

			return nil, nil, unknownSessionError()
		}

		if flight := a.sessionFlights[id]; flight != nil {
			a.mu.Unlock()

			if contextOwnsCallbackGeneration(ctx, a, flight) {
				return nil, nil, closedCallbackRefusal()
			}

			return nil, nil, unknownSessionError()
		}

		if existing := a.sessionUses[id]; existing != nil {
			wait := existing.done
			a.mu.Unlock()

			if contextOwnsCallbackGeneration(ctx, a, existing) {
				return nil, nil, closedCallbackRefusal()
			}

			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}

		if len(a.cleanupOwners[id]) != 0 {
			if active := a.sessions[id]; active != nil {
				a.clearCleanupOwnerLocked(id, active)
			}
		}

		if len(a.cleanupOwners[id]) != 0 {
			a.mu.Unlock()

			return nil, nil, acp.NewInternalError(map[string]any{jsonFieldError: "session cleanup pending"})
		}

		a.nextSessionGeneration++
		use := &agentSessionUse{
			generation: a.nextSessionGeneration,
			session:    a.sessions[id],
			done:       make(chan struct{}),
		}
		a.sessionUses[id] = use
		a.mu.Unlock()

		useCtx := withCallbackProvenance(ctx, a, use)

		return withCallbackSessionScope(useCtx, a, id), use, nil
	}
}

// bindSessionUse publishes the cold wrapper before continuability export. If a
// teardown flight was published while preparation was in progress, that flight
// takes the exact pointer and the caller starts no export.
func (a *Agent) bindSessionUse(id acp.SessionId, use *agentSessionUse, session *agentSession) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if use == nil || a.sessionUses[id] != use || a.sessionUses[id].generation != use.generation {
		return false
	}

	use.session = session
	if flight := a.sessionFlights[id]; flight != nil {
		if flight.use == use {
			if flight.session == nil {
				flight.session = session
			}
		}

		return false
	}

	_, deleted := a.deleted[id]

	return !deleted
}

func (a *Agent) finishSessionUse(id acp.SessionId, use *agentSessionUse) {
	if use == nil {
		return
	}

	a.mu.Lock()
	if a.sessionUses[id] == use && a.sessionUses[id].generation == use.generation {
		delete(a.sessionUses, id)
		close(use.done)
	}

	flight, session, reclaim := a.claimAbandonedSessionFlightLocked(id, use)
	a.mu.Unlock()

	if reclaim {
		a.reclaimAbandonedSessionFlight(id, flight, session)
	}
}

func (a *Agent) validateSessionUse(id acp.SessionId, use *agentSessionUse, expect *agentSession) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	current := a.sessionUses[id]
	if use == nil || current != use || current.generation != use.generation || current.session != expect {
		return acp.NewInternalError(map[string]any{jsonFieldError: "session use ownership changed"})
	}

	if a.isDeletedLocked(id) {
		return unknownSessionError()
	}

	if flight := a.sessionFlights[id]; flight != nil {
		if flight.use != use {
			return unknownSessionError()
		}

		if flight.session != nil && expect != nil && flight.session != expect {
			return acp.NewInternalError(map[string]any{jsonFieldError: wrapperOwnershipChanged})
		}
	}

	if active := a.sessions[id]; active != nil && active != expect {
		return acp.NewInternalError(map[string]any{jsonFieldError: wrapperOwnershipChanged})
	}

	return nil
}

func (a *Agent) cleanupUninstalledSession(id acp.SessionId, use *agentSessionUse, session *agentSession) error {
	a.mu.Lock()
	flight := a.sessionFlights[id]
	transferred := flight != nil && flight.use == use && flight.session == session
	a.mu.Unlock()

	if transferred {
		return nil
	}

	a.retainCleanupOwner(id, session, agentCleanupPrepared)

	cleanupErr := session.Close(context.Background())
	if cleanupErr == nil {
		a.clearCleanupOwner(id, session)
	}

	return cleanupErr
}

func (a *Agent) beginSessionFlight(ctx context.Context, id acp.SessionId, kind agentSessionFlightKind, expect *agentSession) (context.Context, *agentSessionFlight, error) {
	for {
		flightCtx, flight, use, existing, err := a.publishSessionFlight(ctx, id, kind, expect)
		if err != nil {
			return nil, nil, err
		}

		if existing != nil {
			wait := existing.done
			if a.contextOwnsSessionFlightDependency(ctx, existing) {
				a.finishSessionFlightWait(existing)

				return nil, nil, closedCallbackRefusal()
			}

			select {
			case <-wait:
				a.finishSessionFlightWait(existing)

				if existing.panicErr != nil {
					return nil, nil, existing.panicErr
				}

				continue
			case <-ctx.Done():
				a.finishSessionFlightWait(existing)

				return nil, nil, ctx.Err()
			}
		}

		if err := a.joinSessionFlightUse(flightCtx, id, flight, use); err != nil {
			return nil, nil, err
		}

		return flightCtx, flight, nil
	}
}

func (a *Agent) contextOwnsSessionFlightDependency(ctx context.Context, flight *agentSessionFlight) bool {
	if contextOwnsCallbackGeneration(ctx, a, flight) {
		return true
	}

	a.mu.Lock()
	use := flight.use
	session := flight.session
	a.mu.Unlock()

	if use != nil && contextOwnsCallbackGeneration(ctx, a, use) {
		return true
	}

	if session == nil {
		return false
	}

	session.teardownMu.Lock()
	teardown := session.teardownFlight
	session.teardownMu.Unlock()

	if teardown != nil && session.contextOwnsTeardownDependency(ctx, teardown) {
		return true
	}

	return false
}

// publishSessionFlight is the teardown linearization: it installs the exact
// generation and snapshots the admitted use without waiting on either. Keeping
// publication separate from the join makes pointer transfer testable with
// barriers and keeps every external wait outside the agent mutex.
func (a *Agent) publishSessionFlight(ctx context.Context, id acp.SessionId, kind agentSessionFlightKind, expect *agentSession) (context.Context, *agentSessionFlight, *agentSessionUse, *agentSessionFlight, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if existing := a.sessionFlights[id]; existing != nil {
		existing.waiters++

		return nil, nil, nil, existing, nil
	}

	if kind == agentSessionCloseFlight {
		if _, deleted := a.deleted[id]; deleted {
			return nil, nil, nil, nil, unknownSessionError()
		}

		current := a.sessions[id]
		if current == nil || (expect != nil && current != expect) {
			return nil, nil, nil, nil, unknownSessionError()
		}
	}

	use := a.sessionUses[id]
	if use != nil && contextOwnsCallbackGeneration(ctx, a, use) {
		return nil, nil, nil, nil, closedCallbackRefusal()
	}

	session := a.sessions[id]
	if session == nil && use != nil {
		session = use.session
	}

	if expect != nil {
		session = expect
	}

	if session != nil && session.contextOwnsTeardownDependency(ctx, nil) {
		return nil, nil, nil, nil, closedCallbackRefusal()
	}

	a.nextSessionGeneration++
	flight := &agentSessionFlight{
		generation: a.nextSessionGeneration,
		kind:       kind,
		session:    session,
		use:        use,
		done:       make(chan struct{}),
	}
	a.sessionFlights[id] = flight

	flightCtx := withCallbackProvenance(ctx, a, flight)

	return withCallbackSessionScope(flightCtx, a, id), flight, use, nil, nil
}

func (a *Agent) finishSessionFlightWait(flight *agentSessionFlight) {
	a.mu.Lock()
	if flight != nil && flight.waiters > 0 {
		flight.waiters--
	}
	a.mu.Unlock()
}

func (a *Agent) joinSessionFlightUse(ctx context.Context, id acp.SessionId, flight *agentSessionFlight, use *agentSessionUse) error {
	if use == nil {
		return nil
	}

	select {
	case <-use.done:
	case <-ctx.Done():
		a.mu.Lock()
		if a.sessionFlights[id] == flight && flight.generation == a.sessionFlights[id].generation {
			flight.abandoned = true
		}

		claimedFlight, session, reclaim := a.claimAbandonedSessionFlightLocked(id, use)
		a.mu.Unlock()

		if reclaim {
			a.reclaimAbandonedSessionFlight(id, claimedFlight, session)
		}

		return ctx.Err()
	}

	a.mu.Lock()
	if a.sessionFlights[id] != flight || a.sessionFlights[id].generation != flight.generation {
		a.mu.Unlock()

		return acp.NewInternalError(map[string]any{jsonFieldError: "session ownership changed"})
	}

	if flight.session == nil {
		flight.session = use.session
	}

	if current := a.sessions[id]; current != nil && flight.session != nil && current != flight.session {
		a.mu.Unlock()
		a.finishSessionFlight(id, flight)

		return acp.NewInternalError(map[string]any{jsonFieldError: wrapperOwnershipChanged})
	}

	if flight.session == nil {
		flight.session = a.sessions[id]
	}
	a.mu.Unlock()

	return nil
}

func (a *Agent) claimAbandonedSessionFlightLocked(id acp.SessionId, use *agentSessionUse) (*agentSessionFlight, *agentSession, bool) {
	flight := a.sessionFlights[id]
	if flight == nil || !flight.abandoned || flight.reclaiming || flight.use != use {
		return nil, nil, false
	}

	if current := a.sessionUses[id]; current == use && current.generation == use.generation {
		return nil, nil, false
	}

	flight.reclaiming = true
	if flight.session == nil {
		flight.session = use.session
	}

	return flight, flight.session, true
}

func (a *Agent) reclaimAbandonedSessionFlight(id acp.SessionId, flight *agentSessionFlight, session *agentSession) {
	panicErr := error(nil)

	defer func() {
		if recover() != nil {
			panicErr = errAgentGoroutinePanic
		}

		if panicErr != nil {
			a.finishSessionFlightWithPanic(id, flight, panicErr)

			return
		}

		a.finishSessionFlight(id, flight)
	}()

	if session != nil {
		a.mu.Lock()
		active := a.sessions[id] == session
		a.mu.Unlock()

		if !active {
			a.retainCleanupOwner(id, session, agentCleanupPrepared)

			if cleanupErr := session.Close(context.Background()); cleanupErr == nil {
				a.clearCleanupOwner(id, session)
			}
		}
	}
}

func (a *Agent) finishSessionFlight(id acp.SessionId, flight *agentSessionFlight) {
	if flight == nil {
		return
	}

	a.mu.Lock()
	if a.sessionFlights[id] == flight && a.sessionFlights[id].generation == flight.generation {
		delete(a.sessionFlights, id)
		close(flight.done)
	}
	a.mu.Unlock()
}

func (a *Agent) finishSessionFlightWithPanic(id acp.SessionId, flight *agentSessionFlight, panicErr error) {
	if flight == nil {
		return
	}

	a.mu.Lock()
	if a.sessionFlights[id] == flight && a.sessionFlights[id].generation == flight.generation {
		flight.panicErr = panicErr

		delete(a.sessionFlights, id)
		close(flight.done)
	}
	a.mu.Unlock()
}

func (a *Agent) finishSessionFlightOnReturn(id acp.SessionId, flight *agentSessionFlight) {
	if recovered := recover(); recovered != nil {
		a.finishSessionFlightWithPanic(id, flight, closedCallbackRefusal())

		panic(recovered)
	}

	a.finishSessionFlight(id, flight)
}

// removeSession tears down session only while it still owns sessionID, so
// close and rollback paths cannot reap a session installed after they read
// theirs.
//
// The settlement and the durable rung both run before the eviction, so the rung
// runs while the session is still addressable: a commit this close cannot land
// leaves the session in the map holding its own unsynced frames, and the host
// closes the same session again once its store is back. A close whose commit
// failed evicts nothing.
func (a *Agent) removeSession(ctx context.Context, sessionID acp.SessionId, session *agentSession) error {
	ctx, flight, err := a.beginSessionFlight(ctx, sessionID, agentSessionCloseFlight, session)
	if err != nil {
		if session.deleteComplete() {
			return nil
		}

		a.mu.Lock()
		current := a.sessions[sessionID]
		a.mu.Unlock()

		if current != session {
			return nil
		}

		return err
	}

	defer a.finishSessionFlightOnReturn(sessionID, flight)

	ctx, wrapperFlight, err := session.beginTeardown(ctx)
	if err != nil {
		return err
	}
	defer session.finishTeardownOnReturn(wrapperFlight)

	settlement := session.settleCloseRung(ctx)
	if fenceErr := session.fencePersistenceForClose(ctx); fenceErr != nil {
		return errors.Join(settlement.runtimeErr, fenceErr)
	}

	// Same order a prompt settles in: the containment boundary is proven before
	// anything durable is written. An unproven boundary keeps the exact installed
	// wrapper addressable, including the settings tree and scratch reservation a
	// surviving descendant may still use.
	if !amp.ProcessContainmentComplete(settlement.boundaryErr) {
		return errors.Join(settlement.runtimeErr, settlement.boundaryErr)
	}
	// A prompt settlement's failed Replace is an owed rung, not a permanent
	// teardown failure. The close-owned retry discharges it when the store has
	// healed; a retry that still fails leaves the same wrapper installed.
	if commitErr := session.commitCloseRung(ctx); commitErr != nil {
		return errors.Join(settlement.runtimeErr, commitErr)
	}

	if terminalErr := session.deliverPendingTerminal(ctx); terminalErr != nil {
		return errors.Join(settlement.runtimeErr, terminalErr)
	}

	// Containment and terminal-delivery failures have no close-owned retry rung.
	// They fail this attempt and retain ownership so a later close can re-evaluate
	// the exact wrapper rather than returning an error after eviction.
	if settlement.runtimeErr != nil {
		return settlement.runtimeErr
	}

	// Local cleanup is part of ownership settlement. The active map keeps the
	// exact pointer and its gauge until directory removal and scratch release have
	// both succeeded.
	if cleanupErr := session.finalizeScratch(settlement.runtimeErr, settlement.boundaryErr); cleanupErr != nil {
		return cleanupErr
	}

	a.mu.Lock()

	currentFlight := a.sessionFlights[sessionID]
	if currentFlight != flight || currentFlight.generation != flight.generation || a.sessions[sessionID] != session {
		a.mu.Unlock()

		return acp.NewInternalError(map[string]any{jsonFieldError: "close ownership changed"})
	}

	delete(a.sessions, sessionID)
	a.clearCleanupOwnerLocked(sessionID, session)
	a.mu.Unlock()
	a.finishSessionFlight(sessionID, flight)

	a.observe.AddActiveSession(ctx, -1)

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

	if manifest.Format != SessionStoreFormat || manifest.SessionID != string(sessionID) || !validNativeSessionID(manifest.NativeSessionID) || !validStoredSessionEnv(manifest.Env) {
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

	if !maps.Equal(s.sessionEnv, composeEnv(meta.options.Env)) {
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
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	if len(a.sessions)+a.pending+a.cleanupOwnerCountLocked() >= a.maxActiveSessions() {
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
	a.mu.Lock()
	defer a.mu.Unlock()

	// A tombstoned or teardown-owned session is wire-indistinguishable from one
	// that never existed. The flight check also makes a Store.Delete callback
	// re-entering CloseSession fail closed instead of waiting on its caller.
	if _, deleted := a.deleted[id]; deleted || a.sessionFlights[id] != nil {
		return nil, unknownSessionError()
	}

	session := a.sessions[id]
	if session == nil {
		return nil, unknownSessionError()
	}

	return session, nil
}

func (a *Agent) sessionForCancel(id acp.SessionId) (*agentSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, deleted := a.deleted[id]; deleted {
		return nil, unknownSessionError()
	}

	if session := a.sessions[id]; session != nil {
		return session, nil
	}

	return nil, unknownSessionError()
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

// isDeletedLocked reads the same marker under a lock the caller already holds.
// The install path needs it that way: the tombstone and the publication have to
// be one atomic decision, or a delete landing between them leaves a session
// nothing can ever address.
func (a *Agent) isDeletedLocked(id acp.SessionId) bool {
	_, ok := a.deleted[id]

	return ok
}

func (a *Agent) cleanupOwner(id acp.SessionId) (agentCleanupOwner, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.cleanupOwnerLocked(id)
}

func (a *Agent) cleanupOwnerLocked(id acp.SessionId) (agentCleanupOwner, bool) {
	owners := a.cleanupOwners[id]
	if len(owners) == 0 {
		return agentCleanupOwner{}, false
	}

	return owners[0], true
}

func (a *Agent) cleanupOwnerOfKindLocked(id acp.SessionId, kind agentCleanupKind) (agentCleanupOwner, bool) {
	for _, owner := range a.cleanupOwners[id] {
		if owner.kind == kind {
			return owner, true
		}
	}

	return agentCleanupOwner{}, false
}

func (a *Agent) cleanupOwnerForSessionLocked(id acp.SessionId, session *agentSession) (agentCleanupOwner, bool) {
	for _, owner := range a.cleanupOwners[id] {
		if owner.session == session {
			return owner, true
		}
	}

	return agentCleanupOwner{}, false
}

func (a *Agent) retainCleanupOwner(id acp.SessionId, session *agentSession, kind agentCleanupKind) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.retainCleanupOwnerLocked(id, session, kind)
}

func (a *Agent) retainCleanupOwnerLocked(id acp.SessionId, session *agentSession, kind agentCleanupKind) {
	owners := a.cleanupOwners[id]
	for index := range owners {
		if owners[index].session == session {
			owners[index].kind = kind
			a.cleanupOwners[id] = owners

			return
		}
	}

	a.cleanupOwners[id] = append(owners, agentCleanupOwner{session: session, kind: kind})
}

func (a *Agent) clearCleanupOwner(id acp.SessionId, expect *agentSession) {
	a.mu.Lock()
	a.clearCleanupOwnerLocked(id, expect)
	a.mu.Unlock()
}

func (a *Agent) clearCleanupOwnerLocked(id acp.SessionId, expect *agentSession) {
	owners := a.cleanupOwners[id]
	for index := range owners {
		if owners[index].session != expect {
			continue
		}

		owners = append(owners[:index], owners[index+1:]...)
		if len(owners) == 0 {
			delete(a.cleanupOwners, id)
		} else {
			a.cleanupOwners[id] = owners
		}

		break
	}
}

// activateSessionLocked transfers one fully constructed wrapper from private
// cleanup ownership to the public active-session map in the same critical
// section. Callers must hold a.mu and must have completed their own tombstone,
// flight, and capacity validation first.
func (a *Agent) activateSessionLocked(session *agentSession) {
	a.sessions[session.id] = session
	a.clearCleanupOwnerLocked(session.id, session)
}

func (a *Agent) cleanupOwnerIDs() []acp.SessionId {
	a.mu.Lock()
	defer a.mu.Unlock()

	ids := make([]acp.SessionId, 0, len(a.cleanupOwners))
	for id := range a.cleanupOwners {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	return ids
}

func (a *Agent) cleanupOwnerCountLocked() int {
	count := 0
	for _, owners := range a.cleanupOwners {
		count += len(owners)
	}

	return count
}

func (a *Agent) retryCleanupOwners(ctx context.Context) error {
	return a.retryCleanupOwnersExcept(ctx, "")
}

func (a *Agent) retryCleanupOwnersExcept(ctx context.Context, except acp.SessionId) error {
	var retryErr error

	for _, id := range a.cleanupOwnerIDs() {
		if id == except {
			continue
		}

		if err := a.retryCleanupOwner(ctx, id); err != nil {
			a.log.DebugContext(ctx, "retry amp session cleanup failed", slog.String(jsonFieldSessionID, string(id)), slog.String("failure", cleanupFailureClass(err)))

			if containmentIncomplete(err) || errors.Is(err, ErrNativeTreeBusy) {
				retryErr = errors.Join(retryErr, err)
			}
		}
	}

	return retryErr
}

func (a *Agent) retryCleanupOwner(ctx context.Context, id acp.SessionId) error {
	for {
		owner, ok := a.cleanupOwner(id)
		if !ok {
			return nil
		}

		a.mu.Lock()
		if a.sessions[id] == owner.session {
			a.clearCleanupOwnerLocked(id, owner.session)
			a.mu.Unlock()

			continue
		}
		a.mu.Unlock()

		flightCtx, flight, err := a.beginSessionFlight(ctx, id, agentSessionDeleteFlight, owner.session)
		if err != nil {
			return err
		}

		a.mu.Lock()
		owner, ok = a.cleanupOwnerForSessionLocked(id, owner.session)
		a.mu.Unlock()

		if !ok {
			a.finishSessionFlight(id, flight)

			continue
		}

		err = func() error {
			defer a.finishSessionFlightOnReturn(id, flight)

			if owner.kind == agentCleanupDeleted {
				return owner.session.Delete(flightCtx)
			}

			return owner.session.Close(flightCtx)
		}()
		if err != nil {
			return err
		}

		a.clearCleanupOwner(id, owner.session)
	}
}

// storedManifest loads the durable main-key row for a session. A stored row
// whose last entry is not a valid manifest still reports stored=true with a
// zero manifest, so the store row remains deletable while the native thread id
// is treated as unknown.
func (a *Agent) storedManifest(ctx context.Context, id acp.SessionId) (ampManifest, bool, error) {
	if a.store == nil {
		return ampManifest{}, false, nil
	}

	loadCtx, cancelLoad := a.sessionStoreLoadContext(ctx)
	entries, err := a.store.Load(loadCtx, SessionKey{SessionID: string(id), Subpath: SessionStoreMainSubpath})

	cancelLoad()

	if err != nil {
		return ampManifest{}, false, err
	}

	if len(entries) == 0 {
		return ampManifest{}, false, nil
	}

	manifest, ok := manifestFromStoreEntry(entries[len(entries)-1])
	if !ok || manifest.SessionID != string(id) || !validSessionCwd(manifest.Cwd) {
		return ampManifest{}, true, nil
	}

	return manifest, true, nil
}

const missingAPIKeyMessage = "AMP_API_KEY is not set: amp sessions run in an " +
	"isolated home where amp login credentials are unavailable; set AMP_API_KEY " +
	"in the host native environment, WithEnv, or " +
	"session env options"

func (a *Agent) ensureNewSessionStartup(ctx context.Context, cwd string, meta parsedSessionMeta) error {
	if amp.HasAPIKey(composeEnv(a.nativeEnvironmentBase(), a.options.Env, meta.options.Env)) {
		return a.ensureStartupWithProbe(ctx, cwd, meta.options.Env, a.options.runtime.startupProbe)
	}

	if a.providerAuth == nil {
		return missingAPIKeyError()
	}

	return a.ensureStartupWithProbe(
		ctx, cwd, meta.options.Env,
		func(ctx context.Context, client *amp.Client) (string, error) {
			return client.DiscoveryProbe(ctx)
		},
	)
}

func (a *Agent) ensureStartup(ctx context.Context, cwd string, meta parsedSessionMeta) error {
	if !amp.HasAPIKey(composeEnv(a.nativeEnvironmentBase(), a.options.Env, meta.options.Env)) {
		return missingAPIKeyError()
	}

	return a.ensureStartupWithProbe(ctx, cwd, meta.options.Env, a.options.runtime.startupProbe)
}

func (a *Agent) nativeEnvironmentBase() map[string]string {
	if a.options.hostAuthoritySupplied {
		return a.nativeEnvironment
	}

	return a.ordinaryEnvironment
}

func missingAPIKeyError() error {
	return acp.NewInternalError(map[string]any{jsonFieldError: missingAPIKeyMessage})
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

// advertisedModes is the menu a host renders, not a gate a value has to pass:
// amp owns the mode namespace, so the list names the modes the shipping CLI
// documents and nothing here refuses a mode outside it. A current value the
// list does not contain is the expected shape whenever a host or amp itself
// names a mode this build has never heard of.
func advertisedModes() []string {
	return []string{modeLow, modeMedium, modeHigh, modeUltra}
}

func (s *agentSession) configOptions() []acp.SessionConfigOption {
	modeCategory := acp.SessionConfigOptionCategoryMode

	return []acp.SessionConfigOption{selectConfig(configMode, "Mode", modeCategory, s.mode, advertisedModes())}
}

// setConfig names the only config option this adapter serves and forwards its
// value verbatim. A mode selects amp's model, system prompt, and tool set on the
// hosted backend, so the value list belongs to amp: an id this adapter does not
// serve is refused by name, and every value under `mode` travels to the native
// `-m` flag for amp to accept or reject. The mode amp actually ran is read back
// from the turn's init frame by reconcileNativeConfig.
func (s *agentSession) setConfig(ctx context.Context, id acp.SessionConfigId, value acp.SessionConfigValueId) error {
	if id != configMode {
		return unsupportedField(fieldConfigID)
	}

	persistCtx, flight, err := s.beginPersistence(ctx, sessionPersistenceOrdinary)
	if err != nil {
		return err
	}
	defer s.finishPersistence(flight)

	s.mu.Lock()
	s.mode = string(value)
	s.updatedUnix = time.Now().UnixMilli()
	s.mu.Unlock()

	if err := s.persistOwned(persistCtx, flight, nil); err != nil {
		return err
	}

	return s.emitUpdate(persistCtx, s.configUpdate())
}

// reconcileNativeConfig aligns the session's advertised mode with the value
// amp actually used, as reported in the stream-json init frame. A
// native-reported value wins over the host-requested one once observed; a field
// amp does not report leaves the host-requested value in place. When the
// reconciled state differs from what was last advertised, a config_option_update
// is emitted so the host reads back amp's truth rather than its own request. The
// reconciled state is persisted with the transcript at turn end.
//
// Nothing local judges a mode value, so this read-back is the only thing that
// makes a server-side substitution visible: a mode amp declined and fell back
// from is reported here under its real name, and a host that asked for a mode
// this build never heard of learns what it actually got.
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
