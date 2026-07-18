package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

func TestAmpMessageIdentityIsStableUUIDAndFrameScoped(t *testing.T) {
	const raw = `{"type":"assistant","message":{"content":[{"type":"text","text":"same"}]},"session_id":"T-1"}`

	first := ampMessageIdentity("T-1", 3, raw)
	require.Equal(t, first, ampMessageIdentity("T-1", 3, raw))
	require.Len(t, first, 36)
	require.Equal(t, byte('8'), first[14], "wrapper derivation uses UUIDv8")
	require.Contains(t, "89ab", string(first[19]), "UUID uses the RFC variant")
	require.NotEqual(t, first, ampMessageIdentity("T-1", 7, raw), "a later byte-identical frame is a different message")
	require.NotEqual(t, first, ampMessageIdentity("T-2", 3, raw), "sessions cannot collide")
	require.NotEqual(t, first, ampMessageIdentity("T-1", 3, raw+" "), "exact mirror bytes anchor the identity")
	require.Empty(t, ampMessageIdentity("T-1", 0, raw))

	delegated := &amp.AssistantMessage{ParentToolUseID: "TU-parent", RawJSONText: raw}
	require.Empty(t, assistantMessageIdentity("T-1", 3, delegated), "delegated output cannot become the terminal identity")
}

func TestMessageIdentityMatchesLiveResponseReplayAndContinuation(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()

	firstAgent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	firstClient, firstCleanup := attachRecordingClient(t, firstAgent)

	created, err := firstAgent.NewSession(ctx, NewSessionRequest(cwd))
	require.NoError(t, err)

	firstResponse, err := firstAgent.Prompt(ctx, TextPromptRequest(created.SessionId, "user-1", "first"))
	require.NoError(t, err)
	firstID := responseAmpMessageID(t, firstResponse)
	waitForMessageIdentities(t, firstClient, 1)
	require.Equal(t, []string{firstID}, updateAmpMessageIDs(t, firstClient.updatesSnapshot()))

	secondResponse, err := firstAgent.Prompt(ctx, TextPromptRequest(created.SessionId, "user-2", "second"))
	require.NoError(t, err)
	secondID := responseAmpMessageID(t, secondResponse)
	require.NotEqual(t, firstID, secondID, "identical native assistant bytes in different turns must not collide")
	waitForMessageIdentities(t, firstClient, 2)
	require.Equal(t, []string{firstID, secondID}, updateAmpMessageIDs(t, firstClient.updatesSnapshot()))

	stored, err := store.Load(ctx, SessionKey{SessionID: string(created.SessionId), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, stored)
	for _, frame := range stored {
		require.NotContains(t, string(frame), `"messageId"`, "native transcript frames stay byte-verbatim")
		require.NotContains(t, string(frame), `"_meta"`, "wrapper identity never contaminates the native mirror")
	}

	require.NoError(t, firstAgent.Close())
	firstCleanup()

	restoredAgent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	restoredClient, restoredCleanup := attachRecordingClient(t, restoredAgent)
	defer restoredCleanup()
	defer func() { require.NoError(t, restoredAgent.Close()) }()

	_, err = restoredAgent.LoadSession(ctx, LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	waitForMessageIdentities(t, restoredClient, 2)
	require.Equal(t, []string{firstID, secondID}, updateAmpMessageIDs(t, restoredClient.updatesSnapshot()), "load replay must reproduce live identities")

	thirdResponse, err := restoredAgent.Prompt(ctx, TextPromptRequest(created.SessionId, "user-3", "continue after load"))
	require.NoError(t, err)
	thirdID := responseAmpMessageID(t, thirdResponse)
	require.NotEqual(t, firstID, thirdID)
	require.NotEqual(t, secondID, thirdID)
	waitForMessageIdentities(t, restoredClient, 3)
	require.Equal(t, []string{firstID, secondID, thirdID}, updateAmpMessageIDs(t, restoredClient.updatesSnapshot()))
}

func TestResumePublishesTerminalMessageIdentityWithoutHistory(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	const sessionID = acp.SessionId("T-resume-identity")
	const mainFrame = `{"type":"assistant","message":{"content":[{"type":"text","text":"answer"}]},"session_id":"T-resume-identity"}`
	store := NewInMemorySessionStore()
	putStoredSession(t, store, string(sessionID), cwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"user","message":{"content":[{"type":"text","text":"question"}]},"session_id":"T-resume-identity"}`),
		json.RawMessage(mainFrame),
		json.RawMessage(`{"type":"assistant","parent_tool_use_id":"TU-parent","message":{"content":[{"type":"text","text":"delegated"}]},"session_id":"T-resume-identity"}`),
		json.RawMessage(`{"type":"result","subtype":"success","is_error":false,"session_id":"T-resume-identity"}`),
	})
	expectedID := ampMessageIdentity(sessionID, 2, mainFrame)

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
	)
	client := newIdentityAgentClient()
	agent.setConnection(client)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })

	_, err := agent.ResumeSession(ctx, ResumeSessionRequest(sessionID, cwd))
	require.NoError(t, err)
	require.Len(t, client.notifications, 1, "resume must not replay transcript history")
	notification := client.notifications[0]
	require.Equal(t, sessionID, notification.SessionId)
	require.Equal(t, acp.SessionUpdate{
		SessionInfoUpdate: &acp.SessionSessionInfoUpdate{},
	}, notification.Update)
	ampMeta, ok := notification.Meta[ampMetaKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, expectedID, ampMeta[metaMessageIDKey])

	client.updateErr = errors.New("identity delivery failed")
	_, err = agent.ResumeSession(ctx, ResumeSessionRequest(sessionID, cwd))
	require.ErrorContains(t, err, "identity delivery failed")
	require.Contains(t, agent.sessions, sessionID, "an active session survives lifecycle delivery failure")

	failingAgent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
	)
	failingClient := newIdentityAgentClient()
	failingClient.updateErr = errors.New("identity delivery failed")
	failingAgent.setConnection(failingClient)
	_, err = failingAgent.ResumeSession(ctx, ResumeSessionRequest(sessionID, cwd))
	require.ErrorContains(t, err, "identity delivery failed")
	require.NotContains(t, failingAgent.sessions, sessionID, "failed cold resume must roll back its materialized session")
	require.NoError(t, failingAgent.Close())

	connectionlessAgent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
	)
	_, err = connectionlessAgent.ResumeSession(ctx, ResumeSessionRequest(sessionID, cwd))
	require.NoError(t, err)
	require.NoError(t, connectionlessAgent.Close())
}

func TestResumeIdentityEmptyAndMalformedTranscript(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()

	emptyStore := NewInMemorySessionStore()
	putStoredSession(t, emptyStore, "T-resume-empty", cwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"result","subtype":"success","is_error":false,"session_id":"T-resume-empty"}`),
	})
	emptyAgent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(emptyStore),
	)
	emptyClient := newIdentityAgentClient()
	emptyAgent.setConnection(emptyClient)
	_, err := emptyAgent.ResumeSession(ctx, ResumeSessionRequest("T-resume-empty", cwd))
	require.NoError(t, err)
	require.Empty(t, emptyClient.notifications, "no assistant identity means no synthetic history update")
	require.NoError(t, emptyAgent.Close())

	malformedStore := NewInMemorySessionStore()
	putStoredSession(t, malformedStore, "T-resume-malformed", cwd, []SessionStoreEntry{
		json.RawMessage(`{`),
	})
	malformedAgent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(malformedStore),
	)
	malformedAgent.setConnection(newIdentityAgentClient())
	_, err = malformedAgent.ResumeSession(ctx, ResumeSessionRequest("T-resume-malformed", cwd))
	require.Error(t, err)
	require.NotContains(t, malformedAgent.sessions, acp.SessionId("T-resume-malformed"))
	require.NoError(t, malformedAgent.Close())
}

type identityAgentClient struct {
	done          chan struct{}
	notifications []acp.SessionNotification
	updateErr     error
}

func newIdentityAgentClient() *identityAgentClient {
	return &identityAgentClient{done: make(chan struct{})}
}

func (c *identityAgentClient) Done() <-chan struct{} {
	return c.done
}

func (c *identityAgentClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	if c.updateErr != nil {
		return c.updateErr
	}
	c.notifications = append(c.notifications, notification)

	return nil
}

func (*identityAgentClient) NotifyExtension(context.Context, string, any) error {
	return nil
}

func responseAmpMessageID(t *testing.T, response acp.PromptResponse) string {
	t.Helper()

	ampMeta, ok := response.Meta[ampMetaKey].(map[string]any)
	require.True(t, ok, "PromptResponse missing _meta.amp: %#v", response.Meta)
	messageID, ok := ampMeta[metaMessageIDKey].(string)
	require.True(t, ok)
	require.NotEmpty(t, messageID)

	return messageID
}

func updateAmpMessageIDs(t *testing.T, notifications []acp.SessionNotification) []string {
	t.Helper()

	ids := make([]string, 0)
	for _, notification := range notifications {
		chunk := notification.Update.AgentMessageChunk
		if chunk == nil {
			continue
		}

		ampMeta, ok := chunk.Meta[ampMetaKey].(map[string]any)
		require.True(t, ok, "agent chunk missing _meta.amp: %#v", chunk.Meta)
		messageID, ok := ampMeta[metaMessageIDKey].(string)
		require.True(t, ok)
		require.NotNil(t, chunk.MessageId)
		require.Equal(t, messageID, *chunk.MessageId)
		ids = append(ids, messageID)
	}

	return ids
}

func waitForMessageIdentities(t *testing.T, client *recordingClient, count int) {
	t.Helper()

	waitForRecorded(t, func() bool {
		return len(updateAmpMessageIDs(t, client.updatesSnapshot())) >= count
	})
}

func TestAmpMessageMetaPreservesSiblingKeys(t *testing.T) {
	meta := map[string]any{
		"other": "kept",
		ampMetaKey: map[string]any{
			metaParentToolUseIDKey: "TU-parent",
		},
	}

	got := ampMessageMeta(meta, "message-id")
	require.Equal(t, "kept", got["other"])
	ampMeta, ok := got[ampMetaKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "TU-parent", ampMeta[metaParentToolUseIDKey])
	require.Equal(t, "message-id", ampMeta[metaMessageIDKey])
	require.Equal(t, got, ampMessageMeta(got, ""))
}

func TestTranscriptIdentityStateFailsClosed(t *testing.T) {
	ctx := context.Background()

	withoutStore := &agentSession{agent: newTestAgent()}
	entries, err := withoutStore.loadTranscript(ctx)
	require.NoError(t, err)
	require.Nil(t, entries)

	driftStore := NewInMemorySessionStore()
	require.NoError(t, driftStore.Append(ctx, SessionKey{SessionID: "T-drift", Subpath: transcriptSubpath}, []SessionStoreEntry{json.RawMessage(`{"type":"result"}`)}))
	driftSession := &agentSession{agent: newTestAgent(WithSessionStore(driftStore)), id: "T-drift"}
	err = driftSession.persistAfterTurn(ctx, nil)
	require.ErrorContains(t, err, "amp transcript frame count drift")

	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	baseStore := NewInMemorySessionStore()
	manifest, marshalErr := json.Marshal(ampManifest{
		Format:   SessionStoreFormat,
		ThreadID: "T-agent-thread",
		Cwd:      cwd,
	})
	require.NoError(t, marshalErr)
	require.NoError(t, baseStore.Replace(ctx, SessionKey{SessionID: "T-agent-thread"}, []SessionStoreReplacement{
		{Key: SessionKey{SessionID: "T-agent-thread"}, Entries: []SessionStoreEntry{manifest}},
	}))
	wantErr := errors.New("transcript unavailable")
	failingStore := &transcriptLoadErrorStore{InMemorySessionStore: baseStore, err: wantErr}
	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(failingStore),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	_, err = agent.ResumeSession(ctx, ResumeSessionRequest("T-agent-thread", cwd))
	require.ErrorIs(t, err, wantErr)
}

type transcriptLoadErrorStore struct {
	*InMemorySessionStore
	err error
}

func (s *transcriptLoadErrorStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if key.Subpath == transcriptSubpath {
		return nil, s.err
	}

	return s.InMemorySessionStore.Load(ctx, key)
}
