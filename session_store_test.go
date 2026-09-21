package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestSnapshotMessageIdentity(t *testing.T) {
	t.Parallel()

	const id = "T-00000000-0000-0000-0000-000000000001"
	for _, tc := range []struct {
		name     string
		messages string
		valid    bool
	}{
		{"empty", `[]`, true},
		{"numeric without thread field", `[{"messageId":1,"role":"user","content":[{"type":"text","text":"hello"}]}]`, true},
		{"missing id", `[{"role":"user","content":[]}]`, false},
		{"null id", `[{"messageId":null,"role":"user","content":[]}]`, false},
		{"zero id", `[{"messageId":0,"role":"user","content":[]}]`, false},
		{"negative id", `[{"messageId":-1,"role":"user","content":[]}]`, false},
		{"fractional id", `[{"messageId":1.5,"role":"user","content":[]}]`, false},
		{"string id", `[{"messageId":"1","role":"user","content":[]}]`, false},
		{"duplicate id", `[{"messageId":1,"role":"user","content":[]},{"messageId":1,"role":"assistant","content":[]}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeSnapshot(fmt.Appendf(nil, `{"id":%q,"messages":%s}`, id, tc.messages), id)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestRestoreRefusesRemoteDivergenceWithoutChangingMirror(t *testing.T) {
	t.Parallel()

	for _, mutation := range []string{"shorter", "conflicting"} {
		for _, resume := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resume=%t", mutation, resume), func(t *testing.T) {
				t.Parallel()
				store := acpcore.NewInMemorySessionStore()
				native := t.TempDir()
				h := newHarness(t, WithSessionStore(store), WithEnv(map[string]string{"ACP_GO_AMP_TEST_NATIVE": native, "GORACE": "atexit_sleep_ms=0"}))
				h.initialize()
				session := h.newSession()
				cwd := sessionCwd(t, h, session.SessionId)
				_, err := h.prompt(session.SessionId, "retained history", nil)
				require.NoError(t, err)
				_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
				require.NoError(t, err)
				before, err := store.Load(t.Context(), string(session.SessionId))
				require.NoError(t, err)
				path := filepath.Join(native, string(session.SessionId)+".json")
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				var snapshot nativeSnapshot
				require.NoError(t, json.Unmarshal(data, &snapshot))
				switch mutation {
				case "shorter":
					snapshot.Messages = snapshot.Messages[:len(snapshot.Messages)-1]
					fakeSave(native, snapshot)
				case "conflicting":
					snapshot.Messages[0].Content[0]["text"] = "different history"
					fakeSave(native, snapshot)
				}
				if resume {
					_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd))
				} else {
					_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
				}
				require.Equal(t, "amp_restore_failed", requestErrorData(t, err)["error"])
				after, err := store.Load(t.Context(), string(session.SessionId))
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
		}
	}
}

type recoveryFaultStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *recoveryFaultStore) Replace(ctx context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("injected store failure")
	}

	return s.SessionStore.Replace(ctx, main, replacements)
}

func TestNativeBindingSurvivesLoadAndResume(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	var record sessionRecord
	rows, found, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, record.NativeSessionID)
	require.Equal(t, wire.NativeSessionMeta(vendor, record.NativeSessionID), created.Meta)
	id := acp.SessionId("acp-conversation-independent-of-native-id")
	record.SessionID = string(id)
	require.NoError(t, sessionlog.Commit(t.Context(), store, string(id), rows, record))
	require.NoError(t, store.Delete(t.Context(), acpcore.SessionKey{SessionID: string(created.SessionId)}))

	before := len(h.rec.snapshot())
	loaded, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(id, cwd))
	require.NoError(t, err)
	require.Equal(t, wire.NativeSessionMeta(vendor, record.NativeSessionID), loaded.Meta)
	_, err = h.prompt(id, "HELLO", nil)
	require.NoError(t, err)
	for _, update := range h.rec.snapshot()[before:] {
		require.Equal(t, id, update.SessionId)
	}
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, id, listed.Sessions[0].SessionId)
	require.Equal(t, loaded.Meta, listed.Sessions[0].Meta)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: id})
	require.NoError(t, err)
	listed, err = h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, loaded.Meta, listed.Sessions[0].Meta)
	resumed, err := h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(id, cwd))
	require.NoError(t, err)
	require.Equal(t, loaded.Meta, resumed.Meta)
	_, err = h.prompt(id, "HELLO", nil)
	require.NoError(t, err)
	var after sessionRecord
	_, found, err = sessionlog.Load(t.Context(), store, string(id), &after)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, record.NativeSessionID, after.NativeSessionID)
	require.Equal(t, string(id), after.SessionID)
}

type firstMirrorFailureStore struct {
	acpcore.SessionStore
	calls atomic.Int32
}

func (s *firstMirrorFailureStore) Replace(ctx context.Context, key acpcore.SessionKey, rows []acpcore.SessionStoreReplacement) error {
	if s.calls.Add(1) == 1 {
		return errors.New("initial mirror unavailable")
	}

	return s.SessionStore.Replace(ctx, key, rows)
}

func TestFailedNewSessionDoesNotPersistDuringCleanup(t *testing.T) {
	t.Parallel()
	store := &firstMirrorFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	response, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	require.Empty(t, response.SessionId)
	rows, err := store.ListSessions(h.ctx())
	require.NoError(t, err)
	require.Empty(t, rows)
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, listed.Sessions)
}

type blockedLoadStore struct {
	acpcore.SessionStore
	block            atomic.Bool
	entered, release chan struct{}
}

func (s *blockedLoadStore) Load(ctx context.Context, sessionID string) (map[string][]acpcore.SessionStoreEntry, error) {
	rows, err := s.SessionStore.Load(ctx, sessionID)
	if err == nil && s.block.CompareAndSwap(true, false) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return rows, err
}

func TestDeleteWinsAgainstPreparedLoad(t *testing.T) {
	t.Parallel()
	store := &blockedLoadStore{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan struct{}), release: make(chan struct{})}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	store.block.Store(true)
	done := make(chan error, 1)
	ctx := h.ctx()
	go func() {
		_, loadErr := h.conn.LoadSession(ctx, wire.LoadSessionRequest(session.SessionId, cwd))
		done <- loadErr
	}()
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("load did not reach stored configuration")
	}
	_, err = h.conn.UnstableDeleteSession(h.ctx(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
	close(store.release)
	require.NoError(t, err)
	require.Equal(t, "unknown session", requestErrorData(t, <-done)["error"])
	list, err := h.conn.ListSessions(h.ctx(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
}

// An ephemeral session is the host's probe: it runs and is deleted without the
// store ever seeing an entry or a tombstone for it, and it is never listed.
func TestEphemeralSessionNeverReachesTheStore(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()

	request := wire.NewSessionRequest(t.TempDir())
	request.Meta = wire.SessionMeta{Ephemeral: true}.Apply(request.Meta)
	ephemeral, err := h.conn.NewSession(h.ctx(), request)
	require.NoError(t, err)
	durable := h.newSession()

	for _, id := range []acp.SessionId{ephemeral.SessionId, durable.SessionId} {
		_, err = h.prompt(id, "HELLO", nil)
		require.NoError(t, err)
	}

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, list.Sessions, 1)
	require.Equal(t, durable.SessionId, list.Sessions[0].SessionId)

	stored, err := store.Load(t.Context(), string(ephemeral.SessionId))
	require.NoError(t, err)
	require.Nil(t, stored)
	stored, err = store.Load(t.Context(), string(durable.SessionId))
	require.NoError(t, err)
	require.NotNil(t, stored)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: ephemeral.SessionId})
	require.NoError(t, err)
	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(ephemeral.SessionId))
	require.NoError(t, err)
	summaries, err := store.ListSessions(t.Context())
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	require.Equal(t, string(durable.SessionId), summaries[0].SessionID)
	stored, err = store.Load(t.Context(), string(ephemeral.SessionId))
	require.NoError(t, err)
	require.Nil(t, stored, "no tombstone is written for a session the store never held")
	_, err = h.prompt(ephemeral.SessionId, "next", nil)
	require.Error(t, err)
}

// The host's session statement is read on session/new alone.
func TestSessionMetaRefusedOutsideSessionNew(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	cwd := sessionCwd(t, h, session.SessionId)
	meta := wire.SessionMeta{Ephemeral: true}.Apply(nil)

	load := wire.LoadSessionRequest(session.SessionId, cwd)
	load.Meta = meta
	_, err = h.conn.LoadSession(h.ctx(), load)
	require.Equal(t, "_meta."+wire.SessionMetaKey, requestErrorData(t, err)["field"])

	resume := wire.ResumeSessionRequest(session.SessionId, cwd)
	resume.Meta = meta
	_, err = h.conn.ResumeSession(h.ctx(), resume)
	require.Equal(t, "_meta."+wire.SessionMetaKey, requestErrorData(t, err)["field"])

	request := wire.NewSessionRequest(t.TempDir())
	request.Meta = map[string]any{wire.SessionMetaKey: map[string]any{"persist": false}}
	_, err = h.conn.NewSession(h.ctx(), request)
	require.Equal(t, "_meta."+wire.SessionMetaKey+".persist", requestErrorData(t, err)["field"])
}
