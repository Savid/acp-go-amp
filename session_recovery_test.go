package ampacp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

type recoveryAPI struct {
	root    string
	server  *httptest.Server
	failure atomic.Int32
	imports atomic.Int32
}

func newRecoveryAPI(t *testing.T) *recoveryAPI {
	t.Helper()
	api := &recoveryAPI{root: t.TempDir()}
	api.server = httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(api.server.Close)

	return api
}

func (api *recoveryAPI) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer native-test-key" {
		w.WriteHeader(http.StatusUnauthorized)

		return
	}
	w.Header().Set("Content-Type", "application/json")
	write := func(value any) { _ = json.NewEncoder(w).Encode(value) }
	switch r.URL.Path {
	case "/api/internal":
		if api.failure.Load() == 1 {
			w.WriteHeader(http.StatusForbidden)

			return
		}
		var body struct {
			Params struct {
				Thread string `json:"thread"`
			} `json:"params"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		_, err := os.Stat(filepath.Join(api.root, body.Params.Thread+".json"))
		if os.IsNotExist(err) {
			write(map[string]any{"ok": false, "error": map[string]any{"code": "thread-not-found"}})

			return
		}
		write(map[string]any{"ok": true})
	case "/api/thread-actors":
		var body struct {
			ID string `json:"threadId"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		write(map[string]any{"threadId": body.ID, "wsToken": "thread-token", "threadVersion": 0})
	case "/actors/gateway/threadActor/request/import":
		api.imports.Add(1)
		if api.failure.Load() == 2 {
			w.WriteHeader(http.StatusConflict)

			return
		}
		if r.Header.Get("X-Rivet-Token") == "" || !strings.Contains(r.Header.Get("X-Rivet-Conn-Params"), "thread-token") {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}
		var body struct {
			Thread struct {
				ID       string `json:"id"`
				Messages []struct {
					ID      string           `json:"messageId"`
					Role    string           `json:"role"`
					Content []map[string]any `json:"content"`
					State   map[string]any   `json:"state"`
				} `json:"messages"`
			} `json:"thread"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Thread.ID != r.URL.Query().Get("rvt-key") {
			w.WriteHeader(http.StatusBadRequest)

			return
		}
		snapshot := nativeSnapshot{ID: body.Thread.ID, Messages: make([]nativeMessage, 0, len(body.Thread.Messages))}
		for index, message := range body.Thread.Messages {
			id, _ := json.Marshal(message.ID)
			if message.Role == roleAssistant && message.State == nil {
				message.State = map[string]any{"type": "complete"}
			}
			snapshot.Messages = append(snapshot.Messages, nativeMessage{ID: int64(index + 101), ProtocolID: id, Role: message.Role, Content: message.Content, State: message.State})
		}
		if api.failure.Load() == 3 && len(snapshot.Messages) > 0 {
			snapshot.Messages[0].Content[0]["text"] = "different history"
		}
		fakeSave(api.root, snapshot)
		fakeWrite(filepath.Join(api.root, snapshot.ID+".initializing"), nil)
		write(map[string]any{"ok": true, "importedMessages": len(body.Thread.Messages), "convertedSummaryMessages": 0})
	default:
		if after, ok := strings.CutPrefix(r.URL.Path, "/api/thread-actors/"); ok {
			write(map[string]any{"ok": true, "threadId": after})

			return
		}
		w.WriteHeader(http.StatusNotFound)
	}
}

func (api *recoveryAPI) options(store acpcore.SessionStore) []Option {
	return []Option{WithSessionStore(store), WithEnv(map[string]string{"AMP_URL": api.server.URL, "AMP_API_KEY": "native-test-key", "ACP_GO_AMP_TEST_NATIVE": api.root, "GORACE": "atexit_sleep_ms=0"})}
}

func TestMissingThreadRecoveryKeepsACPIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		resume, active bool
	}{
		{name: "closed-load"}, {name: "closed-resume", resume: true},
		{name: "active-load", active: true}, {name: "active-resume", active: true, resume: true},
	} {
		resume := tc.resume
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := newRecoveryAPI(t)
			store := acpcore.NewInMemorySessionStore()
			h := newHarness(t, api.options(store)...)
			h.initialize(withLifecycle())
			cwd := t.TempDir()
			created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
			require.NoError(t, err)
			_, err = h.prompt(created.SessionId, "HELLO", promptMeta(1))
			require.NoError(t, err)
			if !tc.active {
				_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
				require.NoError(t, err)
			}
			before, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			require.NoError(t, os.Remove(filepath.Join(api.root, string(created.SessionId)+".json")))
			updates := len(h.rec.snapshot())
			var meta map[string]any
			if resume {
				response, callErr := h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd))
				err = callErr
				meta = response.Meta
			} else {
				response, callErr := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
				err = callErr
				meta = response.Meta
			}
			require.NoError(t, err)
			binding, ok := meta[vendor].(map[string]any)
			require.True(t, ok)
			id, ok := binding["nativeSessionId"].(string)
			require.True(t, ok)
			require.NotEqual(t, string(created.SessionId), id)
			require.EqualValues(t, 1, api.imports.Load())
			var record sessionRecord
			rows, found, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, id, record.NativeSessionID)
			require.Equal(t, string(created.SessionId), record.SessionID)
			require.NotEmpty(t, record.Usage)
			snapshot, err := decodeSnapshot(rows[0], id)
			require.NoError(t, err)
			require.Len(t, snapshot.Messages, 2)
			require.Nil(t, snapshot.Messages[1].Usage)
			var used int
			for _, update := range h.rec.snapshot()[updates:] {
				require.Equal(t, created.SessionId, update.SessionId)
				if resume {
					require.Nil(t, update.Update.UserMessageChunk)
					require.Nil(t, update.Update.AgentMessageChunk)
				}
				if update.Update.UsageUpdate != nil {
					used = update.Update.UsageUpdate.Used
				}
			}
			if !resume {
				require.Equal(t, 13, used)
			}
			require.NotEqual(t, before[""], []acpcore.SessionStoreEntry{rows[0]})
			_, err = h.prompt(created.SessionId, "HELLO", promptMeta(2))
			require.NoError(t, err)
			_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
			require.NoError(t, err)
			response, err := h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd))
			require.NoError(t, err)
			require.Equal(t, meta, response.Meta)
			require.EqualValues(t, 1, api.imports.Load())
		})
	}
}

func TestRecoveryFailurePreservesCommittedBinding(t *testing.T) {
	t.Parallel()
	for _, scenario := range []int32{1, 2, 3, 4, 14} {
		failure := scenario % 10
		active := scenario >= 10
		names := map[int32]string{1: "authorization", 2: "import-conflict", 3: "history-mismatch", 4: "store-commit"}
		name := names[failure]
		if active {
			name = "active-" + name
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			api := newRecoveryAPI(t)
			store := &failingStore{SessionStore: acpcore.NewInMemorySessionStore()}
			h := newHarness(t, api.options(store)...)
			h.initialize()
			cwd := t.TempDir()
			created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
			require.NoError(t, err)
			_, err = h.prompt(created.SessionId, "HELLO", nil)
			require.NoError(t, err)
			if !active {
				_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
				require.NoError(t, err)
			}
			before, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			require.NoError(t, os.Remove(filepath.Join(api.root, string(created.SessionId)+".json")))
			api.failure.Store(failure)
			if failure == 4 {
				store.fail.Store(true)
			}
			_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
			require.Error(t, err)
			after, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			require.Equal(t, before, after)
			if active {
				listed, listErr := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
				require.NoError(t, listErr)
				require.Len(t, listed.Sessions, 1)
				require.Equal(t, created.Meta, listed.Sessions[0].Meta)
			}
			if failure == 1 {
				require.Zero(t, api.imports.Load())
			}
			files, globErr := filepath.Glob(filepath.Join(api.root, "*.json"))
			require.NoError(t, globErr)
			require.Empty(t, files, "an unbound recovery destination is deleted; nothing else is created")
			deleted, globErr := filepath.Glob(filepath.Join(api.root, "*.deleted"))
			require.NoError(t, globErr)
			require.Len(t, deleted, map[bool]int{true: 0, false: 1}[failure == 1], "only a created destination is deleted")
			api.failure.Store(0)
			store.fail.Store(false)
			_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd))
			require.NoError(t, err)
			_, err = h.prompt(created.SessionId, "HELLO", nil)
			require.NoError(t, err)
		})
	}
}
