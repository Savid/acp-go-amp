package ampacp

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

type commitBarrier struct {
	acpcore.SessionStore
	block   atomic.Bool
	entered chan acpcore.SessionKey
	release chan struct{}
}

func (s *commitBarrier) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.block.CompareAndSwap(true, false) {
		s.entered <- key
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}
func TestEstablishmentExcludesPrompt(t *testing.T) {
	for _, phase := range []string{"creation", "cold_load"} {
		t.Run(phase, func(t *testing.T) {
			store := &commitBarrier{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan acpcore.SessionKey, 1), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(store.release) })
			h := newHarness(t, WithSessionStore(store))
			h.initialize()
			t.Cleanup(release)
			cwd := t.TempDir()
			var id acp.SessionId
			if phase == "cold_load" {
				created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
				require.NoError(t, err)
				id = created.SessionId
				_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: id})
				require.NoError(t, err)
			}
			store.block.Store(true)
			done := make(chan error, 1)
			ctx := h.ctx()
			go func() {
				if phase == "creation" {
					_, err := h.conn.NewSession(ctx, wire.NewSessionRequest(cwd))
					done <- err
				} else {
					_, err := h.conn.LoadSession(ctx, wire.LoadSessionRequest(id, cwd))
					done <- err
				}
			}()
			select {
			case key := <-store.entered:
				id = acp.SessionId(key.SessionID)
			case <-ctx.Done():
				t.Fatal("establishment never reached commit")
			}
			// An empty prompt cannot dispatch native work, but admission must still reject
			// it as busy before parsing content while establishment holds the session.
			_, err := h.conn.Prompt(ctx, wire.PromptRequest(id))
			data := requestErrorData(t, err)
			release()
			require.NoError(t, <-done)
			require.Equal(t, "session_prompt", data["limit"], "establishing session admitted a prompt into content validation: %v", data)
		})
	}
}

func TestFailedRestoreCloseReleasesSlot(t *testing.T) {
	for _, method := range []string{acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume} {
		t.Run(method, func(t *testing.T) {
			store := &recoveryFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
			h := newHarness(t, WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
			h.initialize()
			cwd := t.TempDir()
			created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
			require.NoError(t, err)
			before, err := store.Load(h.ctx(), string(created.SessionId))
			require.NoError(t, err)
			option := wire.WithSessionMetaValue(map[string]any{"amp": map[string]any{"options": map[string]any{"env": map[string]string{"RESTORE_TEST": "changed"}}}})
			store.fail.Store(true)
			if method == acp.AgentMethodSessionLoad {
				_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd, option))
			} else {
				_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd, option))
			}
			store.fail.Store(false)
			require.Error(t, err, "store failure must fail restore")
			after, err := store.Load(h.ctx(), string(created.SessionId))
			require.NoError(t, err)
			require.Equal(t, before, after, "failed teardown must retain the durable generation")
			_, err = h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err, "a failed restore-close leaked its active-session slot")
		})
	}
}

// limitedHarness serves an agent limited to one active session over a native
// root the test can inspect.
func limitedHarness(t *testing.T, env map[string]string) (h *harness, native string) {
	t.Helper()

	native = filepath.Join(t.TempDir(), "native")
	merged := map[string]string{"ACP_GO_AMP_TEST_NATIVE": native, "GORACE": "atexit_sleep_ms=0"}
	maps.Copy(merged, env)
	h = newHarness(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}), WithEnv(merged))
	h.initialize()

	return h, native
}

// nativeLaunches counts the scripted Amp processes started against native.
func nativeLaunches(native string) int {
	data, _ := os.ReadFile(filepath.Join(native, "launches"))

	return strings.Count(string(data), "\n")
}

func requireActiveSessionsBackpressure(t *testing.T, err error) {
	t.Helper()

	data := requestErrorData(t, err)
	require.Equal(t, "backpressure", data["error"])
	require.Equal(t, "active_sessions", data["limit"])
}

func TestActiveSessionLimitRefusesBeforeLaunch(t *testing.T) {
	t.Parallel()

	h, native := limitedHarness(t, nil)
	h.newSession()
	launches := nativeLaunches(native)

	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	requireActiveSessionsBackpressure(t, err)
	require.Equal(t, launches, nativeLaunches(native), "a refused session/new launched native work")
}

func TestActiveSessionLimitCountsEstablishing(t *testing.T) {
	t.Parallel()

	gate := filepath.Join(t.TempDir(), "gate")
	h, native := limitedHarness(t, map[string]string{"ACP_GO_AMP_TEST_NEW_GATE": gate})
	release := sync.OnceFunc(func() { require.NoError(t, os.WriteFile(gate, nil, 0o600)) })
	t.Cleanup(release)

	ctx := h.ctx()
	cwd := t.TempDir()
	done := make(chan error, 1)

	go func() {
		_, err := h.conn.NewSession(ctx, wire.NewSessionRequest(cwd))
		done <- err
	}()

	require.Eventually(t, func() bool { return nativeLaunches(native) == 1 }, testTimeout, 5*time.Millisecond)

	_, err := h.conn.NewSession(ctx, wire.NewSessionRequest(t.TempDir()))
	requireActiveSessionsBackpressure(t, err)
	require.Equal(t, 1, nativeLaunches(native), "a refused session/new launched native work")

	release()
	require.NoError(t, <-done)
}

func TestActiveSessionLimitFailedLaunchFreesSlot(t *testing.T) {
	t.Parallel()

	h, _ := limitedHarness(t, nil)
	fail := WithSessionAmpOptions(AmpOptions{Env: map[string]string{"ACP_GO_AMP_TEST_NEW_FAIL": "1"}})

	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir(), fail))
	require.Equal(t, "amp_internal_failure", requestErrorData(t, err)["error"])

	h.newSession()
}

func TestActiveSessionLimitRefusesRestoreBeforeLaunch(t *testing.T) {
	t.Parallel()

	for _, method := range []string{acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			h, native := limitedHarness(t, nil)
			cwd := t.TempDir()
			created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
			require.NoError(t, err)
			_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
			require.NoError(t, err)
			h.newSession()
			launches := nativeLaunches(native)

			if method == acp.AgentMethodSessionLoad {
				_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
			} else {
				_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd))
			}

			requireActiveSessionsBackpressure(t, err)
			require.Equal(t, launches, nativeLaunches(native), "a refused restore launched native work")
		})
	}
}
