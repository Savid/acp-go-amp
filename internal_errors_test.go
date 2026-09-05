package ampacp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// TestOffPromptInternalErrorsCarryTheClosedVocabulary drives every off-prompt
// `-32603` this adapter can reach and pins the closed token, the JSON-RPC
// message constant, the absence of any `message` member, and that no Go or
// native error text appears anywhere in `data`.
//
// Amp has no shared native runtime — one short-lived amp process serves one
// prompt and exits — so `amp_runtime_unavailable` has no condition to name here
// and is never emitted.
func TestOffPromptInternalErrorsCarryTheClosedVocabulary(t *testing.T) {
	for name, test := range map[string]struct {
		call func(t *testing.T) error
		want map[string]any
	}{
		"construction verdict on initialize": {
			call: func(t *testing.T) error {
				t.Helper()
				_, err := newTestAgent(WithInputHandoffRoot("relative/handoff")).
					Initialize(t.Context(), acp.InitializeRequest{})

				return err
			},
			want: map[string]any{jsonFieldError: errorInvalidOptions},
		},
		"construction verdict on session establishment": {
			call: func(t *testing.T) error {
				t.Helper()
				_, err := newTestAgent(WithInputHandoffRoot("relative/handoff")).
					NewSession(t.Context(), NewSessionRequest(t.TempDir()))

				return err
			},
			want: map[string]any{jsonFieldError: errorInvalidOptions},
		},
		"construction verdict naming the refused option": {
			call: func(t *testing.T) error {
				t.Helper()
				t.Setenv("AMP_API_KEY", "")
				_, err := newTestAgent(WithScratchDir(testScratchDir(t))).
					NewSession(t.Context(), NewSessionRequest(t.TempDir()))

				return err
			},
			want: map[string]any{jsonFieldError: errorInvalidOptions, jsonFieldField: optionEnvKey},
		},
		"unrestorable store entry": {
			call: func(t *testing.T) error {
				t.Helper()
				t.Setenv("AMP_API_KEY", "conformance-key")

				store := NewInMemorySessionStore()
				cwd := t.TempDir()
				putStoredSession(t, store, "T-agent-thread", cwd, nil)
				require.NoError(t, store.Append(t.Context(),
					SessionKey{SessionID: "T-agent-thread", Subpath: transcriptSubpath},
					[]SessionStoreEntry{json.RawMessage(`{ not json`)}))

				path, _ := fakeAgentAmpPath(t, "")
				agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
				t.Cleanup(func() { _ = agent.Close() })

				_, err := agent.LoadSession(t.Context(), LoadSessionRequest("T-agent-thread", cwd))

				return err
			},
			want: map[string]any{jsonFieldError: errorRestoreFailed},
		},
		"poisoned session": {
			call: func(t *testing.T) error {
				t.Helper()

				session := &agentSession{agent: newTestAgent(), id: "T-poisoned"}

				return session.poison(t.Context(), causeNativeIDDrift, `got "T-a", want "T-b"`)
			},
			want: map[string]any{jsonFieldError: errorSessionPoisoned, keyCause: causeNativeIDDrift},
		},
		"unclassified handler failure": {
			call: func(t *testing.T) error {
				t.Helper()

				return requestError(context.Background(), errNativeDeleteOpen)
			},
			want: map[string]any{jsonFieldError: errorInternalFailure, keyClass: classHandlerFailed},
		},
		"classified internal failure": {
			call: func(t *testing.T) error {
				t.Helper()

				session := &agentSession{agent: newTestAgent(), id: "T-pending", turn: make(chan struct{}, 1)}
				session.pendingTerminal = &promptTerminalDelivery{}

				return session.ready()
			},
			want: map[string]any{jsonFieldError: errorInternalFailure, keyClass: classTerminalDeliveryPending},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := test.call(t)

			var requestErr *acp.RequestError

			require.ErrorAs(t, err, &requestErr)
			require.Equal(t, -32603, requestErr.Code)
			require.Equal(t, "Internal error", requestErr.Message, "the JSON-RPC message stays the constant")

			data, ok := requestErr.Data.(map[string]any)
			require.True(t, ok)
			require.Equal(t, test.want, data)
			require.NotContains(t, data, keyMessage, "an off-prompt token never carries a message member")

			rendered, marshalErr := json.Marshal(data)
			require.NoError(t, marshalErr)

			// Whatever prose produced the failure stays in the log and on the
			// joined Go error; none of it survives into the wire payload.
			for _, leak := range []string{"relative/handoff", "AMP_API_KEY", "not json", "T-a", "delivery pending", errNativeDeleteOpen.Error()} {
				require.NotContains(t, string(rendered), leak)
			}

			token, isString := data[jsonFieldError].(string)
			require.True(t, isString)
			require.True(t, strings.HasPrefix(token, "amp_"), "every off-prompt token is vendor-prefixed")
		})
	}
}
