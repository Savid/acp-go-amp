package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestLocalAgentConnectionHandleDispatch(t *testing.T) {
	ctx := context.Background()
	conn := &localAgentConnection{agent: newTestAgent()}

	// Every non-initialize method is rejected until initialize succeeds.
	_, reqErr := conn.handle(ctx, acp.AgentMethodSessionNew, json.RawMessage(`{}`))
	if reqErr == nil || reqErr.Code != -32600 {
		t.Fatalf("pre-initialize dispatch = %#v", reqErr)
	}

	result, reqErr := conn.handle(ctx, acp.AgentMethodInitialize, json.RawMessage(`{"protocolVersion":1}`))
	if reqErr != nil {
		t.Fatalf("initialize dispatch: %v", reqErr)
	}
	if _, ok := result.(acp.InitializeResponse); !ok || !conn.initialized.Load() {
		t.Fatalf("initialize result = %#v initialized=%v", result, conn.initialized.Load())
	}

	// Unknown methods are method-not-found after dispatch, wire and extension.
	_, reqErr = conn.handle(ctx, "unknown/method", nil)
	if reqErr == nil || reqErr.Code != -32601 {
		t.Fatalf("unknown method = %#v", reqErr)
	}
	_, reqErr = conn.handle(ctx, "_amp/unknown", nil)
	if reqErr == nil || reqErr.Code != -32601 {
		t.Fatalf("unknown extension = %#v", reqErr)
	}

	// The namespaced fork extension routes through HandleExtensionMethod.
	rawFork, err := json.Marshal(ForkSessionRequest("T-1", "/tmp/cwd"))
	if err != nil {
		t.Fatal(err)
	}
	_, reqErr = conn.handle(ctx, ForkSessionMethod, rawFork)
	if reqErr == nil || reqErr.Code != -32602 {
		t.Fatalf("fork extension = %#v", reqErr)
	}

	// Malformed params and failed validation both reject with invalid params.
	_, reqErr = conn.handle(ctx, acp.AgentMethodSessionNew, json.RawMessage(`{bad`))
	if reqErr == nil || reqErr.Code != -32602 {
		t.Fatalf("malformed params = %#v", reqErr)
	}
	_, reqErr = conn.handle(ctx, acp.AgentMethodSessionNew, json.RawMessage(`{"cwd":"relative"}`))
	if reqErr == nil || reqErr.Code != -32602 {
		t.Fatalf("invalid params = %#v", reqErr)
	}

	// Notification handlers decode, validate, and surface handler errors.
	_, reqErr = conn.handle(ctx, acp.AgentMethodSessionCancel, json.RawMessage(`{bad`))
	if reqErr == nil || reqErr.Code != -32602 {
		t.Fatalf("malformed cancel = %#v", reqErr)
	}
	_, reqErr = conn.handle(ctx, acp.AgentMethodSessionCancel, json.RawMessage(`{"sessionId":"T-missing"}`))
	if reqErr == nil || reqErr.Code != -32602 {
		t.Fatalf("unknown-session cancel = %#v", reqErr)
	}

	// A handler error on the response path converts through requestError.
	_, reqErr = conn.handle(ctx, acp.AgentMethodSessionPrompt, json.RawMessage(`{"sessionId":"T-missing","prompt":[{"type":"text","text":"x"}]}`))
	if reqErr == nil || reqErr.Code != -32602 {
		t.Fatalf("unknown-session prompt = %#v", reqErr)
	}
}

func TestLocalAgentConnectionClosedWinsBeforeDispatchAndDecode(t *testing.T) {
	ctx := context.Background()
	agent := newTestAgent()
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}

	conn := &localAgentConnection{agent: agent}
	conn.initialized.Store(true)

	for name, request := range map[string]struct {
		method string
		params json.RawMessage
	}{
		"initialize":        {method: acp.AgentMethodInitialize, params: json.RawMessage(`{bad`)},
		"known malformed":   {method: acp.AgentMethodSessionNew, params: json.RawMessage(`{bad`)},
		"unknown stable":    {method: "unknown/method", params: json.RawMessage(`{bad`)},
		"unknown extension": {method: "_amp/unknown", params: json.RawMessage(`{bad`)},
	} {
		t.Run(name, func(t *testing.T) {
			_, reqErr := conn.handle(ctx, request.method, request.params)
			if reqErr == nil || reqErr.Code != -32600 || !strings.Contains(reqErr.Error(), "agent closed") {
				t.Fatalf("closed dispatch = %#v", reqErr)
			}
		})
	}
}

func TestLocalAgentConnectionNotifyExtensionValidatesMethod(t *testing.T) {
	conn := &localAgentConnection{agent: newTestAgent()}

	if err := conn.NotifyExtension(context.Background(), "", nil); err == nil {
		t.Fatal("empty extension method accepted")
	}
	if err := conn.NotifyExtension(context.Background(), "no-underscore", nil); err == nil {
		t.Fatal("non-underscore extension method accepted")
	}
}

func TestRequestErrorConversions(t *testing.T) {
	live := t.Context()

	if requestError(live, nil) != nil {
		t.Fatal("nil error converted")
	}

	passthrough := acp.NewMethodNotFound("x")
	if got := requestError(live, passthrough); got != passthrough {
		t.Fatalf("request error not passed through: %#v", got)
	}

	if got := requestError(live, errors.New("boom")); got == nil || got.Code != -32603 {
		t.Fatalf("internal conversion = %#v", got)
	}

	// A cancellation reached by unwrapping the error is not an honored cancel:
	// the peer never withdrew this request, so it stays an internal failure.
	if got := requestError(live, fmt.Errorf("read prompt: %w", context.Canceled)); got == nil || got.Code != -32603 {
		t.Fatalf("wrapped cancellation conversion = %#v", got)
	}
}

// TestRequestErrorReportsAnHonoredCancelAheadOfAnEmbeddedRequestError pins the
// discriminator: only a request context whose cause is context.Canceled is an
// honored $/cancel_request, and it outranks any typed error the aborted work
// joined on its way out. Amp joins a backpressure -32600 with the teardown error
// on the session-establishing paths, so an unguarded errors.As would answer a
// withdrawn request with an error about its parameters.
func TestRequestErrorReportsAnHonoredCancelAheadOfAnEmbeddedRequestError(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(context.Canceled)

	for name, err := range map[string]error{
		"embedded request error": errors.Join(backpressureError("active_sessions"), context.Canceled),
		"plain failure":          errors.New("boom"),
	} {
		t.Run(name, func(t *testing.T) {
			if got := requestError(ctx, err); got == nil || got.Code != -32800 {
				t.Fatalf("honored cancel conversion = %#v", got)
			}
		})
	}
}

// TestRequestErrorReportsATornDownConnectionByItsOwnError pins that a connection
// teardown is not a cancel. The SDK cancels the parent context with the
// transport cause rather than context.Canceled, so a request aborted by it
// reports what actually failed instead of -32800.
func TestRequestErrorReportsATornDownConnectionByItsOwnError(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)

	cancel(errors.New("peer connection closed"))

	invalid := acp.NewInvalidParams(map[string]any{jsonFieldField: fieldPrompt})
	if got := requestError(ctx, errors.Join(invalid, context.Canceled)); got != invalid {
		t.Fatalf("torn-down conversion = %#v", got)
	}

	if got := requestError(ctx, errors.New("boom")); got == nil || got.Code != -32603 {
		t.Fatalf("torn-down internal conversion = %#v", got)
	}
}

// TestRequestErrorReportsAnExpiredDeadlineAsAnInternalFailure pins that an
// adapter-internal deadline is a failure of the turn and never a cancel: its
// cause is context.DeadlineExceeded, which the cancel discriminator excludes by
// name rather than by accident of error matching.
func TestRequestErrorReportsAnExpiredDeadlineAsAnInternalFailure(t *testing.T) {
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	if got := requestError(ctx, context.DeadlineExceeded); got == nil || got.Code != -32603 {
		t.Fatalf("deadline conversion = %#v", got)
	}
}

// TestLocalAgentConnectionSerializesAnHonoredCancel proves the request context
// reaches the error mapper through dispatch: the same call answers -32602 on a
// live context and -32800 once the peer has withdrawn it.
func TestLocalAgentConnectionSerializesAnHonoredCancel(t *testing.T) {
	conn := &localAgentConnection{agent: newTestAgent()}
	conn.initialized.Store(true)

	params := json.RawMessage(`{"sessionId":"T-missing"}`)

	if _, reqErr := conn.handle(t.Context(), acp.AgentMethodSessionClose, params); reqErr == nil || reqErr.Code != -32602 {
		t.Fatalf("live unknown-session close = %#v", reqErr)
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(context.Canceled)

	if _, reqErr := conn.handle(ctx, acp.AgentMethodSessionClose, params); reqErr == nil || reqErr.Code != -32800 {
		t.Fatalf("cancelled unknown-session close = %#v", reqErr)
	}
}

// TestLocalAgentConnectionAnswersAnHonoredNotification pins the notification
// handler's own success answer through dispatch. Over the wire a notification is
// fire-and-forget: the peer's send returns before the handler runs, so nothing a
// caller observes proves the handler ever finished, and only dispatching one
// directly settles what an honored notification answers with.
func TestLocalAgentConnectionAnswersAnHonoredNotification(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	t.Cleanup(func() { _ = agent.Close() })

	conn := &localAgentConnection{agent: agent}
	conn.initialized.Store(true)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	params, err := json.Marshal(acp.CancelNotification{SessionId: created.SessionId})
	if err != nil {
		t.Fatal(err)
	}

	resp, reqErr := conn.handle(t.Context(), acp.AgentMethodSessionCancel, params)
	if reqErr != nil || resp != nil {
		t.Fatalf("honored cancel = %#v, %#v", resp, reqErr)
	}
}

// These requests go through Serve and the SDK transport with their original
// duplicate members and number lexemes, rather than predecoded Go maps.
func TestServePreservesOwnedLifecycleMetadata(t *testing.T) {
	for _, tc := range []struct{ name, meta, field string }{
		{"exact", `"_meta":{"acp-go.dev/lifecycle":{"version":1}}`, ""},
		{"duplicate version", `"_meta":{"acp-go.dev/lifecycle":{"version":2,"version":1}}`, ".version"},
		{"near integer", `"_meta":{"acp-go.dev/lifecycle":{"version":1.0000000000000001}}`, ".version"},
		{"float overflow", `"_meta":{"acp-go.dev/lifecycle":{"version":1e400}}`, ".version"},
		{"metadata alias", `"_META":{"acp-go.dev/lifecycle":{"version":1e400}}`, ".version"},
		{"alias erasure", `"_META":{"acp-go.dev/lifecycle":{"version":2}},"_meta":null`, "root"},
		{"int wrap", `"_meta":{"acp-go.dev/lifecycle":{"version":4294967297}}`, ".version"},
		{"duplicate namespace", `"_meta":{"acp-go.dev/lifecycle":{"version":2},"acp-go.dev/lifecycle":{"version":1}}`, "root"},
		{"erased envelope", `"_meta":{"acp-go.dev/lifecycle":{"version":2}},"_meta":null`, "root"},
		{"foreign only", `"_meta":{"foreign":{"version":2,"version":1}},"_meta":{"foreign":1}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := lifecycleWireConnection(t, newTestAgent())
			_, err := acp.SendRequest[json.RawMessage](conn, t.Context(), acp.AgentMethodInitialize,
				json.RawMessage(`{"protocolVersion":1,`+tc.meta+`}`))
			if tc.field == "" {
				require.NoError(t, err)

				return
			}
			field := lifecycle.MetaPath
			if tc.field != "root" {
				field += tc.field
			}
			requireInvalidParamsData(t, err, map[string]any{"error": "unsupported", "field": field})
		})
	}
}

func TestServeLifecycleCorrelationAndForbiddenSurfaces(t *testing.T) {
	agent := newTestAgent(WithEnv(map[string]string{"AMP_API_KEY": "fake"}), WithScratchDir(t.TempDir()))
	session, err := newAgentSession(t.Context(), agent, "wire-session", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	agent.mu.Lock()
	agent.activateSessionLocked(session)
	agent.mu.Unlock()
	conn := lifecycleWireConnection(t, agent)
	_, err = acp.SendRequest[json.RawMessage](conn, t.Context(), acp.AgentMethodInitialize,
		json.RawMessage(`{"protocolVersion":1,"_meta":{"acp-go.dev/lifecycle":{"version":1}}}`))
	require.NoError(t, err)
	for _, tc := range []struct{ name, meta, field, verdict string }{
		{"duplicate submission", `"_meta":{"acp-go.dev/lifecycle":{"version":1,"submission":{"submissionId":"first","submissionId":"second","clientNonce":"nonce"}}}`, ".submission.submissionId", "unsupported"},
		{"duplicate correlation", `"_meta":{"acp-go.dev/lifecycle":{"version":1,"submission":{},"submission":{"submissionId":"s","clientNonce":"n"}}}`, ".submission", "unsupported"},
		{"near integer", `"_meta":{"acp-go.dev/lifecycle":{"version":1.0000000000000001}}`, ".version", "unsupported"},
		{"overflow", `"_META":{"acp-go.dev/lifecycle":{"version":1e400}}`, ".version", "unsupported"},
		{"erased", `"_meta":{"acp-go.dev/lifecycle":{"version":1}},"_meta":{}`, "", "unsupported"},
		{"missing", `"_meta":{"foreign":true}`, "", "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, promptErr := acp.SendRequest[json.RawMessage](conn, t.Context(), acp.AgentMethodSessionPrompt,
				json.RawMessage(`{"sessionId":"wire-session","prompt":[],`+tc.meta+`}`))
			requireInvalidParamsData(t, promptErr, map[string]any{"error": tc.verdict, "field": lifecycle.MetaPath + tc.field})
		})
	}
	// A valid correlation reaches image validation without launching native work.
	_, err = acp.SendRequest[json.RawMessage](conn, t.Context(), acp.AgentMethodSessionPrompt,
		json.RawMessage(`{"sessionId":"wire-session","prompt":[{"type":"image","data":"","mimeType":"image/png"}],"_meta":{"acp-go.dev/lifecycle":{"version":1,"submission":{"submissionId":"s","clientNonce":"n"}},"foreign":{"x":1,"x":2}}}`))
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.Contains(t, requestErr.Error(), imageErrorMissingData)
	for _, method := range []string{acp.AgentMethodSessionClose, "_amp/unknown"} {
		_, err = acp.SendRequest[json.RawMessage](conn, t.Context(), method,
			json.RawMessage(`{"sessionId":"wire-session","_meta":{"acp-go.dev/lifecycle":null},"_meta":{}}`))
		requireInvalidParamsData(t, err, map[string]any{"error": "unsupported", "field": lifecycle.MetaPath})
	}
}

func TestServeOwnedMetadataPreservesConstructionPrecedence(t *testing.T) {
	conn := lifecycleWireConnection(t, newTestAgent(WithHostAuthority(nil)))
	_, err := acp.SendRequest[json.RawMessage](conn, t.Context(), acp.AgentMethodInitialize,
		json.RawMessage(`{"protocolVersion":1,"_meta":{"acp-go.dev/lifecycle":{"version":1e400}}}`))
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, -32603, requestErr.Code)
}

func TestServeHandoffNumbersRetainTheirExactValues(t *testing.T) {
	root := t.TempDir()
	agent := newTestAgent(WithEnv(map[string]string{"AMP_API_KEY": "fake"}), WithScratchDir(t.TempDir()), WithInputHandoffRoot(root))
	session, err := newAgentSession(t.Context(), agent, "wire-image", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	agent.mu.Lock()
	agent.activateSessionLocked(session)
	agent.mu.Unlock()
	var launched atomic.Int64
	agent.options.runtime.executeThread = func(context.Context, *nativeamp.Client, any) (*nativeamp.Turn, error) {
		launched.Add(1)

		return nil, errors.New("deterministic admission refusal")
	}
	conn := lifecycleWireConnection(t, agent)
	_, err = acp.SendRequest[json.RawMessage](conn, t.Context(), acp.AgentMethodInitialize, json.RawMessage(`{"protocolVersion":1}`))
	require.NoError(t, err)
	uri, err := json.Marshal(fileURI(filepath.Join(root, "absent.png")))
	require.NoError(t, err)
	for _, tc := range []struct{ name, version, size, data, want string }{
		{"version fraction", "1.0000000000000001", "1", "", handoffVersionInvalidMessage},
		{"version overflow", "1e400", "1", "", handoffVersionInvalidMessage},
		{"size fraction", "1", "1.0000000000000001", "", handoffSizeBytesInvalidMessage},
		{"size underflow", "1", "-1e-400", "", handoffSizeBytesInvalidMessage},
		{"size overflow", "1", "1e400", "", handoffSizeBytesInvalidMessage},
		{"integral forms", "1.0", "1e0", "", handoffFileAbsentMessage},
		{"duplicate allowed", `2,"version":1`, "1", "", handoffFileAbsentMessage},
		{"embedded dominance", "1e400", "-1e-400", validPNGBase64, turnFailedError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := fmt.Sprintf(`{"sessionId":"wire-image","prompt":[{"type":"image","mimeType":"image/png","data":%q,"uri":%s,"_META":{"acp-go.dev/handoff":{"version":%s,"sizeBytes":%s,"digest":%q}}}]}`, tc.data, uri, tc.version, tc.size, strings.Repeat("0", 64))
			_, promptErr := acp.SendRequest[json.RawMessage](conn, t.Context(), acp.AgentMethodSessionPrompt, json.RawMessage(raw))
			require.Error(t, promptErr)
			require.Contains(t, promptErr.Error(), tc.want)
		})
	}
	require.EqualValues(t, 1, launched.Load(), "only embedded data reaches the deterministic native boundary")
}

func lifecycleWireConnection(t *testing.T, agent *Agent) *acp.Connection {
	t.Helper()
	original := newAgentForServe
	newAgentForServe = func(...Option) *Agent { return agent }
	t.Cleanup(func() { newAgentForServe = original })
	ctx, cancel := context.WithCancel(t.Context())
	input, send := io.Pipe()
	receive, output := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, input, output) }()
	conn := acp.NewConnection(func(context.Context, string, json.RawMessage) (any, *acp.RequestError) { return nil, nil }, send, receive)
	t.Cleanup(func() {
		cancel()
		_ = send.Close()
		_ = input.Close()
		_ = output.Close()
		_ = receive.Close()
		_ = receiveCorrection(t, done, "Serve teardown")
	})

	return conn
}
