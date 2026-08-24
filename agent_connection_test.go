package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
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
