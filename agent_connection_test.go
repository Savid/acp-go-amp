package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestLocalAgentConnectionHandleDispatch(t *testing.T) {
	ctx := context.Background()
	conn := &localAgentConnection{agent: NewAgent()}

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

func TestLocalAgentConnectionNotifyExtensionValidatesMethod(t *testing.T) {
	conn := &localAgentConnection{agent: NewAgent()}

	if err := conn.NotifyExtension(context.Background(), "", nil); err == nil {
		t.Fatal("empty extension method accepted")
	}
	if err := conn.NotifyExtension(context.Background(), "no-underscore", nil); err == nil {
		t.Fatal("non-underscore extension method accepted")
	}
}

func TestRequestErrorConversions(t *testing.T) {
	if requestError(nil) != nil {
		t.Fatal("nil error converted")
	}

	passthrough := acp.NewMethodNotFound("x")
	if got := requestError(passthrough); got != passthrough {
		t.Fatalf("request error not passed through: %#v", got)
	}

	if got := requestError(context.Canceled); got == nil || got.Code != -32800 {
		t.Fatalf("cancelled conversion = %#v", got)
	}

	if got := requestError(errors.New("boom")); got == nil || got.Code != -32603 {
		t.Fatalf("internal conversion = %#v", got)
	}
}
