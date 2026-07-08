package ampacp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

// requireTurnFailure pins the uniform native-turn-failure shape: JSON-RPC -32603
// with data {error:"amp_turn_failed", cause:<class>, message:<real cause>}. The
// message must carry the real native cause, never a fixed placeholder or bare
// "EOF".
func requireTurnFailure(t *testing.T, err error, wantCause, wantMessageSubstr string) {
	t.Helper()
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error = %T %v, want RequestError", err, err)
	}
	if reqErr.Code != -32603 {
		t.Fatalf("code = %d, want -32603 (%v)", reqErr.Code, err)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %#v, want map", reqErr.Data)
	}
	if data[jsonFieldError] != turnFailedError {
		t.Fatalf("data.error = %#v, want %q", data[jsonFieldError], turnFailedError)
	}
	if data["cause"] != wantCause {
		t.Fatalf("data.cause = %#v, want %q", data["cause"], wantCause)
	}
	message, _ := data["message"].(string)
	if message == "" || message == "EOF" {
		t.Fatalf("data.message must be a real cause, got %q", message)
	}
	if !strings.Contains(message, wantMessageSubstr) {
		t.Fatalf("data.message = %q, want substring %q", message, wantMessageSubstr)
	}
}

// T1: a provider error inside the harness terminates session/prompt with the
// uniform failure error (cause "provider"), never a PromptResponse and never
// end_turn.
func TestTurnFailureProviderError(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		mode string
		want string
	}{
		{name: "generic", mode: "result-error", want: "native failed"},
		{name: "auth", mode: "provider-auth-error", want: "invalid API key"},
		{name: "rate limit", mode: "provider-rate-error", want: "429 too many requests"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := fakeAgentAmpPath(t, tc.mode)
			agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
			resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
			if promptErr == nil {
				t.Fatalf("provider error returned success: %#v", promptResp)
			}
			if promptResp.StopReason == acp.StopReasonEndTurn {
				t.Fatalf("provider failure reported as end_turn")
			}
			requireTurnFailure(t, promptErr, causeProvider, tc.want)
		})
	}
}

// L1: when result.error is empty the real cause is recovered from result.result.
func TestTurnFailureFallsBackToResultField(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "result-only-in-result")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
	requireTurnFailure(t, promptErr, causeProvider, "failure carried in result field")
}

// T2: a transport failure mid-turn surfaces the real cause, never a bare "EOF"
// or a fixed placeholder string.
func TestTurnFailureTransportRecoversCause(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		mode string
		want string
	}{
		{name: "stream ended", mode: "no-result", want: "stream ended without result"},
		{name: "malformed line", mode: "malformed-only", want: "decode amp json line"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := fakeAgentAmpPath(t, tc.mode)
			agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
			resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
			requireTurnFailure(t, promptErr, causeTransport, tc.want)
		})
	}
}

// T3: a non-zero process exit mid-turn surfaces cause "process_exit" with the
// exit/stderr cause, and the session stays addressable and retriable.
func TestTurnFailureProcessDeathIsRetriable(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "delayed-error")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
	requireTurnFailure(t, promptErr, causeProcessExit, "delayed failure")

	// The session is neither removed nor poisoned: it re-drives the native turn.
	if _, sessionErr := agent.session(resp.SessionId); sessionErr != nil {
		t.Fatalf("session removed after process death: %v", sessionErr)
	}
	_, retryErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "again"))
	requireTurnFailure(t, retryErr, causeProcessExit, "delayed failure")
}

// T4: a single malformed native line is a structured transport failure, never a
// process-exit misclassification and never a silent hang; the session survives.
func TestTurnFailureMalformedLineNotFatal(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "malformed-only")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
	requireTurnFailure(t, promptErr, causeTransport, "decode amp json line")

	if _, sessionErr := agent.session(resp.SessionId); sessionErr != nil {
		t.Fatalf("session torn down by malformed line: %v", sessionErr)
	}
	_, retryErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "again"))
	requireTurnFailure(t, retryErr, causeTransport, "decode amp json line")
}

// T5: a cancel delivered while the harness is failing yields StopReason
// cancelled with a nil error; the native failure is suppressed.
func TestTurnFailureCancelNotConflated(t *testing.T) {
	ctx := context.Background()
	path, state := fakeAgentAmpPath(t, "delayed-error")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
		resultCh <- promptResp
		errCh <- promptErr
	}()
	waitForPath(t, filepath.Join(state, "continue-ready"))
	if cancelErr := agent.Cancel(ctx, acp.CancelNotification{SessionId: resp.SessionId}); cancelErr != nil {
		t.Fatalf("cancel: %v", cancelErr)
	}
	select {
	case promptErr := <-errCh:
		promptResp := <-resultCh
		if promptErr != nil || promptResp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("cancel conflated with failure: resp=%#v err=%v", promptResp, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled prompt did not return")
	}
}

// T6: with WithTurnTimeout, a silent-hang harness yields cause "timeout" (a
// failure), never cancelled, and the prompt returns rather than hanging.
func TestTurnFailureTimeout(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "hang")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithTurnTimeout(150*time.Millisecond))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
		resultCh <- promptResp
		errCh <- promptErr
	}()
	select {
	case promptErr := <-errCh:
		promptResp := <-resultCh
		if promptResp.StopReason == acp.StopReasonCancelled {
			t.Fatalf("timeout reported as cancelled: %#v", promptResp)
		}
		requireTurnFailure(t, promptErr, causeTimeout, "WithTurnTimeout")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout prompt did not return")
	}
}

// TestFirstNonEmpty covers the local cause-selection helper, including the
// all-empty fallthrough.
func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("", " ", "value"); got != "value" {
		t.Fatalf("firstNonEmpty picked %q", got)
	}
	if got := firstNonEmpty("first", "second"); got != "first" {
		t.Fatalf("firstNonEmpty picked %q", got)
	}
	if got := firstNonEmpty("", "  "); got != "" {
		t.Fatalf("firstNonEmpty all-empty = %q", got)
	}
}
