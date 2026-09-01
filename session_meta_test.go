package ampacp

import (
	"context"
	"encoding/json"
	"maps"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestSessionMetaStrictness(t *testing.T) {
	_, err := parseSessionMeta(map[string]any{"amp": map[string]any{"bad": true}})
	if err == nil {
		t.Fatal("expected unknown own namespace error")
	}
	meta, err := parseSessionMeta(map[string]any{
		"other": map[string]any{"ignored": true},
		"amp": map[string]any{
			"options":  map[string]any{"mode": "low"},
			"rawEvent": map[string]any{"enabled": true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if meta.options.Mode != "low" || !meta.rawEvent {
		t.Fatalf("bad meta: %+v", meta)
	}
}

func TestActiveOmittedEnvMeansDefaultEnv(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	extra := t.TempDir()
	server := StdioMCPServer("stdio", "printf", []string{"ok"}, map[string]string{"A": "B"})
	explicitOptions := NewAmpOptions(
		WithAmpEnv(map[string]string{"AMP_URL": "https://session.example.test"}),
		WithAmpMode("high"),
	)
	omittedEnvOptions := []SessionRequestOption{
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(NewAmpOptions(WithAmpMode("high"))),
	}

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithEnv(map[string]string{"AMP_API_KEY": "default"}),
	)
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd,
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(explicitOptions),
	))
	if err != nil {
		t.Fatalf("NewSession explicit env: %v", err)
	}
	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd, omittedEnvOptions...)); !isMismatchField(loadErr, "env") {
		t.Fatalf("active load omitted env = %v, want env mismatch", loadErr)
	}
	if _, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest(resp.SessionId, cwd, omittedEnvOptions...)); !isMismatchField(resumeErr, "env") {
		t.Fatalf("active resume omitted env = %v, want env mismatch", resumeErr)
	}

	defaultAgent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithEnv(map[string]string{"AMP_API_KEY": "default"}),
	)
	defaultResp, err := defaultAgent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession default env: %v", err)
	}
	if _, err := defaultAgent.LoadSession(ctx, LoadSessionRequest(defaultResp.SessionId, cwd)); err != nil {
		t.Fatalf("active load omitted env against default env: %v", err)
	}
	if _, err := defaultAgent.ResumeSession(ctx, ResumeSessionRequest(defaultResp.SessionId, cwd)); err != nil {
		t.Fatalf("active resume omitted env against default env: %v", err)
	}
}

func TestColdLoadReconstructsStoredEnvAndRefusesAChangedCarrier(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()
	created := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "create-default"}),
	)
	resp, err := created.NewSession(ctx, NewSessionRequest(cwd, WithSessionAmpOptions(NewAmpOptions(
		WithAmpEnv(map[string]string{"AMP_URL": "https://session.example.test", "PATH": "/create/bin"}),
	))))
	if err != nil {
		t.Fatalf("NewSession explicit env: %v", err)
	}
	if closeErr := created.Close(); closeErr != nil {
		t.Fatalf("Close created agent: %v", closeErr)
	}
	entries, err := store.Load(ctx, SessionKey{SessionID: string(resp.SessionId), Subpath: SessionStoreMainSubpath})
	if err != nil || len(entries) != 1 {
		t.Fatalf("load stored manifest: entries=%d err=%v", len(entries), err)
	}
	var manifest ampManifest
	if decodeErr := json.Unmarshal(entries[0], &manifest); decodeErr != nil {
		t.Fatalf("decode stored manifest: %v", decodeErr)
	}
	if want := map[string]string{"AMP_URL": "https://session.example.test", "PATH": "/create/bin"}; !maps.Equal(manifest.Env, want) {
		t.Fatalf("stored session env = %#v, want %#v", manifest.Env, want)
	}
	matching := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "matching-default"}),
	)
	if _, resumeErr := matching.ResumeSession(ctx, ResumeSessionRequest(resp.SessionId, cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{
			"AMP_URL": "https://session.example.test",
			"PATH":    "/create/bin",
		}))),
	)); resumeErr != nil {
		t.Fatalf("cold resume with matching env: %v", resumeErr)
	}
	if closeErr := matching.Close(); closeErr != nil {
		t.Fatalf("Close matching agent: %v", closeErr)
	}

	restored := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithEnv(map[string]string{
			"AMP_API_KEY":  "restore-default",
			"AMP_URL":      "https://default.example.test",
			"RESTORE_ONLY": "must-not-leak",
		}),
	)
	_, mismatchErr := restored.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{
			"AMP_API_KEY": "rotated",
			"PATH":        "/rotated/bin",
		}))),
	))
	if !isMismatchField(mismatchErr, optionEnvKey) {
		t.Fatalf("cold load with changed env = %v, want env mismatch", mismatchErr)
	}

	if _, loadErr := restored.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd)); loadErr != nil {
		t.Fatalf("cold load omitted env: %v", loadErr)
	}
	session, err := restored.session(resp.SessionId)
	if err != nil {
		t.Fatalf("restored session lookup: %v", err)
	}
	env := activeRequestEnv(session.env)
	if env["AMP_API_KEY"] != "restore-default" || env["AMP_URL"] != "https://session.example.test" || env["PATH"] != "/create/bin" || env["RESTORE_ONLY"] != "must-not-leak" {
		t.Fatalf("cold load env = %#v, want stored session env", env)
	}
}

// TestPromptErrorAfterCallerContextCancelInterrupts pins that a caller
// cancellation still answers cancelled when a native error follows it. The
// request context is the one the agent derives its operation context from, so
// the cancel is observed by the turn loop itself and the outcome is decided
// before settlement records one — never rewritten over a turn that already
// settled as failed.
func TestPromptErrorAfterCallerContextCancelInterrupts(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "delayed-error")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	promptCtx, cancelPrompt := context.WithCancel(context.Background())
	defer cancelPrompt()

	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(promptCtx, TextPromptRequest(resp.SessionId, "test-turn", "cancel before error"))
		resultCh <- result
		errCh <- promptErr
	}()

	waitForPath(t, filepath.Join(state, "continue-ready"))
	cancelPrompt()

	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if promptErr != nil || result.StopReason != acp.StopReasonCancelled {
			t.Fatalf("prompt error after caller cancel = %#v, %v", result, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prompt error after caller cancel did not return")
	}
}

func TestSessionPromptErrorAfterContextErrInterrupts(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "delayed-error")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	session, err := agent.session(resp.SessionId)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	promptCtx := &manualErrContext{}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := session.Prompt(promptCtx, TextPromptRequest(resp.SessionId, "test-turn", "cancel before error"))
		resultCh <- result
		errCh <- promptErr
	}()

	waitForPath(t, filepath.Join(state, "continue-ready"))
	promptCtx.cancel()

	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if promptErr != nil || result.StopReason != acp.StopReasonCancelled {
			t.Fatalf("session prompt error after context error = %#v, %v", result, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session prompt error after context error did not return")
	}
}

type manualErrContext struct {
	cancelled atomic.Bool
}

func (c *manualErrContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *manualErrContext) Done() <-chan struct{} { return nil }

func (c *manualErrContext) Err() error {
	if c.cancelled.Load() {
		return context.Canceled
	}

	return nil
}

func (c *manualErrContext) Value(any) any { return nil }

func (c *manualErrContext) cancel() { c.cancelled.Store(true) }
