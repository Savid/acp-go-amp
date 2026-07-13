package ampacp

import (
	"context"
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
			"options":  map[string]any{"mode": "rush", "effort": "low"},
			"rawEvent": map[string]any{"enabled": true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if meta.options.Mode != "rush" || meta.options.Effort != "low" || !meta.rawEvent {
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
		WithAmpMode("deep"),
		WithAmpEffort("max"),
	)
	omittedEnvOptions := []SessionRequestOption{
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(NewAmpOptions(WithAmpMode("deep"), WithAmpEffort("max"))),
	}

	agent := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
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

	defaultAgent := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
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

func TestColdLoadOmittedEnvBuildsDefaultEnv(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()
	created := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "create-default"}),
	)
	resp, err := created.NewSession(ctx, NewSessionRequest(cwd, WithSessionAmpOptions(NewAmpOptions(
		WithAmpEnv(map[string]string{"AMP_URL": "https://session.example.test"}),
	))))
	if err != nil {
		t.Fatalf("NewSession explicit env: %v", err)
	}
	if closeErr := created.Close(); closeErr != nil {
		t.Fatalf("Close created agent: %v", closeErr)
	}

	restored := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithSessionStore(store),
		WithEnv(map[string]string{
			"AMP_API_KEY": "restore-default",
			"AMP_URL":     "https://default.example.test",
		}),
	)
	if _, loadErr := restored.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd)); loadErr != nil {
		t.Fatalf("cold load omitted env: %v", loadErr)
	}
	session, err := restored.session(resp.SessionId)
	if err != nil {
		t.Fatalf("restored session lookup: %v", err)
	}
	env := activeRequestEnv(session.env)
	if env["AMP_API_KEY"] != "restore-default" || env["AMP_URL"] != "https://default.example.test" {
		t.Fatalf("cold load env = %#v, want restored default env", env)
	}
}

func TestPromptErrorAfterCallerContextCancelInterrupts(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "delayed-error")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	promptCtx := &manualErrContext{}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(promptCtx, TextPromptRequest(resp.SessionId, "cancel before error"))
		resultCh <- result
		errCh <- promptErr
	}()

	waitForPath(t, filepath.Join(state, "continue-ready"))
	promptCtx.cancel()

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
