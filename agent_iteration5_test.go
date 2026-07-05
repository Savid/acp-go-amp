package ampacp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestIteration5ActiveLoadResumeY1Semantics(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	extra := t.TempDir()
	server := StdioMCPServer("stdio", "printf", []string{"ok"}, map[string]string{"A": "B"})
	options := NewAmpOptions(WithAmpEnv(map[string]string{"AMP_URL": "https://amp.example.test"}), WithAmpMode("deep"), WithAmpEffort("max"))
	requestOptions := func(raw bool) []SessionRequestOption {
		return []SessionRequestOption{
			WithSessionAdditionalDirectories(extra),
			WithSessionMCPServers(server),
			WithSessionAmpOptions(options),
			WithSessionRawEvents(raw),
		}
	}
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	resp, newErr := agent.NewSession(ctx, NewSessionRequest(cwd, requestOptions(false)...))
	if newErr != nil {
		t.Fatalf("NewSession: %v", newErr)
	}
	id := resp.SessionId

	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(id, t.TempDir(), requestOptions(false)...)); !isMismatchField(loadErr, "cwd") {
		t.Fatalf("different active cwd = %v, want cwd mismatch", loadErr)
	}
	if _, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd,
		WithSessionMCPServers(HTTPMCPServer("http", "https://example.test/mcp", nil)),
		WithSessionAdditionalDirectories(extra),
		WithSessionAmpOptions(options),
	)); !isMismatchField(resumeErr, "mcpServers") {
		t.Fatalf("different active mcp = %v, want mcpServers mismatch", resumeErr)
	}
	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(id, cwd,
		WithSessionAdditionalDirectories(t.TempDir()),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(options),
	)); !isMismatchField(loadErr, "additionalDirectories") {
		t.Fatalf("different active additionalDirectories = %v, want mismatch", loadErr)
	}
	if _, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd,
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_URL": "https://other.example.test"}), WithAmpMode("deep"), WithAmpEffort("max"))),
	)); !isMismatchField(resumeErr, "env") {
		t.Fatalf("different active env = %v, want env mismatch", resumeErr)
	}
	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(id, cwd,
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_URL": "https://amp.example.test"}), WithAmpMode("rush"), WithAmpEffort("max"))),
	)); !isMismatchField(loadErr, "mode") {
		t.Fatalf("different active mode = %v, want mode mismatch", loadErr)
	}
	if _, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd,
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_URL": "https://amp.example.test"}), WithAmpMode("deep"), WithAmpEffort("low"))),
	)); !isMismatchField(resumeErr, "effort") {
		t.Fatalf("different active effort = %v, want effort mismatch", resumeErr)
	}

	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd, requestOptions(true)...)); err != nil {
		t.Fatalf("active load applying raw events: %v", err)
	}
	session, err := agent.session(id)
	if err != nil {
		t.Fatalf("session lookup: %v", err)
	}
	if !session.rawEvents {
		t.Fatal("active load did not apply rawEvent=true")
	}
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, requestOptions(false)...)); err != nil {
		t.Fatalf("active resume applying raw events: %v", err)
	}
	if session.rawEvents {
		t.Fatal("active resume did not apply rawEvent=false")
	}
}

func TestIteration5ActiveLoadRetriesMirrorBeforeReplay(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	store := &flakyReplaceStore{InMemorySessionStore: NewInMemorySessionStore()}
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(store))
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := resp.SessionId

	store.failReplaces = 1
	if _, err := agent.Prompt(ctx, TextPromptRequest(id, "persist after native success")); err == nil {
		t.Fatal("prompt with failing persist returned no error")
	}
	store.failReplaces = 1
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd)); err == nil || !strings.Contains(err.Error(), "mirror_unsynced") {
		t.Fatalf("active LoadSession did not fail on retry outage: %v", err)
	}
	before := len(client.updatesSnapshot())
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd)); err != nil {
		t.Fatalf("active LoadSession after store recovery: %v", err)
	}
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) > before })
	if len(client.updatesSnapshot()) <= before {
		t.Fatal("active load replayed stale transcript before mirror retry")
	}
}

func TestIteration5ActiveLoadVerifiesContinuability(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "missing-export")
	cwd := t.TempDir()
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd)); err != nil {
		t.Fatalf("active LoadSession with missing native thread should replay only: %v", err)
	}
	if _, err := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "should fail")); err == nil || !strings.Contains(err.Error(), "native_state_missing") {
		t.Fatalf("prompt after active load missing export = %v, want native_state_missing", err)
	}
}

func TestIteration5ActiveLoadPropagatesContinuabilityFailure(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "export-fail")
	cwd := t.TempDir()
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd)); err == nil || !strings.Contains(err.Error(), "export failed") {
		t.Fatalf("active LoadSession export failure = %v, want export failed", err)
	}
}

func isMismatchField(err error, field string) bool {
	if err == nil {
		return false
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		return false
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		return strings.Contains(err.Error(), field) && strings.Contains(err.Error(), "mismatch")
	}

	return data[jsonFieldError] == "mismatch" && data[jsonFieldField] == field
}
