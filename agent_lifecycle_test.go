//nolint:gocyclo,errcheck,govet,nlreturn,nilnil // Fake-process conformance tests are intentionally branch-heavy.
package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestFakeAmpAgentHelper(t *testing.T) {
	if os.Getenv("GO_WANT_FAKE_AMP_AGENT") != "1" {
		return
	}
	args := helperArgs()
	state := os.Getenv("FAKE_AMP_AGENT_STATE")
	mode := os.Getenv("FAKE_AMP_AGENT_MODE")
	recordHelperJSON(state, "args.jsonl", args)

	if len(args) > 0 && args[0] == "version" {
		os.Stdout.WriteString("0.0.1783155105-gfake\n")
		os.Exit(0)
	}
	if len(args) > 0 && args[len(args)-1] == "--help" {
		os.Stdout.WriteString("--settings-file --mcp-config -m --effort --json --stream-json-input threads continue threads export threads delete\n")
		os.Exit(0)
	}
	threads := slices.Index(args, "threads")
	if threads < 0 || threads+1 >= len(args) {
		os.Stderr.WriteString("missing threads subcommand\n")
		os.Exit(2)
	}
	switch args[threads+1] {
	case "new":
		if mode == "bad-new-id" {
			os.Stdout.WriteString("not-a-thread\n")
			return
		}
		os.Stdout.WriteString("T-agent-thread\n")
	case "export":
		if mode == "bad-export-json" {
			os.Stdout.WriteString("{")
			os.Exit(0)
		}
		os.Stdout.WriteString(`{"thread":"` + args[len(args)-1] + `"}` + "\n")
	case "delete":
		if mode == "delete-fail" {
			os.Stderr.WriteString("delete failed\n")
			os.Exit(1)
		}
		os.Stdout.WriteString("deleted\n")
	case "continue":
		stdin, _ := io.ReadAll(os.Stdin)
		recordHelperJSON(state, "stdin.jsonl", strings.TrimSpace(string(stdin)))
		switch mode {
		case "missing":
			os.Stderr.WriteString("Thread not found\n")
			os.Exit(1)
		default:
			os.Stderr.WriteString("native stderr noise\n")
			os.Stdout.WriteString("native stdout noise\n")
			os.Stdout.WriteString(`{"type":"system","subtype":"init","cwd":"/tmp/project","session_id":"T-agent-thread","tools":["Read"],"mcp_servers":[{"name":"svc","status":"connected"}],"agent_mode":"smart","reasoning_effort":"high"}` + "\n")
			os.Stdout.WriteString(`{"type":"user","message":{"content":[{"type":"text","text":"echoed user"},{"type":"tool_result","tool_use_id":"TU-1","content":"tool output","is_error":true}]},"session_id":"T-agent-thread"}` + "\n")
			os.Stdout.WriteString(`{"type":"assistant","message":{"content":[{"type":"text","text":"agent text"},{"type":"tool_use","id":"TU-1","name":"Read","input":{"path":"README.md"}}],"usage":{"input_tokens":3,"cache_creation_input_tokens":5,"cache_read_input_tokens":7,"output_tokens":11,"max_tokens":200,"service_tier":"standard"}},"session_id":"T-agent-thread"}` + "\n")
			os.Stdout.WriteString(`{"type":"result","subtype":"success","duration_ms":1,"is_error":false,"num_turns":1,"result":"done","session_id":"T-agent-thread","usage":{"input_tokens":13,"output_tokens":17,"max_tokens":300}}` + "\n")
		}
	default:
		os.Stderr.WriteString("unknown threads subcommand\n")
		os.Exit(2)
	}
	os.Exit(0)
}

func TestServeFakeAmpLifecycleStdoutCleanStoreReplayAndDelete(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()
	conn, client, cleanup := startTestServe(t,
		WithExecutablePath(path),
		WithHome(t.TempDir()),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	defer cleanup()
	ctx := context.Background()

	initResp, err := conn.Initialize(ctx, acp.InitializeRequest{
		ClientCapabilities: acp.ClientCapabilities{
			PositionEncodings: []acp.PositionEncodingKind{acp.PositionEncodingKindUtf8},
			Elicitation:       &acp.ElicitationCapabilities{},
		},
	})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if initResp.AgentCapabilities.PositionEncoding == nil || *initResp.AgentCapabilities.PositionEncoding != acp.PositionEncodingKindUtf8 {
		t.Fatalf("position encoding = %#v", initResp.AgentCapabilities.PositionEncoding)
	}
	ampMeta := initResp.AgentCapabilities.Meta[ampMetaKey].(map[string]any)
	if _, ok := ampMeta["elicitation"]; ok {
		t.Fatalf("amp elicitation advertised: %#v", ampMeta)
	}
	if _, ok := ampMeta["fork"]; ok {
		t.Fatalf("amp fork advertised: %#v", ampMeta)
	}

	_, stableForkErr := conn.UnstableForkSession(ctx, ForkSessionRequest("T-agent-thread", cwd))
	requireRequestErrorCode(t, stableForkErr, -32601)

	sessionOptions := []SessionRequestOption{
		WithSessionRawEvents(true),
		WithSessionAdditionalDirectories("/tmp/other"),
		WithSessionAmpOptions(NewAmpOptions(
			WithAmpEnv(map[string]string{"AMP_URL": "https://amp.example.test"}),
			WithAmpMode("deep"),
			WithAmpEffort("max"),
		)),
		WithSessionMCPServers(
			StdioMCPServer("stdio", "printf", []string{"ok"}, map[string]string{"A": "B"}),
			HTTPMCPServer("http", "https://example.com/mcp", map[string]string{"H": "V"}),
		),
	}
	newResp, err := conn.NewSession(ctx, NewSessionRequest(cwd, sessionOptions...))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if newResp.SessionId != "T-agent-thread" {
		t.Fatalf("session id = %q", newResp.SessionId)
	}
	messageID := "00000000-0000-4000-8000-000000000001"
	promptResp, err := conn.Prompt(ctx, acp.PromptRequest{
		SessionId: newResp.SessionId,
		MessageId: &messageID,
		Prompt:    []acp.ContentBlock{acp.TextBlock("/review ordinary prompt")},
	})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if promptResp.StopReason != acp.StopReasonEndTurn || promptResp.UserMessageId == nil || *promptResp.UserMessageId != messageID {
		t.Fatalf("prompt response = %#v", promptResp)
	}
	if promptResp.Usage == nil || promptResp.Usage.TotalTokens != 30 {
		t.Fatalf("prompt usage = %#v", promptResp.Usage)
	}
	if client.permissionCalls != 0 || client.elicitationCalls != 0 {
		t.Fatalf("amp emitted permission=%d elicitation=%d", client.permissionCalls, client.elicitationCalls)
	}
	requireUpdateKinds(t, client.updatesSnapshot(),
		"user_message_chunk",
		"tool_call_update",
		"agent_message_chunk",
		"tool_call",
		"usage_update",
		"usage_update",
	)
	if raw := client.rawSnapshot(); len(raw) < 4 {
		t.Fatalf("raw events = %d", len(raw))
	}

	beforeLoad := len(client.updatesSnapshot())
	if _, err := conn.LoadSession(ctx, LoadSessionRequest(newResp.SessionId, cwd, sessionOptions...)); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	afterLoad := len(client.updatesSnapshot())
	if afterLoad <= beforeLoad {
		t.Fatalf("load did not replay transcript: before=%d after=%d", beforeLoad, afterLoad)
	}
	if _, err := conn.ResumeSession(ctx, ResumeSessionRequest(newResp.SessionId, cwd, sessionOptions...)); err != nil {
		t.Fatalf("ResumeSession: %v", err)
	}
	if got := len(client.updatesSnapshot()); got != afterLoad {
		t.Fatalf("resume replayed transcript: before=%d after=%d", afterLoad, got)
	}

	listResp, err := conn.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd(cwd)))
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != 1 || listResp.Sessions[0].SessionId != newResp.SessionId {
		t.Fatalf("sessions = %#v", listResp.Sessions)
	}
	if err := conn.Cancel(ctx, acp.CancelNotification{SessionId: newResp.SessionId}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: newResp.SessionId}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if _, err := conn.UnstableDeleteSession(ctx, DeleteSessionRequest(newResp.SessionId)); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if _, err := conn.LoadSession(ctx, LoadSessionRequest(newResp.SessionId, cwd)); err == nil {
		t.Fatal("deleted session loaded")
	}
	listResp, err = conn.ListSessions(ctx, ListSessionsRequest())
	if err != nil {
		t.Fatalf("ListSessions after delete: %v", err)
	}
	if len(listResp.Sessions) != 0 {
		t.Fatalf("deleted session still listed: %#v", listResp.Sessions)
	}

	argsRecords := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	var continueArgs []string
	for _, args := range argsRecords {
		if slices.Contains(args, "continue") {
			continueArgs = args
		}
	}
	for _, want := range []string{"--no-ide", "--no-color", "--no-notifications", "--settings-file", "--mcp-config", "-m", "deep", "--effort", "max", "threads", "continue", "T-agent-thread", "--stream-json", "--stream-json-input", "-x"} {
		if !slices.Contains(continueArgs, want) {
			t.Fatalf("continue args missing %q: %#v", want, continueArgs)
		}
	}
	stdin := readHelperJSON[string](t, filepath.Join(state, "stdin.jsonl"))
	if len(stdin) != 1 || !strings.Contains(stdin[0], `/review ordinary prompt`) {
		t.Fatalf("stdin = %#v", stdin)
	}
}

func TestAgentErrorAndConformanceBranches(t *testing.T) {
	ctx := context.Background()
	for _, caps := range []acp.ClientCapabilities{
		{},
		{Elicitation: &acp.ElicitationCapabilities{}},
		{Elicitation: &acp.ElicitationCapabilities{Url: &acp.ElicitationUrlCapabilities{}}},
		{Elicitation: &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}},
	} {
		resp, err := NewAgent().Initialize(ctx, acp.InitializeRequest{ClientCapabilities: caps})
		if err != nil {
			t.Fatalf("Initialize: %v", err)
		}
		if meta := resp.AgentCapabilities.Meta[ampMetaKey].(map[string]any); meta["elicitation"] != nil {
			t.Fatalf("elicitation advertised for caps %#v: %#v", caps, meta)
		}
	}

	agent := NewAgent(
		WithAgentName("amp-test"),
		WithAgentTitle("Amp Test"),
		WithAgentVersion("v1.2.3"),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}),
	)
	_, err := agent.Initialize(ctx, acp.InitializeRequest{})
	requireRequestErrorCode(t, err, -32602)

	agent = NewAgent()
	if _, err := agent.Authenticate(ctx, acp.AuthenticateRequest{MethodId: "none"}); err == nil {
		t.Fatal("Authenticate succeeded")
	}
	if _, err := agent.Logout(ctx, acp.LogoutRequest{}); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if _, err := agent.SetSessionMode(ctx, acp.SetSessionModeRequest{}); err == nil {
		t.Fatal("SetSessionMode succeeded")
	}
	if _, err := agent.HandleExtensionMethod(ctx, "_amp/unknown", nil); err == nil {
		t.Fatal("unknown extension succeeded")
	}
	_, err = agent.HandleExtensionMethod(ctx, ForkSessionMethod, json.RawMessage(`{}`))
	requireRequestErrorCode(t, err, -32602)

	for _, meta := range []map[string]any{
		{"amp": "bad"},
		{"amp": map[string]any{"rawEvent": "bad"}},
		{"amp": map[string]any{"options": "bad"}},
		{"amp": map[string]any{"options": map[string]any{"env": "bad"}}},
		{"amp": map[string]any{"options": map[string]any{"env": map[string]any{"A": 1}}}},
		{"amp": map[string]any{"options": map[string]any{"deleted": true}}},
		{"traceparent": "00-test", "tracestate": "state", "baggage": "bag", "foreign": map[string]any{"ok": true}},
	} {
		_, _ = parseSessionMeta(meta)
	}
	if err := NewAgent(WithDefaultModel("gpt")).validateSessionStartOptions(AmpOptions{}); err == nil {
		t.Fatal("default model accepted")
	}
	if err := NewAgent().validateSessionStartOptions(AmpOptions{Model: "gpt"}); err == nil {
		t.Fatal("session model accepted")
	}
	if err := NewAgent().validateSessionStartOptions(AmpOptions{OutputSchema: map[string]any{"type": "object"}}); err == nil {
		t.Fatal("output schema accepted")
	}
	if err := NewAgent().validateSessionStartOptions(AmpOptions{OutputSchema: map[string]any{}}); err == nil {
		t.Fatal("empty output schema accepted")
	}
	if err := NewAgent().validateSessionStartOptions(AmpOptions{Mode: "large"}); err == nil {
		t.Fatal("hidden mode accepted")
	}
	if err := NewAgent().validateSessionStartOptions(AmpOptions{Effort: "extreme"}); err == nil {
		t.Fatal("unknown effort accepted")
	}
}

func TestNativeMissingThreadAndDeleteFailureTombstone(t *testing.T) {
	ctx := context.Background()
	missingPath, _ := fakeAgentAmpPath(t, "missing")
	store := NewInMemorySessionStore()
	agent := NewAgent(WithExecutablePath(missingPath), WithSessionStore(store), WithHome(t.TempDir()))
	newResp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, err = agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "continue missing"))
	if err == nil || !strings.Contains(err.Error(), "native_state_missing") {
		t.Fatalf("missing thread prompt error = %v", err)
	}

	deletePath, _ := fakeAgentAmpPath(t, "delete-fail")
	deleteStore := NewInMemorySessionStore()
	deleteAgent := NewAgent(WithExecutablePath(deletePath), WithSessionStore(deleteStore), WithHome(t.TempDir()))
	deleteResp, err := deleteAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession delete: %v", err)
	}
	_, err = deleteAgent.UnstableDeleteSession(ctx, DeleteSessionRequest(deleteResp.SessionId))
	if err == nil {
		t.Fatal("expected native delete failure")
	}
	if entries, loadErr := deleteStore.Load(ctx, SessionKey{SessionID: string(deleteResp.SessionId), Subpath: SessionStoreMainSubpath}); loadErr != nil || len(entries) != 0 {
		t.Fatalf("tombstone not durable before native delete failure: entries=%d err=%v", len(entries), loadErr)
	}
	if _, err := deleteAgent.LoadSession(ctx, LoadSessionRequest(deleteResp.SessionId, "")); !errors.Is(err, errSessionDeleted) {
		t.Fatalf("deleted load error = %v", err)
	}
}

func TestSessionDirectBranches(t *testing.T) {
	ctx := context.Background()
	fileHome := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileHome, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newAgentSession(NewAgent(WithHome(fileHome)), "T-1", "", parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("newAgentSession with file home succeeded")
	}

	path, _ := fakeAgentAmpPath(t, "")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentPrompts: 1}))
	session, err := newAgentSession(agent, "T-1", t.TempDir(), parsedSessionMeta{rawEvent: true}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	session.turn <- struct{}{}
	if _, err := session.acquireTurn(ctx); err == nil {
		t.Fatal("expected session_prompt backpressure")
	}
	<-session.turn
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	session.turn <- struct{}{}
	if _, err := session.acquireTurn(cancelCtx); err == nil {
		t.Fatal("expected canceled acquireTurn")
	}
	<-session.turn
	session.poisonCause = "poisoned"
	if err := session.ready(); err == nil {
		t.Fatal("poisoned session ready")
	}
	session.poisonCause = ""
	session.closed = true
	if err := session.ready(); !errors.Is(err, errSessionClosed) {
		t.Fatalf("closed ready = %v", err)
	}
	session.closed = false
	if err := session.Cancel(ctx); err != nil {
		t.Fatalf("Cancel without turn: %v", err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := session.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

type recordingClient struct {
	mu               sync.Mutex
	updates          []acp.SessionNotification
	raw              []json.RawMessage
	permissionCalls  int
	elicitationCalls int
}

func (c *recordingClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, nil
}

func (c *recordingClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, nil
}

func (c *recordingClient) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.permissionCalls++
	c.mu.Unlock()
	return acp.RequestPermissionResponse{}, nil
}

func (c *recordingClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updates = append(c.updates, notification)
	return nil
}

func (c *recordingClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, nil
}

func (c *recordingClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (c *recordingClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, nil
}

func (c *recordingClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (c *recordingClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func (c *recordingClient) UnstableCreateElicitation(context.Context, acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
	c.mu.Lock()
	c.elicitationCalls++
	c.mu.Unlock()
	return acp.UnstableCreateElicitationResponse{}, nil
}

func (c *recordingClient) HandleExtensionMethod(_ context.Context, method string, params json.RawMessage) (any, error) {
	if method != RawEventMethod {
		return nil, acp.NewMethodNotFound(method)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.raw = append(c.raw, append(json.RawMessage(nil), params...))
	return nil, nil
}

func (c *recordingClient) updatesSnapshot() []acp.SessionNotification {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]acp.SessionNotification(nil), c.updates...)
}

func (c *recordingClient) rawSnapshot() []json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]json.RawMessage(nil), c.raw...)
}

func startTestServe(t *testing.T, opts ...Option) (*acp.ClientSideConnection, *recordingClient, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- Serve(ctx, c2aR, a2cW, opts...)
	}()
	client := &recordingClient{}
	conn := acp.NewClientSideConnection(client, c2aW, a2cR)
	cleanup := func() {
		cancel()
		_ = c2aW.Close()
		_ = c2aR.Close()
		_ = a2cW.Close()
		_ = a2cR.Close()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("Serve returned %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Serve did not stop")
		}
	}
	return conn, client, cleanup
}

func requireUpdateKinds(t *testing.T, notifications []acp.SessionNotification, kinds ...string) {
	t.Helper()
	got := make([]string, 0, len(notifications))
	for _, notification := range notifications {
		update := notification.Update
		switch {
		case update.UserMessageChunk != nil:
			got = append(got, update.UserMessageChunk.SessionUpdate)
		case update.AgentMessageChunk != nil:
			got = append(got, update.AgentMessageChunk.SessionUpdate)
		case update.ToolCallUpdate != nil:
			got = append(got, update.ToolCallUpdate.SessionUpdate)
		case update.ToolCall != nil:
			got = append(got, update.ToolCall.SessionUpdate)
		case update.UsageUpdate != nil:
			got = append(got, update.UsageUpdate.SessionUpdate)
		case update.ConfigOptionUpdate != nil:
			got = append(got, update.ConfigOptionUpdate.SessionUpdate)
		}
	}
	for _, want := range kinds {
		if !slices.Contains(got, want) {
			t.Fatalf("missing update %q in %#v", want, got)
		}
	}
}

func requireRequestErrorCode(t *testing.T, err error, code int) {
	t.Helper()
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error = %T %v, want RequestError", err, err)
	}
	if reqErr.Code != code {
		t.Fatalf("code = %d, want %d (%v)", reqErr.Code, code, err)
	}
}

func fakeAgentAmpPath(t *testing.T, mode string) (string, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake executable source is built only for local POSIX tests")
	}
	dir, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "amp")
	source := filepath.Join(dir, "fake_amp.go")
	if err := os.WriteFile(source, []byte(fakeAgentAmpSource(mode, state)), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "build", "-o", path, source)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fake amp: %v\n%s", err, out)
	}
	if out, err := exec.Command(path, "threads", "new").CombinedOutput(); err != nil {
		t.Fatalf("preflight fake amp: %v\n%s", err, out)
	}
	return path, state
}

func fakeAgentAmpSource(mode string, state string) string {
	return `package main

import (
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const fakeMode = ` + strconv.Quote(mode) + `
const fakeState = ` + strconv.Quote(state) + `

func main() {
	args := os.Args[1:]
	state := fakeState
	mode := fakeMode
	record(state, "args.jsonl", args)
	if len(args) > 0 && args[0] == "version" {
		if mode == "bad-version" {
			os.Stdout.WriteString("0.0.1\n")
			return
		}
		os.Stdout.WriteString("0.0.1783155105-gfake\n")
		return
	}
	// Startup method-present probes use a known-missing thread id; answer with the
	// domain missing-thread error regardless of mode so probes never hang.
	for _, a := range args {
		if a == "T-00000000-0000-0000-0000-000000000000" {
			os.Stderr.WriteString("Thread not found\n")
			os.Exit(1)
		}
	}
	threads := index(args, "threads")
	if threads < 0 || threads+1 >= len(args) {
		os.Stderr.WriteString("missing threads subcommand\n")
		os.Exit(2)
	}
	switch args[threads+1] {
	case "new":
		if mode == "bad-new-id" {
			os.Stdout.WriteString("not-a-thread\n")
			return
		}
		os.Stdout.WriteString("T-agent-thread\n")
	case "list":
		if mode == "probe-list-fail" {
			os.Stdout.WriteString("{\n")
			return
		}
		os.Stdout.WriteString("[]\n")
	case "export":
		if mode == "missing-export" {
			os.Stderr.WriteString("Thread not found\n")
			os.Exit(1)
		}
		if mode == "export-fail" {
			os.Stderr.WriteString("export failed\n")
			os.Exit(1)
		}
		if mode == "bad-export-json" {
			os.Stdout.WriteString("{")
			return
		}
		os.Stdout.WriteString("{\"thread\":\"" + args[len(args)-1] + "\"}\n")
	case "delete":
		if mode == "delete-fail-once" {
			marker := filepath.Join(state, "delete-failed-once")
			if _, err := os.Stat(marker); err != nil {
				_ = os.WriteFile(marker, []byte("1"), 0600)
				os.Stderr.WriteString("delete failed once\n")
				os.Exit(1)
			}
		}
		if mode == "delete-fail" {
			os.Stderr.WriteString("delete failed\n")
			os.Exit(1)
		}
		os.Stdout.WriteString("deleted\n")
	case "continue":
		if mode == "block-stdin" {
			record(state, "continue-started", "yes")
			for {
				time.Sleep(time.Hour)
			}
		}
		stdin, _ := io.ReadAll(os.Stdin)
		record(state, "stdin.jsonl", strings.TrimSpace(string(stdin)))
		if mode == "missing" {
			os.Stderr.WriteString("Thread not found\n")
			os.Exit(1)
		}
		if mode == "result-error" {
			os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"error\",\"duration_ms\":1,\"is_error\":true,\"error\":\"native failed\",\"session_id\":\"T-agent-thread\"}\n")
			return
		}
		if mode == "no-result" {
			os.Stdout.WriteString("{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"partial\"}]},\"session_id\":\"T-agent-thread\"}\n")
			return
		}
		if mode == "malformed-only" {
			os.Stdout.WriteString("{bad json\n")
			return
		}
		if mode == "session-drift" {
			os.Stdout.WriteString("{\"type\":\"system\",\"subtype\":\"init\",\"cwd\":\"/tmp/project\",\"session_id\":\"T-other\"}\n")
			os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"success\",\"duration_ms\":1,\"is_error\":false,\"num_turns\":1,\"result\":\"late\",\"session_id\":\"T-other\"}\n")
			return
		}
		if mode == "sigint-result" {
			os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"error_during_execution\",\"duration_ms\":1,\"is_error\":true,\"error\":\"User cancelled (SIGINT/SIGTERM)\",\"session_id\":\"T-agent-thread\"}\n")
			return
		}
		if mode == "delayed-error" {
			record(state, "continue-ready", "yes")
			time.Sleep(100 * time.Millisecond)
			os.Stderr.WriteString("delayed failure\n")
			os.Exit(1)
		}
		if mode == "hang" {
			for {
				time.Sleep(time.Hour)
			}
		}
		if mode == "sigint-ignore" {
			record(state, "pid.jsonl", os.Getpid())
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
			record(state, "continue-ready", "yes")
			go func() {
				sig := <-signals
				record(state, "signal", sig.String())
			}()
			for {
				time.Sleep(time.Hour)
			}
		}
		os.Stderr.WriteString("native stderr noise\n")
		os.Stdout.WriteString("native stdout noise\n")
		os.Stdout.WriteString("{\"type\":\"system\",\"subtype\":\"init\",\"cwd\":\"/tmp/project\",\"session_id\":\"T-agent-thread\",\"tools\":[\"Read\"],\"mcp_servers\":[{\"name\":\"svc\",\"status\":\"connected\"}],\"agent_mode\":\"smart\",\"reasoning_effort\":\"high\"}\n")
		os.Stdout.WriteString("{\"type\":\"user\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"echoed user\"},{\"type\":\"tool_result\",\"tool_use_id\":\"TU-1\",\"content\":\"tool output\",\"is_error\":true}]},\"session_id\":\"T-agent-thread\"}\n")
		os.Stdout.WriteString("{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"agent text\"},{\"type\":\"tool_use\",\"id\":\"TU-1\",\"name\":\"Read\",\"input\":{\"path\":\"README.md\"}}],\"usage\":{\"input_tokens\":3,\"cache_creation_input_tokens\":5,\"cache_read_input_tokens\":7,\"output_tokens\":11,\"max_tokens\":200,\"service_tier\":\"standard\"}},\"session_id\":\"T-agent-thread\"}\n")
		os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"success\",\"duration_ms\":1,\"is_error\":false,\"num_turns\":1,\"result\":\"done\",\"session_id\":\"T-agent-thread\",\"usage\":{\"input_tokens\":13,\"output_tokens\":17,\"max_tokens\":300}}\n")
	default:
		os.Stderr.WriteString("unknown threads subcommand\n")
		os.Exit(2)
	}
}

func index(values []string, target string) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
}

func record(state string, name string, value any) {
	if state == "" {
		return
	}
	file, err := os.OpenFile(filepath.Join(state, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(value); err != nil {
		os.Exit(2)
	}
}
`
}

func helperArgs() []string {
	for i, arg := range os.Args {
		if arg == "--" {
			return append([]string(nil), os.Args[i+1:]...)
		}
	}
	return nil
}

func recordHelperJSON(state string, name string, value any) {
	if state == "" {
		return
	}
	file, err := os.OpenFile(filepath.Join(state, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		os.Exit(2)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(value); err != nil {
		os.Exit(2)
	}
}

func readHelperJSON[T any](t *testing.T, path string) []T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	out := make([]T, 0, len(lines))
	for _, line := range lines {
		var value T
		if err := json.Unmarshal([]byte(line), &value); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		out = append(out, value)
	}
	return out
}
