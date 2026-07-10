package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

type fakeAgentConnection struct {
	initErr   error
	newErr    error
	promptErr error

	cwd       string
	prompt    string
	closed    bool
	sessionID acp.SessionId
}

func (f *fakeAgentConnection) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{}, f.initErr
}

func (f *fakeAgentConnection) NewSession(_ context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	f.cwd = params.Cwd
	if f.sessionID == "" {
		f.sessionID = "session-1"
	}

	return acp.NewSessionResponse{SessionId: f.sessionID}, f.newErr
}

func (f *fakeAgentConnection) Prompt(_ context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	if len(params.Prompt) > 0 && params.Prompt[0].Text != nil {
		f.prompt = params.Prompt[0].Text.Text
	}

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, f.promptErr
}

func (f *fakeAgentConnection) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	f.closed = true

	return acp.CloseSessionResponse{}, nil
}

func TestClientFileMethods(t *testing.T) {
	t.Parallel()

	c := client{}
	path := filepath.Join(t.TempDir(), "note.txt")

	if _, err := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{
		Path:    path,
		Content: "hello",
	}); err != nil {
		t.Fatalf("WriteTextFile: %v", err)
	}

	read, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: path})
	if err != nil {
		t.Fatalf("ReadTextFile: %v", err)
	}

	if read.Content != "hello" {
		t.Fatalf("content = %q, want hello", read.Content)
	}

	if _, err := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: "relative"}); err == nil {
		t.Fatal("relative write did not error")
	}

	if _, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: "relative"}); err == nil {
		t.Fatal("relative read did not error")
	}

	if _, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: filepath.Join(t.TempDir(), "missing.txt")}); err == nil {
		t.Fatal("missing read did not error")
	}

	parentFile := filepath.Join(t.TempDir(), "parent")
	if err := os.WriteFile(parentFile, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{
		Path: filepath.Join(parentFile, "child.txt"),
	}); err == nil {
		t.Fatal("write under file did not error")
	}
}

func TestClientPermissionMethods(t *testing.T) {
	t.Parallel()

	c := client{}
	resp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		Options: []acp.PermissionOption{
			{OptionId: "reject", Kind: acp.PermissionOptionKindRejectOnce},
			{OptionId: "allow", Kind: acp.PermissionOptionKindAllowOnce},
		},
	})
	if err != nil {
		t.Fatalf("RequestPermission: %v", err)
	}

	if resp.Outcome.Selected == nil || resp.Outcome.Selected.OptionId != "allow" {
		t.Fatalf("permission outcome = %#v, want selected allow", resp.Outcome)
	}

	resp, err = c.RequestPermission(context.Background(), acp.RequestPermissionRequest{})
	if err != nil {
		t.Fatalf("RequestPermission empty: %v", err)
	}

	if resp.Outcome.Cancelled == nil {
		t.Fatal("empty permission outcome is not cancelled")
	}
}

func TestClientSessionUpdatePrintsVisibleEvents(t *testing.T) {
	c := client{}
	status := acp.ToolCallStatusCompleted

	output := captureStdout(t, func() {
		if err := c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("hello")},
			},
		}); err != nil {
			t.Fatalf("message chunk: %v", err)
		}

		if err := c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{Content: acp.TextBlock("thinking")},
			},
		}); err != nil {
			t.Fatalf("thought chunk: %v", err)
		}

		if err := c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				ToolCall: &acp.SessionUpdateToolCall{ToolCallId: "tool-1", Title: "Read file"},
			},
		}); err != nil {
			t.Fatalf("tool call: %v", err)
		}

		if err := c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "tool-1", Status: &status},
			},
		}); err != nil {
			t.Fatalf("tool call update: %v", err)
		}

		if err := c.SessionUpdate(context.Background(), acp.SessionNotification{}); err != nil {
			t.Fatalf("empty update: %v", err)
		}
	})

	for _, want := range []string{
		"hello",
		"[thought] thinking",
		"[tool] tool-1 Read file",
		"[tool] tool-1 completed",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output %q missing %q", output, want)
		}
	}
}

func TestClientSessionUpdateReconcilesFinalMessageSnapshot(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	c := client{output: &output}

	c.fallback.writeText(&output, "")

	for _, text := range []string{"Hello", " from ACP", "Hello from ACP"} {
		if err := c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
					Content: acp.TextBlock(text),
				},
			},
		}); err != nil {
			t.Fatalf("SessionUpdate %q: %v", text, err)
		}
	}

	if output.String() != "Hello from ACP" {
		t.Fatalf("output = %q, want Hello from ACP", output.String())
	}
}

func TestClientSessionUpdateCompletesPartialFinalSnapshot(t *testing.T) {
	t.Parallel()

	messageID := "33333333-3333-4333-8333-333333333333"
	var output bytes.Buffer
	c := client{output: &output}

	for _, text := range []string{"Hello from", "Hello from ACP"} {
		if err := c.SessionUpdate(context.Background(), acp.SessionNotification{
			Update: acp.SessionUpdate{
				AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
					MessageId: &messageID,
					Content:   acp.TextBlock(text),
				},
			},
		}); err != nil {
			t.Fatalf("SessionUpdate %q: %v", text, err)
		}
	}

	if output.String() != "Hello from ACP" {
		t.Fatalf("output = %q, want Hello from ACP", output.String())
	}
}

func TestClientSessionUpdateUsesConfiguredWriter(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	c := client{output: &output}

	if c.writer() != &output {
		t.Fatal("writer did not return configured output")
	}

	if err := c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{
			AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("hello")},
		},
	}); err != nil {
		t.Fatalf("SessionUpdate: %v", err)
	}

	if output.String() != "hello" {
		t.Fatalf("output = %q, want hello", output.String())
	}
}

func TestClientTerminalMethods(t *testing.T) {
	t.Parallel()

	c := client{}
	terminal, err := c.CreateTerminal(context.Background(), acp.CreateTerminalRequest{})
	if err != nil {
		t.Fatalf("CreateTerminal: %v", err)
	}

	if terminal.TerminalId != "terminal-1" {
		t.Fatalf("terminal id = %q, want terminal-1", terminal.TerminalId)
	}

	output, err := c.TerminalOutput(context.Background(), acp.TerminalOutputRequest{})
	if err != nil {
		t.Fatalf("TerminalOutput: %v", err)
	}

	if output.Truncated {
		t.Fatal("terminal output truncated")
	}

	if _, err := c.KillTerminal(context.Background(), acp.KillTerminalRequest{}); err != nil {
		t.Fatalf("KillTerminal: %v", err)
	}

	if _, err := c.ReleaseTerminal(context.Background(), acp.ReleaseTerminalRequest{}); err != nil {
		t.Fatalf("ReleaseTerminal: %v", err)
	}

	if _, err := c.WaitForTerminalExit(context.Background(), acp.WaitForTerminalExitRequest{}); err != nil {
		t.Fatalf("WaitForTerminalExit: %v", err)
	}
}

func TestRunConversation(t *testing.T) {
	t.Parallel()

	conn := &fakeAgentConnection{}
	var output bytes.Buffer

	if err := runConversation(context.Background(), conn, "hello", "/repo", &output); err != nil {
		t.Fatalf("runConversation: %v", err)
	}

	if conn.cwd != "/repo" {
		t.Fatalf("cwd = %q, want /repo", conn.cwd)
	}

	if conn.prompt != "hello" {
		t.Fatalf("prompt = %q, want hello", conn.prompt)
	}

	if !conn.closed {
		t.Fatal("session not closed")
	}

	if !strings.Contains(output.String(), "stop reason: end_turn") {
		t.Fatalf("output %q missing stop reason", output.String())
	}
}

func TestRunConversationErrors(t *testing.T) {
	t.Parallel()

	for _, conn := range []*fakeAgentConnection{
		{initErr: errors.New("init")},
		{newErr: errors.New("new")},
		{promptErr: errors.New("prompt")},
	} {
		var output bytes.Buffer
		if err := runConversation(context.Background(), conn, "hello", "/repo", &output); err == nil {
			t.Fatal("runConversation did not error")
		}
	}
}

func TestRun(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
	})

	conn := &fakeAgentConnection{}
	var closed bool
	var waited bool
	startAgent = func(context.Context, io.Writer, io.Writer) (*startedAgent, error) {
		return &startedAgent{
			conn: conn,
			close: func() {
				closed = true
			},
			wait: func() error {
				waited = true

				return nil
			},
		}, nil
	}
	getwd = func() (string, error) {
		return "/repo", nil
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run(context.Background(), nil, &stdout, &stderr); code != 0 {
		t.Fatalf("run code = %d, want 0", code)
	}

	if conn.prompt != "Reply with a short hello from ACP." {
		t.Fatalf("prompt = %q, want default", conn.prompt)
	}

	if !closed {
		t.Fatal("agent not closed")
	}

	if !waited {
		t.Fatal("agent not waited")
	}

	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}

	startAgent = func(context.Context, io.Writer, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{}}, nil
	}

	stdout.Reset()
	stderr.Reset()
	if code := run(context.Background(), []string{"hello"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run code = %d, want 0", code)
	}

	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestMain(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	originalExit := exit
	originalArgs := os.Args
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
		exit = originalExit
		os.Args = originalArgs
	})

	startAgent = func(context.Context, io.Writer, io.Writer) (*startedAgent, error) {
		return &startedAgent{
			conn:  &fakeAgentConnection{},
			close: func() {},
			wait:  func() error { return nil },
		}, nil
	}
	getwd = func() (string, error) { return "/repo", nil }

	var gotCode int
	exit = func(code int) {
		gotCode = code
	}
	os.Args = []string{"minimal-client", "hello"}

	main()

	if gotCode != 0 {
		t.Fatalf("exit code = %d, want 0", gotCode)
	}
}

func TestStartAgentProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses a POSIX shell script")
	}

	binDir := t.TempDir()
	goPath := filepath.Join(binDir, "go")
	if err := os.WriteFile(goPath, []byte("#!/bin/sh\nwhile IFS= read -r _; do :; done\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("PATH", binDir)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	agent, err := startAgentProcess(context.Background(), &stdout, &stderr)
	if err != nil {
		t.Fatalf("startAgentProcess: %v", err)
	}

	if agent.conn == nil {
		t.Fatal("agent conn is nil")
	}

	agent.close()
	if err := agent.wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

func TestStartAgentProcessUsesModuleEntrypoint(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses POSIX shell")
	}

	originalCommandContext := commandContext
	t.Cleanup(func() {
		commandContext = originalCommandContext
	})

	var gotName string
	var gotArgs []string
	commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		gotName = name
		gotArgs = append([]string(nil), args...)

		return exec.CommandContext(ctx, "sh", "-c", "while IFS= read -r _; do :; done")
	}

	agent, err := startAgentProcess(context.Background(), io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("startAgentProcess: %v", err)
	}

	if agent.conn == nil {
		t.Fatal("agent conn is nil")
	}

	agent.close()
	if err := agent.wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}

	if gotName != "go" {
		t.Fatalf("name = %q, want go", gotName)
	}

	if len(gotArgs) != 2 || gotArgs[0] != "run" || gotArgs[1] != agentPackage {
		t.Fatalf("args = %v, want [run %s]", gotArgs, agentPackage)
	}
}

func TestStartAgentProcessStartError(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	agent, err := startAgentProcess(context.Background(), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("startAgentProcess did not error")
	}

	if agent != nil {
		t.Fatal("agent is not nil")
	}
}

func TestStartAgentProcessPipeErrors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses POSIX shell")
	}

	originalCommandContext := commandContext
	t.Cleanup(func() {
		commandContext = originalCommandContext
	})

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "sh", "-c", "cat")
		cmd.Stdin = strings.NewReader("")

		return cmd
	}
	agent, err := startAgentProcess(context.Background(), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("stdin pipe error not returned")
	}

	if agent != nil {
		t.Fatal("agent is not nil")
	}

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "sh", "-c", "cat")
		cmd.Stdout = io.Discard

		return cmd
	}
	agent, err = startAgentProcess(context.Background(), io.Discard, io.Discard)
	if err == nil {
		t.Fatal("stdout pipe error not returned")
	}

	if agent != nil {
		t.Fatal("agent is not nil")
	}
}

func TestRunErrors(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
	})

	getwd = func() (string, error) {
		return "", errors.New("cwd")
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run(context.Background(), []string{"hello"}, &stdout, &stderr); code != 1 {
		t.Fatalf("run code = %d, want 1", code)
	}

	if !strings.Contains(stderr.String(), "cwd") {
		t.Fatalf("stderr %q missing cwd", stderr.String())
	}

	getwd = func() (string, error) {
		return "/repo", nil
	}
	startAgent = func(context.Context, io.Writer, io.Writer) (*startedAgent, error) {
		return nil, errors.New("start")
	}

	stderr.Reset()
	if code := run(context.Background(), []string{"hello"}, &stdout, &stderr); code != 1 {
		t.Fatalf("run code = %d, want 1", code)
	}

	if !strings.Contains(stderr.String(), "start") {
		t.Fatalf("stderr %q missing start", stderr.String())
	}

	startAgent = func(context.Context, io.Writer, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{initErr: errors.New("init")}}, nil
	}

	stderr.Reset()
	if code := run(context.Background(), []string{"hello"}, &stdout, &stderr); code != 1 {
		t.Fatalf("run code = %d, want 1", code)
	}

	if !strings.Contains(stderr.String(), "init") {
		t.Fatalf("stderr %q missing init", stderr.String())
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}

	os.Stdout = writer
	defer func() {
		os.Stdout = original
	}()

	fn()

	if err = writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if err = reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}

	return strings.TrimSpace(string(data))
}
