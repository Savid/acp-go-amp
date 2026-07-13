package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	ampacp "github.com/savid/acp-go-amp"
)

func TestReadTranscriptJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	body := "\n" +
		`{"type":"system","subtype":"init","cwd":"/repo","session_id":"T-1"}` + "\n" +
		`{"type":"assistant"}` + "\n"

	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	entries, sessionID, cwd, err := readTranscriptJSONL(path)
	if err != nil {
		t.Fatalf("readTranscriptJSONL: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}

	if sessionID != "T-1" {
		t.Fatalf("sessionID = %q, want T-1", sessionID)
	}

	if cwd != "/repo" {
		t.Fatalf("cwd = %q, want /repo", cwd)
	}

	missingEntries, missingSessionID, missingCwd, missingErr := readTranscriptJSONL(filepath.Join(t.TempDir(), "missing.jsonl"))
	if missingErr == nil {
		t.Fatal("missing file did not error")
	}

	if missingEntries != nil || missingSessionID != "" || missingCwd != "" {
		t.Fatalf("missing file returned values: %v %q %q", missingEntries, missingSessionID, missingCwd)
	}
}

func TestReadTranscriptScannerError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "long.jsonl")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", bufio.MaxScanTokenSize+1)), 0o600); err != nil {
		t.Fatal(err)
	}

	entries, sessionID, cwd, err := readTranscriptJSONL(path)
	if err == nil {
		t.Fatal("oversized line did not error")
	}

	if entries != nil || sessionID != "" || cwd != "" {
		t.Fatalf("oversized line returned values: %v %q %q", entries, sessionID, cwd)
	}
}

func TestRunUsesInferredValuesAndLoadedSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.jsonl")
	cwd := t.TempDir()
	body := fmt.Sprintf(`{"type":"system","subtype":"init","cwd":%q,"session_id":"T-1"}`+"\n", cwd) +
		`{"type":"assistant"}` + "\n"

	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	previousRunLoaded := runLoaded
	t.Cleanup(func() { runLoaded = previousRunLoaded })

	expectedSessionID := "T-1"
	expectedCwd := cwd
	expectedPrompt := "prompt"
	expectedPath := "/bin/amp"
	expectedScratch := "/tmp/scratch"
	runLoaded = func(_ context.Context, store ampacp.SessionStore, sessionID string, gotCwd string, prompt string, ampPath string, scratchDir string, stdout io.Writer) error {
		t.Helper()

		if sessionID != expectedSessionID {
			t.Fatalf("sessionID = %q, want %q", sessionID, expectedSessionID)
		}

		if gotCwd != expectedCwd {
			t.Fatalf("cwd = %q, want %q", gotCwd, expectedCwd)
		}

		if prompt != expectedPrompt {
			t.Fatalf("prompt = %q, want %q", prompt, expectedPrompt)
		}

		if ampPath != expectedPath {
			t.Fatalf("path = %q, want %q", ampPath, expectedPath)
		}

		if scratchDir != expectedScratch {
			t.Fatalf("scratch-dir = %q, want %q", scratchDir, expectedScratch)
		}

		manifests, err := store.Load(context.Background(), ampacp.SessionKey{SessionID: sessionID, Subpath: ampacp.SessionStoreMainSubpath})
		if err != nil {
			t.Fatalf("load manifest: %v", err)
		}

		if len(manifests) != 1 {
			t.Fatalf("manifests = %d, want 1", len(manifests))
		}

		var manifest map[string]any
		if unmarshalErr := json.Unmarshal(manifests[0], &manifest); unmarshalErr != nil {
			t.Fatalf("manifest json: %v", unmarshalErr)
		}

		if manifest["format"] != ampacp.SessionStoreFormat || manifest["threadId"] != sessionID {
			t.Fatalf("manifest = %v", manifest)
		}

		frames, err := store.Load(context.Background(), ampacp.SessionKey{SessionID: sessionID, Subpath: transcriptSubpath})
		if err != nil {
			t.Fatalf("load transcript: %v", err)
		}

		if len(frames) != 2 {
			t.Fatalf("transcript frames = %d, want 2", len(frames))
		}

		fmt.Fprint(stdout, "loaded")

		return nil
	}

	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"-file", path, "-prompt", "prompt", "-path", "/bin/amp", "-scratch-dir", "/tmp/scratch"}, &stdout, io.Discard); err != nil {
		t.Fatalf("run inferred: %v", err)
	}

	if stdout.String() != "loaded" {
		t.Fatalf("stdout = %q, want loaded", stdout.String())
	}

	stdout.Reset()

	expectedSessionID = "T-explicit"
	expectedPrompt = defaultPrompt
	expectedPath = ""
	expectedScratch = ""

	if err := run(context.Background(), []string{"-file", path, "-session", "T-explicit", "-cwd", cwd}, &stdout, io.Discard); err != nil {
		t.Fatalf("run explicit: %v", err)
	}
}

func TestRunErrors(t *testing.T) {
	entries, sessionID, cwd, readErr := readTranscriptJSONL("")
	if readErr == nil {
		t.Fatal("empty path did not error")
	}

	if entries != nil || sessionID != "" || cwd != "" {
		t.Fatalf("empty path returned values: %v %q %q", entries, sessionID, cwd)
	}

	if err := run(context.Background(), []string{"-bad"}, io.Discard, io.Discard); err == nil {
		t.Fatal("bad flag did not error")
	}

	if err := run(context.Background(), []string{"-file", filepath.Join(t.TempDir(), "missing.jsonl")}, io.Discard, io.Discard); err == nil {
		t.Fatal("missing file did not error")
	}

	path := filepath.Join(t.TempDir(), "session.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"assistant"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	runErr := run(context.Background(), []string{"-file", path}, io.Discard, io.Discard)
	if runErr == nil || !strings.Contains(runErr.Error(), "session id is required") {
		t.Fatalf("missing session id error = %v", runErr)
	}

	previousGetwd := getwd
	previousRunLoaded := runLoaded
	t.Cleanup(func() {
		getwd = previousGetwd
		runLoaded = previousRunLoaded
	})

	getwd = func() (string, error) { return "", errors.New("getwd failed") }
	runLoaded = func(context.Context, ampacp.SessionStore, string, string, string, string, string, io.Writer) error {
		return nil
	}

	if err := os.WriteFile(path, []byte(`{"type":"assistant","session_id":"T-1"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	runErr = run(context.Background(), []string{"-file", path}, io.Discard, io.Discard)
	if runErr == nil || !strings.Contains(runErr.Error(), "getwd failed") {
		t.Fatalf("getwd error = %v", runErr)
	}

	getwd = func() (string, error) { return t.TempDir(), nil }
	runLoaded = func(context.Context, ampacp.SessionStore, string, string, string, string, string, io.Writer) error {
		return errors.New("load failed")
	}

	runErr = run(context.Background(), []string{"-file", path}, io.Discard, io.Discard)
	if runErr == nil || !strings.Contains(runErr.Error(), "load failed") {
		t.Fatalf("runLoaded error = %v", runErr)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if err := run(cancelled, []string{"-file", path, "-session", "T-1", "-cwd", t.TempDir()}, io.Discard, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled run error = %v", err)
	}
}

func TestRunLoadedSessionWithFakeServe(t *testing.T) {
	previousServe := serve
	serve = fakeServe(t, "", nil)
	t.Cleanup(func() { serve = previousServe })

	var stdout bytes.Buffer
	if err := runLoadedSession(context.Background(), ampacp.NewInMemorySessionStore(), "T-1", t.TempDir(), "prompt", "", "", &stdout); err != nil {
		t.Fatalf("runLoadedSession: %v", err)
	}

	if !strings.Contains(stdout.String(), "== resume smoke test ==") {
		t.Fatalf("stdout missing banner: %q", stdout.String())
	}

	if !strings.Contains(stdout.String(), "stop reason: end_turn") {
		t.Fatalf("stdout missing stop reason: %q", stdout.String())
	}
}

func TestRunLoadedSessionErrors(t *testing.T) {
	previousServe := serve
	t.Cleanup(func() { serve = previousServe })

	for _, tc := range []struct {
		name   string
		method string
		err    *acp.RequestError
	}{
		{name: "initialize", method: acp.AgentMethodInitialize, err: acp.NewInternalError(map[string]any{"error": "init"})},
		{name: "load", method: acp.AgentMethodSessionLoad, err: acp.NewInternalError(map[string]any{"error": "load"})},
		{name: "prompt", method: acp.AgentMethodSessionPrompt, err: acp.NewInternalError(map[string]any{"error": "prompt"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serve = fakeServe(t, tc.method, tc.err)

			if err := runLoadedSession(context.Background(), ampacp.NewInMemorySessionStore(), "T-1", t.TempDir(), "prompt", "", "", io.Discard); err == nil {
				t.Fatal("runLoadedSession did not error")
			}
		})
	}
}

func fakeServe(t *testing.T, failMethod string, failErr *acp.RequestError) func(context.Context, io.Reader, io.Writer, ...ampacp.Option) error {
	t.Helper()

	return func(ctx context.Context, input io.Reader, output io.Writer, _ ...ampacp.Option) error {
		_ = acp.NewConnection(func(_ context.Context, method string, params json.RawMessage) (any, *acp.RequestError) {
			if method == failMethod {
				return nil, failErr
			}

			switch method {
			case acp.AgentMethodInitialize:
				return acp.InitializeResponse{ProtocolVersion: acp.ProtocolVersionNumber}, nil
			case acp.AgentMethodSessionLoad:
				var req acp.LoadSessionRequest
				if err := json.Unmarshal(params, &req); err != nil {
					return nil, acp.NewInvalidParams(map[string]any{"error": err.Error()})
				}

				return acp.LoadSessionResponse{}, nil
			case acp.AgentMethodSessionPrompt:
				return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
			case acp.AgentMethodSessionClose:
				return acp.CloseSessionResponse{}, nil
			default:
				return nil, acp.NewMethodNotFound(method)
			}
		}, output, input)
		<-ctx.Done()

		return nil
	}
}

func TestMainUsesRunMainAndExit(t *testing.T) {
	previousRunMain := runMain
	previousExit := exit
	previousArgs := os.Args
	t.Cleanup(func() {
		runMain = previousRunMain
		exit = previousExit
		os.Args = previousArgs
	})

	runMain = func(context.Context, []string, io.Writer, io.Writer) error { return nil }
	exit = func(code int) { panic(fmt.Sprintf("exit %d", code)) }
	os.Args = []string{"resume-from-file"}
	main()

	runMain = func(context.Context, []string, io.Writer, io.Writer) error { return errors.New("boom") }

	mustPanicWithValue(t, "exit 1", main)
}

func mustPanicWithValue(t *testing.T, want any, fn func()) {
	t.Helper()

	defer func() {
		if got := recover(); got != want {
			t.Fatalf("panic = %v, want %v", got, want)
		}
	}()

	fn()
}

func TestClientMethods(t *testing.T) {
	var stdout bytes.Buffer

	c := &client{output: &stdout}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "file.txt")

	if _, err := c.WriteTextFile(ctx, acp.WriteTextFileRequest{Path: path, Content: "body"}); err != nil {
		t.Fatalf("WriteTextFile: %v", err)
	}

	read, readErr := c.ReadTextFile(ctx, acp.ReadTextFileRequest{Path: path})
	if readErr != nil {
		t.Fatalf("ReadTextFile: %v", readErr)
	}

	if read.Content != "body" {
		t.Fatalf("content = %q, want body", read.Content)
	}

	if _, err := c.ReadTextFile(ctx, acp.ReadTextFileRequest{Path: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Fatal("missing file read did not error")
	}

	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := c.WriteTextFile(ctx, acp.WriteTextFileRequest{Path: filepath.Join(notDir, "child"), Content: "body"}); err == nil {
		t.Fatal("write under file did not error")
	}

	permission, permErr := c.RequestPermission(ctx, acp.RequestPermissionRequest{})
	if permErr != nil {
		t.Fatalf("RequestPermission: %v", permErr)
	}

	if permission.Outcome.Cancelled == nil {
		t.Fatal("permission outcome is not cancelled")
	}

	if err := c.SessionUpdate(ctx, acp.SessionNotification{}); err != nil {
		t.Fatalf("empty SessionUpdate: %v", err)
	}

	if err := c.SessionUpdate(ctx, acp.SessionNotification{Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("hi")}}}); err != nil {
		t.Fatalf("text SessionUpdate: %v", err)
	}

	if stdout.String() != "hi" || c.text.String() != "hi" {
		t.Fatalf("captured text = %q / %q, want hi", stdout.String(), c.text.String())
	}

	if err := (&client{}).SessionUpdate(ctx, acp.SessionNotification{Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("stdout")}}}); err != nil {
		t.Fatalf("stdout SessionUpdate: %v", err)
	}

	terminal, termErr := c.CreateTerminal(ctx, acp.CreateTerminalRequest{})
	if termErr != nil {
		t.Fatalf("CreateTerminal: %v", termErr)
	}

	if terminal.TerminalId != "terminal-1" {
		t.Fatalf("terminal id = %q, want terminal-1", terminal.TerminalId)
	}

	if _, err := c.KillTerminal(ctx, acp.KillTerminalRequest{}); err != nil {
		t.Fatalf("KillTerminal: %v", err)
	}

	output, outErr := c.TerminalOutput(ctx, acp.TerminalOutputRequest{})
	if outErr != nil {
		t.Fatalf("TerminalOutput: %v", outErr)
	}

	if output.Truncated {
		t.Fatal("terminal output truncated")
	}

	if _, err := c.ReleaseTerminal(ctx, acp.ReleaseTerminalRequest{}); err != nil {
		t.Fatalf("ReleaseTerminal: %v", err)
	}

	if _, err := c.WaitForTerminalExit(ctx, acp.WaitForTerminalExitRequest{}); err != nil {
		t.Fatalf("WaitForTerminalExit: %v", err)
	}
}
