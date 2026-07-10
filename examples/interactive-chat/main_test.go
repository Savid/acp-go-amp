package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"golang.org/x/term"
)

type fakeAgentConnection struct {
	mu                  sync.Mutex
	cwd                 string
	prompts             []string
	cancelled           []acp.SessionId
	closed              bool
	sessionID           acp.SessionId
	initErr             error
	newErr              error
	promptErr           error
	cancelErr           error
	promptWait          <-chan struct{}
	ignorePromptContext bool
}

func (f *fakeAgentConnection) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{}, f.initErr
}

func (f *fakeAgentConnection) NewSession(_ context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cwd = params.Cwd
	if f.sessionID == "" {
		f.sessionID = "session-1"
	}

	return acp.NewSessionResponse{SessionId: f.sessionID}, f.newErr
}

func (f *fakeAgentConnection) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	f.mu.Lock()
	if len(params.Prompt) > 0 && params.Prompt[0].Text != nil {
		f.prompts = append(f.prompts, params.Prompt[0].Text.Text)
	}
	wait := f.promptWait
	promptErr := f.promptErr
	f.mu.Unlock()

	if wait != nil {
		if f.ignorePromptContext {
			<-wait
		} else {
			select {
			case <-wait:
			case <-ctx.Done():
				return acp.PromptResponse{}, ctx.Err()
			}
		}
	}

	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, promptErr
}

func (f *fakeAgentConnection) Cancel(_ context.Context, params acp.CancelNotification) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cancelled = append(f.cancelled, params.SessionId)

	return f.cancelErr
}

func (f *fakeAgentConnection) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true

	return acp.CloseSessionResponse{}, nil
}

func (f *fakeAgentConnection) promptsSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.prompts...)
}

func (f *fakeAgentConnection) cancelledSnapshot() []acp.SessionId {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]acp.SessionId(nil), f.cancelled...)
}

func (f *fakeAgentConnection) closedSnapshot() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.closed
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

type signalWriter struct {
	needle string
	seen   chan struct{}
	once   sync.Once
}

func newSignalWriter(needle string) *signalWriter {
	return &signalWriter{
		needle: needle,
		seen:   make(chan struct{}),
	}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), w.needle) {
		w.once.Do(func() { close(w.seen) })
	}

	return len(p), nil
}

func mustContain(t *testing.T, text string, sub string) {
	t.Helper()

	if !strings.Contains(text, sub) {
		t.Fatalf("missing %q in %q", sub, text)
	}
}

func mustNotContain(t *testing.T, text string, sub string) {
	t.Helper()

	if strings.Contains(text, sub) {
		t.Fatalf("unexpected %q in %q", sub, text)
	}
}

func mustNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustError(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func eqStrings(a []string, b []string) bool {
	return reflect.DeepEqual(a, b)
}

func eventually(t *testing.T, cond func() bool, wait time.Duration, tick time.Duration) {
	t.Helper()

	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(tick)
	}

	t.Fatal("condition was not met in time")
}

func TestChatClientFilePermissionAndTerminalMethods(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	c := chatClient{ui: newChatUI(&output)}
	path := filepath.Join(t.TempDir(), "note.txt")

	_, err := c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{
		Path:    path,
		Content: "hello",
	})
	mustNoError(t, err)

	read, err := c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: path})
	mustNoError(t, err)

	if read.Content != "hello" {
		t.Fatalf("content = %q, want hello", read.Content)
	}

	_, err = c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: "relative"})
	mustError(t, err)

	_, err = c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: "relative"})
	mustError(t, err)

	_, err = c.ReadTextFile(context.Background(), acp.ReadTextFileRequest{Path: filepath.Join(t.TempDir(), "missing.txt")})
	mustError(t, err)

	parentFile := filepath.Join(t.TempDir(), "parent")
	mustNoError(t, os.WriteFile(parentFile, []byte("file"), 0o600))
	_, err = c.WriteTextFile(context.Background(), acp.WriteTextFileRequest{Path: filepath.Join(parentFile, "child.txt")})
	mustError(t, err)

	title := "Run tool"
	resp, err := c.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		ToolCall: acp.ToolCallUpdate{Title: &title},
		Options: []acp.PermissionOption{
			{OptionId: "reject", Kind: acp.PermissionOptionKindRejectOnce},
			{OptionId: "allow", Kind: acp.PermissionOptionKindAllowAlways},
		},
	})
	mustNoError(t, err)

	if resp.Outcome.Selected == nil || resp.Outcome.Selected.OptionId != "allow" {
		t.Fatalf("permission outcome = %#v, want selected allow", resp.Outcome)
	}

	if !strings.Contains(output.String(), "permission> Run tool") {
		t.Fatalf("permission notice missing: %q", output.String())
	}

	resp, err = c.RequestPermission(context.Background(), acp.RequestPermissionRequest{})
	mustNoError(t, err)

	if resp.Outcome.Cancelled == nil {
		t.Fatal("expected cancelled permission outcome for empty request")
	}

	terminal, err := c.CreateTerminal(context.Background(), acp.CreateTerminalRequest{})
	mustNoError(t, err)

	if terminal.TerminalId != "terminal-1" {
		t.Fatalf("terminal id = %q, want terminal-1", terminal.TerminalId)
	}

	terminalOutput, err := c.TerminalOutput(context.Background(), acp.TerminalOutputRequest{})
	mustNoError(t, err)

	if terminalOutput.Truncated {
		t.Fatal("terminal output truncated")
	}

	_, err = c.KillTerminal(context.Background(), acp.KillTerminalRequest{})
	mustNoError(t, err)
	_, err = c.ReleaseTerminal(context.Background(), acp.ReleaseTerminalRequest{})
	mustNoError(t, err)
	_, err = c.WaitForTerminalExit(context.Background(), acp.WaitForTerminalExitRequest{})
	mustNoError(t, err)
}

func TestChatUIAndSessionUpdates(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)

	if ui.output != io.Writer(&output) {
		t.Fatal("ui.output is not the provided writer")
	}

	if newChatUI(nil).output != io.Writer(os.Stdout) {
		t.Fatal("newChatUI(nil).output is not os.Stdout")
	}

	c := chatClient{ui: ui}
	status := acp.ToolCallStatusCompleted
	line := 12
	messageID := "33333333-3333-4333-8333-333333333333"
	emptyID := ""

	ui.writeHeader("/repo")
	ui.writePrompt()
	ui.writeUserPrompt("hello")
	ui.writeNotice("empty", " ")

	if ui.messageDisplay(nil) != &ui.fallback {
		t.Fatal("nil message id did not map to fallback display")
	}

	if ui.messageDisplay(&emptyID) != &ui.fallback {
		t.Fatal("empty message id did not map to fallback display")
	}

	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{UserMessageChunk: &acp.SessionUpdateUserMessageChunk{Content: acp.TextBlock("user text")}},
	}))
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: acp.TextBlock("agent text")}},
	}))
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			MessageId: &messageID,
			Content:   acp.TextBlock("Hello"),
		}},
	}))
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			MessageId: &messageID,
			Content:   acp.TextBlock("Hello world"),
		}},
	}))
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{Content: acp.TextBlock("thinking")}},
	}))
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{
			ToolCallId: "tool-1",
			Title:      "Read file",
			Kind:       acp.ToolKindRead,
			Status:     acp.ToolCallStatusPending,
			Locations:  []acp.ToolCallLocation{{Path: "/repo/main.go", Line: &line}},
			RawInput:   map[string]any{"path": "/repo/main.go"},
			Content:    []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("reading /repo/main.go"))},
		}},
	}))
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "tool-1"}},
	}))
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{
			ToolCallId: "tool-1",
			Status:     &status,
			Content:    []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("done"))},
			RawOutput:  map[string]any{"bytes": 5},
		}},
	}))
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{}))

	ui.endAgentTurn(acp.StopReasonEndTurn)
	ui.endAgentTurn(acp.StopReasonCancelled)

	var direct bytes.Buffer
	display := messageDisplay{}
	display.write(&direct, "")
	display.write(&direct, "Hi")
	display.write(&direct, "Hi")
	display.write(&direct, "Hi there")
	display.write(&direct, " reset")

	if direct.String() != "Hi there reset" {
		t.Fatalf("direct = %q, want %q", direct.String(), "Hi there reset")
	}

	text := output.String()
	mustContain(t, text, "ACP interactive chat example")
	mustContain(t, text, "you> user text")
	mustContain(t, text, "amp> agent text")
	mustContain(t, text, "thinking> thinking")
	mustContain(t, text, "tool> Read file [pending, read] (tool-1)")
	mustContain(t, text, "location: /repo/main.go:12")
	mustContain(t, text, "content: reading /repo/main.go")
	mustContain(t, text, `"path": "/repo/main.go"`)
	mustContain(t, text, "tool> Read file updated [pending, read] (tool-1)")
	mustContain(t, text, "tool> Read file [completed, read] (tool-1)")
	mustContain(t, text, "result: done")
	mustContain(t, text, "stop> end_turn")
	mustContain(t, text, "stop> cancelled")
}

func TestRawInputEchoAndLineLayout(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writePrompt()
	ui.echoInputRune('h')
	ui.echoInputRune('i')

	if !ui.echoInputBackspace() {
		t.Fatal("expected backspace to report a change")
	}

	ui.echoInputRune('!')

	if !ui.echoInputSubmit() {
		t.Fatal("expected submit to report a visible prompt")
	}

	ui.writeUserPrompt("h!")
	ui.beginAgentTurn("h!")
	ui.writeAgentText(nil, "hello\nthere")
	ui.writeNotice("interrupt", "requested")
	ui.endAgentTurn(acp.StopReasonCancelled)

	text := output.String()
	mustContain(t, text, ansiAlternateScreen)
	mustContain(t, text, ansiHideCursor)
	mustContain(t, text, "message> h!")
	mustContain(t, text, "\r\n")
	mustContain(t, text, "you> h!")
	mustContain(t, text, "amp> hello\r\n  there")
	mustContain(t, text, "interrupt> requested")
	mustContain(t, text, "status> thinking")
	mustNotContain(t, text, "message> amp>")
	mustContain(t, text, "stop> cancelled")
}

func TestRawInputHeaderAndRestore(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writeHeader("/repo")
	ui.writePrompt()
	ui.setRawInput(false)

	text := output.String()
	mustContain(t, text, ansiAlternateScreen)
	mustContain(t, text, ansiHideCursor)
	mustContain(t, text, ansiShowCursor)
	mustContain(t, text, ansiMainScreen)

	frame := lastRenderedFrame(text)
	assertLineOrder(t, frame,
		"ACP interactive chat example",
		"cwd: /repo",
		"Enter submits, Esc interrupts the current turn, Ctrl-C exits",
		"type /exit or /quit to leave",
		"message> ",
	)
}

func TestRawInputQueuedPromptOrdering(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writeUserPrompt("first")
	ui.beginAgentTurn("first")
	ui.writeAgentText(nil, "working")
	ui.writeQueuedPrompt("second")

	frame := lastRenderedFrame(output.String())
	assertLineOrder(t, frame,
		"you> first",
		"amp> working",
		"queued> second",
		"status> thinking",
		"message> ",
	)
}

func TestRawInputKeepsToolAndStreamingMessageOrder(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writeUserPrompt("search")
	ui.beginAgentTurn("search")
	ui.writeToolCall(&acp.SessionUpdateToolCall{
		ToolCallId: "tool-1",
		Title:      "Web search",
		Status:     acp.ToolCallStatusPending,
	})
	ui.writeAgentText(nil, "answer")
	ui.writeAgentText(nil, "answer with more detail")

	frame := lastRenderedFrame(output.String())
	assertLineOrder(t, frame,
		"you> search",
		"tool> Web search [pending] (tool-1)",
		"amp> answer with more detail",
		"status> thinking",
		"message> ",
	)
	mustNotContain(t, frame, "amp> answer\r\namp> answer with more detail")
}

func TestReadRawInputEchoesVisiblePromptOnly(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writePrompt()

	events := make(chan inputEvent, 4)
	readRawInput(context.Background(), strings.NewReader("ab\x7fcd\n\x1b\x03"), events, ui)

	var got []inputEvent
	for event := range events {
		got = append(got, event)
	}

	if len(got) != 3 {
		t.Fatalf("events = %d, want 3", len(got))
	}

	if got[0].kind != inputPrompt || got[0].text != "acd" {
		t.Fatalf("event[0] = %+v, want prompt acd", got[0])
	}

	if got[1].kind != inputInterrupt {
		t.Fatalf("event[1] kind = %v, want interrupt", got[1].kind)
	}

	if got[2].kind != inputExit {
		t.Fatalf("event[2] kind = %v, want exit", got[2].kind)
	}

	mustContain(t, output.String(), "message> acd")
}

func TestRawInputScreenWrappingAndCropping(t *testing.T) {
	t.Parallel()

	if !eqStrings(wrapLine("abcdef", 4), []string{"abcd", "ef"}) {
		t.Fatalf("wrapLine(abcdef, 4) = %v", wrapLine("abcdef", 4))
	}

	if !eqStrings(wrapLine("", 4), []string{""}) {
		t.Fatalf("wrapLine(\"\", 4) = %v", wrapLine("", 4))
	}

	if !eqStrings(wrapLine("abcdef", 0), []string{"abcdef"}) {
		t.Fatalf("wrapLine(abcdef, 0) = %v", wrapLine("abcdef", 0))
	}

	ui := newChatUI(io.Discard)
	ui.width = 4
	ui.height = 3

	lines := ui.fitScreenLinesLocked([]string{"one", "twothree", "", "four"})
	if !eqStrings(lines, []string{"hree", "", "four"}) {
		t.Fatalf("fitScreenLinesLocked = %v", lines)
	}
}

func TestTerminalInputPathAndSize(t *testing.T) {
	originalIsTerminal := isTerminal
	originalGetTerminalSize := getTerminalSize
	originalMakeTerminalRaw := makeTerminalRaw
	originalRestoreTerminal := restoreTerminal
	t.Cleanup(func() {
		isTerminal = originalIsTerminal
		getTerminalSize = originalGetTerminalSize
		makeTerminalRaw = originalMakeTerminalRaw
		restoreTerminal = originalRestoreTerminal
	})

	isTerminal = func(int) bool { return true }

	var sizeErr error
	var width int
	var height int
	getTerminalSize = func(int) (int, int, error) {
		return width, height, sizeErr
	}

	var rawFD int
	makeTerminalRaw = func(fd int) (*term.State, error) {
		rawFD = fd

		return &term.State{}, nil
	}

	var restored bool
	restoreTerminal = func(int, *term.State) error {
		restored = true

		return nil
	}

	outputFile, err := os.CreateTemp(t.TempDir(), "output")
	mustNoError(t, err)
	defer outputFile.Close()

	ui := newChatUI(outputFile)
	if ui.fd != int(outputFile.Fd()) {
		t.Fatalf("ui.fd = %d, want %d", ui.fd, int(outputFile.Fd()))
	}

	sizeErr = errors.New("size")
	ui.refreshTerminalSizeLocked()
	if ui.width != 0 || ui.height != 0 {
		t.Fatalf("size after error = %dx%d, want 0x0", ui.width, ui.height)
	}

	sizeErr = nil
	width = 80
	height = 24
	ui.setRawInput(true)
	if ui.width != 80 || ui.height != 24 {
		t.Fatalf("size = %dx%d, want 80x24", ui.width, ui.height)
	}

	width = 100
	height = 30
	ui.setRawInput(true)
	if ui.width != 100 || ui.height != 30 {
		t.Fatalf("size = %dx%d, want 100x30", ui.width, ui.height)
	}

	inputFile, err := os.CreateTemp(t.TempDir(), "input")
	mustNoError(t, err)
	defer inputFile.Close()

	events, restore := startInputReader(context.Background(), inputFile, ui)
	event := <-events
	if event.kind != inputClosed {
		t.Fatalf("event kind = %v, want closed", event.kind)
	}
	restore()

	if rawFD != int(inputFile.Fd()) {
		t.Fatalf("rawFD = %d, want %d", rawFD, int(inputFile.Fd()))
	}

	if !restored {
		t.Fatal("terminal was not restored")
	}
}

func TestTerminalInputRawSetupFailureFallsBackToLineInput(t *testing.T) {
	originalIsTerminal := isTerminal
	originalMakeTerminalRaw := makeTerminalRaw
	t.Cleanup(func() {
		isTerminal = originalIsTerminal
		makeTerminalRaw = originalMakeTerminalRaw
	})

	isTerminal = func(int) bool { return true }
	makeTerminalRaw = func(int) (*term.State, error) {
		return nil, errors.New("raw")
	}

	inputFile, err := os.CreateTemp(t.TempDir(), "input")
	mustNoError(t, err)
	defer inputFile.Close()

	_, err = inputFile.WriteString("hello\n")
	mustNoError(t, err)
	_, err = inputFile.Seek(0, io.SeekStart)
	mustNoError(t, err)

	ui := newChatUI(io.Discard)
	ui.fd = int(inputFile.Fd())
	events, restore := startInputReader(context.Background(), inputFile, ui)
	defer restore()

	first := <-events
	if first.kind != inputPrompt || first.text != "hello" {
		t.Fatalf("first event = %+v, want prompt hello", first)
	}

	second := <-events
	if second.kind != inputClosed {
		t.Fatalf("second event kind = %v, want closed", second.kind)
	}
}

func TestChatUIRenderingBranches(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)

	ui.redrawScreenLocked()
	ui.redrawPromptLocked()
	ui.echoInputRune('x')

	if !ui.echoInputBackspace() {
		t.Fatal("expected first backspace to report a change")
	}

	if ui.echoInputBackspace() {
		t.Fatal("expected empty backspace to report no change")
	}

	if ui.echoInputSubmit() {
		t.Fatal("expected submit to report no visible prompt")
	}

	ui.agentOpen = true
	ui.writeUserPrompt("during turn")
	ui.promptVisible = true
	ui.writeAgentText(nil, "with redraw")
	ui.endAgentTurn(acp.StopReasonEndTurn)

	ui.agentOpen = true
	ui.promptVisible = true
	ui.writeBlockLocked("notice", []string{"redraw while open"})

	ui.agentOpen = true
	ui.promptVisible = false
	ui.writeBlockLocked("notice", []string{"first", "second"})

	ui.agentOpen = false
	ui.promptVisible = true
	ui.writeBlockLocked("notice", []string{"third"})
	ui.writeBlockLocked("empty", []string{" ", "\n"})

	var raw bytes.Buffer
	rawUI := newChatUI(&raw)
	rawUI.setRawInput(true)
	rawUI.writePrompt()

	if !rawUI.clearPromptLocked() {
		t.Fatal("expected first clearPromptLocked to report a change")
	}

	if rawUI.clearPromptLocked() {
		t.Fatal("expected second clearPromptLocked to report no change")
	}

	rawUI.inputText = "again"
	rawUI.redrawPromptLocked()

	text := output.String()
	mustContain(t, text, "x\b \b")
	mustContain(t, text, "you> during turn")
	mustContain(t, text, "amp> with redraw")
	mustContain(t, text, "amp> ")
	mustContain(t, text, "stop> end_turn")
	mustContain(t, text, "notice> redraw while open")
	mustContain(t, text, "notice> first")
	mustContain(t, text, "  second")
	mustContain(t, raw.String(), ansiClearLine)
	mustContain(t, raw.String(), "message> again")
}

func TestRawUIUpdatesUsageToolsAndSpinner(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	ui := newChatUI(&output)
	c := chatClient{ui: ui}

	ui.writeUsage(nil)
	mustNoError(t, c.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
			Size: 100,
			Used: 40,
			Cost: &acp.Cost{Amount: 0.25, Currency: "USD"},
		}},
	}))
	ui.tick()
	mustContain(t, output.String(), "usage> tokens 40/100, cost 0.2500 USD")

	var raw bytes.Buffer
	rawUI := newChatUI(&raw)
	rawUI.setRawInput(true)
	rawClient := chatClient{ui: rawUI}
	rawUI.tick()
	rawUI.writeUserChunk("raw user")
	rawUI.writeAgentText(nil, "late agent")
	rawUI.writeToolCall(nil)
	rawUI.writeToolCallUpdate(nil)

	title := "Search"
	kind := acp.ToolKindRead
	status := acp.ToolCallStatusCompleted
	rawUI.writeToolCallUpdate(&acp.SessionToolCallUpdate{
		ToolCallId: "tool-2",
		Title:      &title,
		Kind:       &kind,
		Status:     &status,
		Content:    []acp.ToolCallContent{acp.ToolContent(acp.TextBlock("result"))},
	})
	mustNoError(t, rawClient.SessionUpdate(context.Background(), acp.SessionNotification{
		Update: acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{Size: 200, Used: 50}},
	}))
	mustContain(t, raw.String(), "amp> late agent")
	mustContain(t, raw.String(), "tool> Search [completed, read] (tool-2)")
	mustContain(t, raw.String(), "result: result")
	rawUI.beginAgentTurn("spin")
	rawUI.writeUsage(&acp.SessionUsageUpdate{Size: 200, Used: 50})
	rawUI.tick()

	frame := lastRenderedFrame(raw.String())
	mustContain(t, frame, "you> raw user")
	mustContain(t, frame, "status> thinking.")
	mustContain(t, frame, "tokens 50/200")
}

func TestFormattingHelpers(t *testing.T) {
	t.Parallel()

	if len(cleanNoticeLines([]string{" ", "\n"})) != 0 {
		t.Fatal("cleanNoticeLines did not drop blank lines")
	}

	if appendSection(nil, " ", "\n") != nil {
		t.Fatal("appendSection with blank section is not nil")
	}

	if prefixedLines("x", []string{" ", "\n"}) != nil {
		t.Fatal("prefixedLines with blank lines is not nil")
	}

	if !eqStrings(appendSection([]string{"a", ""}, "b"), []string{"a", "", "b"}) {
		t.Fatalf("appendSection = %v", appendSection([]string{"a", ""}, "b"))
	}

	if usageSummary(nil) != "" {
		t.Fatal("usageSummary(nil) is not empty")
	}

	if got := usageSummary(&acp.SessionUsageUpdate{Size: 2, Used: 1}); got != "tokens 1/2" {
		t.Fatalf("usageSummary = %q, want tokens 1/2", got)
	}

	if got := usageSummary(&acp.SessionUsageUpdate{Cost: &acp.Cost{Amount: 1.25, Currency: "USD"}}); got != "cost 1.2500 USD" {
		t.Fatalf("usageSummary = %q, want cost 1.2500 USD", got)
	}

	if got := usageSummary(&acp.SessionUsageUpdate{
		Size: 2,
		Used: 1,
		Cost: &acp.Cost{Amount: 1.25, Currency: "USD"},
	}); got != "tokens 1/2, cost 1.2500 USD" {
		t.Fatalf("usageSummary = %q, want tokens 1/2, cost 1.2500 USD", got)
	}

	if got := formatElapsed(-time.Second); got != "0s" {
		t.Fatalf("formatElapsed = %q, want 0s", got)
	}

	if got := toolSummary("", &toolDisplay{}, true); got != "tool updated" {
		t.Fatalf("toolSummary = %q, want tool updated", got)
	}

	line := 4
	if toolLocationsLines(nil) != nil {
		t.Fatal("toolLocationsLines(nil) is not nil")
	}

	if toolLocationsLines([]acp.ToolCallLocation{{}}) != nil {
		t.Fatal("toolLocationsLines with empty location is not nil")
	}

	if got := toolLocationsLines([]acp.ToolCallLocation{
		{Path: "/a"},
		{Path: "/b", Line: &line},
	}); !eqStrings(got, []string{"locations: /a, /b:4"}) {
		t.Fatalf("toolLocationsLines = %v", got)
	}

	uri := "file://image.png"
	mimeType := "text/plain"
	mustContain(t, contentBlockLines("content", acp.ContentBlock{
		Image: &acp.ContentBlockImage{MimeType: "image/png", Uri: &uri, Data: "abcd"},
	})[0], "image/png file://image.png (4 base64 chars)")
	mustContain(t, contentBlockLines("content", acp.AudioBlock("abcd", "audio/wav"))[0], "audio/wav (4 base64 chars)")

	if got := contentBlockLines("content", acp.ResourceLinkBlock("guide", "file://guide")); !eqStrings(got, []string{"resource: guide file://guide"}) {
		t.Fatalf("resource link lines = %v", got)
	}

	if got := contentBlockLines("content", acp.ResourceBlock(acp.EmbeddedResourceResource{
		TextResourceContents: &acp.TextResourceContents{Uri: "file://text", Text: "hello"},
	})); !eqStrings(got, []string{"resource: file://text", "content: hello"}) {
		t.Fatalf("resource lines = %v", got)
	}

	mustContain(t, contentBlockLines("content", acp.ResourceBlock(acp.EmbeddedResourceResource{
		BlobResourceContents: &acp.BlobResourceContents{Uri: "file://blob", MimeType: &mimeType, Blob: "abcd"},
	}))[0], "file://blob text/plain (4 base64 chars)")

	if contentBlockLines("content", acp.ContentBlock{}) != nil {
		t.Fatal("empty content block lines is not nil")
	}

	if embeddedResourceLines("content", acp.EmbeddedResourceResource{}) != nil {
		t.Fatal("empty embedded resource lines is not nil")
	}

	oldText := "old"
	contentLines := toolContentLines("result", []acp.ToolCallContent{
		acp.ToolDiffContent("/tmp/a", "new", oldText),
		acp.ToolTerminalRef("terminal-1"),
	})
	mustContain(t, strings.Join(contentLines, "\n"), "diff: /tmp/a, old 3 chars, new 3 chars")
	mustContain(t, strings.Join(contentLines, "\n"), "terminal: terminal-1")

	if diffContentLines(nil) != nil {
		t.Fatal("diffContentLines(nil) is not nil")
	}

	if got := diffContentLines(&acp.ToolCallContentDiff{Path: "/tmp/new", NewText: "new"}); !eqStrings(got, []string{"diff: /tmp/new, new 3 chars"}) {
		t.Fatalf("diffContentLines = %v", got)
	}

	mustContain(t, strings.Join(toolValueLines("input", map[string]any{"a": 1}), "\n"), `"a": 1`)
	mustContain(t, toolValueLines("input", math.NaN())[0], "NaN")

	if toolValueLines("input", nil) != nil {
		t.Fatal("toolValueLines(nil) is not nil")
	}

	if previewLines("content", " ", 10) != nil {
		t.Fatal("previewLines with blank text is not nil")
	}

	if got := previewLines("content", "abcdef", 3); !eqStrings(got, []string{"content: abc ..."}) {
		t.Fatalf("previewLines = %v", got)
	}

	multiline := strings.Join(previewLines("content", "a\nb\nc", 4), "\n")
	mustContain(t, multiline, "content:")
	mustContain(t, multiline, "  a")

	if value, truncated := truncateString("abc", 0); value != "" || !truncated {
		t.Fatalf("truncateString(abc, 0) = %q %v, want \"\" true", value, truncated)
	}

	if value, truncated := truncateString("abcdef", 3); value != "abc" || !truncated {
		t.Fatalf("truncateString(abcdef, 3) = %q %v, want abc true", value, truncated)
	}
}

func TestStateHelpers(t *testing.T) {
	t.Parallel()

	ui := newChatUI(io.Discard)
	ui.appendCurrentLinesLocked([]string{" ", "\n"})
	if len(ui.currentBlocks) != 0 {
		t.Fatal("appendCurrentLinesLocked kept blank lines")
	}

	ui.upsertMessageBlockLocked(&messageDisplay{text: " "})
	if len(ui.currentBlocks) != 0 {
		t.Fatal("upsertMessageBlockLocked kept blank display")
	}

	ui.queued = []string{"first", "target"}
	if !ui.removeQueuedPromptLocked("target") {
		t.Fatal("removeQueuedPromptLocked did not find target")
	}

	if !eqStrings(ui.queued, []string{"first"}) {
		t.Fatalf("queued = %v, want [first]", ui.queued)
	}

	if ui.removeQueuedPromptLocked("missing") {
		t.Fatal("removeQueuedPromptLocked found missing prompt")
	}

	if ui.historyEndsWithLocked("you", "") {
		t.Fatal("historyEndsWithLocked matched blank text")
	}

	ui.history = []string{"you> hello"}
	if ui.historyEndsWithLocked("you", "bye") {
		t.Fatal("historyEndsWithLocked matched wrong text")
	}

	if !ui.historyEndsWithLocked("you", "hello") {
		t.Fatal("historyEndsWithLocked did not match hello")
	}
}

func TestHandleInputEventBranches(t *testing.T) {
	t.Parallel()

	conn := &fakeAgentConnection{}
	ui := newChatUI(io.Discard)
	var enqueued []string
	var queuedFlags []bool
	enqueue := func(prompt string, queued bool) {
		enqueued = append(enqueued, prompt)
		queuedFlags = append(queuedFlags, queued)
	}

	control := handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputPrompt, text: " /quit "}, false, enqueue)
	if !control.exit {
		t.Fatal("quit did not request exit")
	}

	if len(conn.cancelledSnapshot()) != 0 {
		t.Fatal("quit without running cancelled the session")
	}

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputPrompt, text: "quit"}, true, enqueue)
	if !control.exit {
		t.Fatal("quit while running did not request exit")
	}

	if !eqSessionIDs(conn.cancelledSnapshot(), []acp.SessionId{"session-1"}) {
		t.Fatalf("cancelled = %v, want [session-1]", conn.cancelledSnapshot())
	}

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputPrompt, text: " work "}, true, enqueue)
	if control.exit {
		t.Fatal("work prompt requested exit")
	}

	if !eqStrings(enqueued, []string{"work"}) {
		t.Fatalf("enqueued = %v, want [work]", enqueued)
	}

	if !reflect.DeepEqual(queuedFlags, []bool{true}) {
		t.Fatalf("queuedFlags = %v, want [true]", queuedFlags)
	}

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputInterrupt}, false, enqueue)
	mustNoError(t, control.err)

	conn.cancelErr = errors.New("cancel")
	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputInterrupt}, true, enqueue)
	mustError(t, control.err)

	conn.cancelErr = nil
	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputInterrupt}, true, enqueue)
	mustNoError(t, control.err)

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputExit}, true, enqueue)
	if !control.exit {
		t.Fatal("exit event did not request exit")
	}

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputClosed}, false, enqueue)
	if !control.inputDone {
		t.Fatal("closed event did not set inputDone")
	}

	err := errors.New("input")
	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputError, err: err}, false, enqueue)
	if !errors.Is(control.err, err) {
		t.Fatalf("control.err = %v, want %v", control.err, err)
	}

	control = handleInputEvent(context.Background(), conn, ui, "session-1", inputEvent{kind: inputEventKind(99)}, false, enqueue)
	if control != (inputControl{}) {
		t.Fatalf("unknown event control = %+v, want zero", control)
	}
}

func eqSessionIDs(a []acp.SessionId, b []acp.SessionId) bool {
	return reflect.DeepEqual(a, b)
}

func TestTurnTickerBranches(t *testing.T) {
	t.Parallel()

	var ticker turnTicker
	ticker.start()
	first := ticker.ch
	ticker.start()
	if ticker.ch != first {
		t.Fatal("second start replaced the ticker channel")
	}

	ticker.stop()
	ticker.stop()
}

func TestReadRawInputAdditionalBranches(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := make(chan inputEvent, 1)
	readRawInput(ctx, strings.NewReader("ignored"), events, newChatUI(io.Discard))
	event := <-events
	if event.kind != inputExit {
		t.Fatalf("cancelled ctx event kind = %v, want exit", event.kind)
	}

	events = make(chan inputEvent, 1)
	readRawInput(context.Background(), errReader{}, events, newChatUI(io.Discard))
	event = <-events
	if event.kind != inputError || event.err == nil {
		t.Fatalf("errReader event = %+v, want error", event)
	}

	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)
	ui.writePrompt()
	events = make(chan inputEvent, 4)
	readRawInput(context.Background(), strings.NewReader("\n\b\ta\r\x01"), events, ui)

	var got []inputEvent
	for event := range events {
		got = append(got, event)
	}

	if len(got) != 2 {
		t.Fatalf("events = %d, want 2", len(got))
	}

	if got[0].kind != inputPrompt || got[0].text != "a" {
		t.Fatalf("event[0] = %+v, want prompt a", got[0])
	}

	if got[1].kind != inputClosed {
		t.Fatalf("event[1] kind = %v, want closed", got[1].kind)
	}

	mustContain(t, output.String(), "message> ")
}

func TestReadLineInputContextCancel(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan inputEvent, 2)
	done := make(chan struct{})
	go func() {
		readLineInput(ctx, reader, events)
		close(done)
	}()

	cancel()
	event := <-events
	if event.kind != inputExit {
		t.Fatalf("event kind = %v, want exit", event.kind)
	}
	_ = reader.Close()
	_ = writer.Close()
	<-done
}

func TestLineInputEventControlC(t *testing.T) {
	t.Parallel()

	event := lineInputEvent(string(rune(0x03)))
	if event.kind != inputExit {
		t.Fatalf("event kind = %v, want exit", event.kind)
	}
}

func TestRunInteractiveLoopAdditionalBranches(t *testing.T) {
	originalStartInput := startInput
	t.Cleanup(func() {
		startInput = originalStartInput
	})

	var restored bool
	startInput = func(context.Context, io.Reader, *chatUI) (<-chan inputEvent, func()) {
		events := make(chan inputEvent)
		close(events)

		return events, func() { restored = true }
	}
	err := runInteractiveLoop(context.Background(), &fakeAgentConnection{}, newChatUI(io.Discard), strings.NewReader(""), "session-1", "")
	mustNoError(t, err)
	if !restored {
		t.Fatal("input was not restored")
	}

	startInput = originalStartInput
	err = runInteractiveLoop(context.Background(), &fakeAgentConnection{promptErr: errors.New("prompt")}, newChatUI(io.Discard), strings.NewReader(""), "session-1", "go")
	mustError(t, err)

	err = runInteractiveLoop(context.Background(), &fakeAgentConnection{promptErr: context.Canceled}, newChatUI(io.Discard), strings.NewReader(""), "session-1", "go")
	mustNoError(t, err)

	err = runInteractiveLoop(context.Background(), &fakeAgentConnection{}, newChatUI(io.Discard), strings.NewReader(""), "session-1", "   ")
	mustNoError(t, err)

	err = runInteractiveLoop(context.Background(), &fakeAgentConnection{}, newChatUI(io.Discard), strings.NewReader("/quit\n"), "session-1", "")
	mustNoError(t, err)
}

func TestRunInteractiveLoopWritesPromptAfterPromptCompletes(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})

	output := newSignalWriter("\nmessage> ")
	done := make(chan error, 1)
	go func() {
		done <- runInteractiveLoop(context.Background(), &fakeAgentConnection{}, newChatUI(output), reader, "session-1", "go")
	}()

	select {
	case <-output.seen:
	case <-time.After(time.Second):
		t.Fatal("prompt was not written")
	}

	mustNoError(t, writer.Close())
	mustNoError(t, <-done)
}

func TestRunInteractiveLoopTicksAndCancelsRunningPrompt(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	conn := &fakeAgentConnection{promptWait: release}
	var output bytes.Buffer
	ui := newChatUI(&output)
	ui.setRawInput(true)

	done := make(chan error, 1)
	go func() {
		done <- runInteractiveLoop(context.Background(), conn, ui, strings.NewReader(""), "session-1", "wait")
	}()

	eventually(t, func() bool {
		return len(conn.promptsSnapshot()) == 1
	}, time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	close(release)
	mustNoError(t, <-done)
	if ui.spinner <= 0 {
		t.Fatalf("spinner = %d, want > 0", ui.spinner)
	}
}

func TestRunInteractiveLoopContextCancelWhileRunning(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	conn := &fakeAgentConnection{promptWait: release, ignorePromptContext: true}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- runInteractiveLoop(ctx, conn, newChatUI(io.Discard), strings.NewReader(""), "session-1", "wait")
	}()

	eventually(t, func() bool {
		return len(conn.promptsSnapshot()) == 1
	}, time.Second, 10*time.Millisecond)
	cancel()
	mustNoError(t, <-done)
	if !eqSessionIDs(conn.cancelledSnapshot(), []acp.SessionId{"session-1"}) {
		t.Fatalf("cancelled = %v, want [session-1]", conn.cancelledSnapshot())
	}
	close(release)
}

func lastRenderedFrame(output string) string {
	index := strings.LastIndex(output, ansiClearScreenHome)
	if index < 0 {
		return output
	}

	return output[index:]
}

func assertLineOrder(t *testing.T, text string, values ...string) {
	t.Helper()

	previous := -1
	for _, value := range values {
		index := strings.Index(text, value)
		if index == -1 {
			t.Fatalf("missing %q in %q", value, text)
		}

		if index <= previous {
			t.Fatalf("%q was not after previous value in %q", value, text)
		}

		previous = index
	}
}

func TestRunChat(t *testing.T) {
	t.Parallel()

	conn := &fakeAgentConnection{}
	ui := newChatUI(io.Discard)
	err := runChat(context.Background(), conn, ui, strings.NewReader("first\n"), "/repo", "initial")
	mustNoError(t, err)

	if conn.cwd != "/repo" {
		t.Fatalf("cwd = %q, want /repo", conn.cwd)
	}

	if !eqStrings(conn.promptsSnapshot(), []string{"initial", "first"}) {
		t.Fatalf("prompts = %v, want [initial first]", conn.promptsSnapshot())
	}

	if !conn.closedSnapshot() {
		t.Fatal("session was not closed")
	}

	conn = &fakeAgentConnection{}
	err = runChat(context.Background(), conn, newChatUI(io.Discard), strings.NewReader(""), "/repo", "")
	mustNoError(t, err)
	if !conn.closedSnapshot() {
		t.Fatal("session was not closed for empty input")
	}
}

func TestRunChatQueuesInputWhilePromptRuns(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	conn := &fakeAgentConnection{promptWait: release}
	done := make(chan error, 1)

	go func() {
		done <- runChat(context.Background(), conn, newChatUI(io.Discard), strings.NewReader("first\nsecond\n"), "/repo", "")
	}()

	eventually(t, func() bool {
		prompts := conn.promptsSnapshot()

		return len(prompts) == 1 && prompts[0] == "first"
	}, time.Second, 10*time.Millisecond)

	close(release)

	mustNoError(t, <-done)
	if !eqStrings(conn.promptsSnapshot(), []string{"first", "second"}) {
		t.Fatalf("prompts = %v, want [first second]", conn.promptsSnapshot())
	}

	if !conn.closedSnapshot() {
		t.Fatal("session was not closed")
	}
}

func TestRunChatEscapeInterruptsCurrentPrompt(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	conn := &fakeAgentConnection{promptWait: release}
	done := make(chan error, 1)

	go func() {
		done <- runChat(context.Background(), conn, newChatUI(io.Discard), strings.NewReader("first\n\x1b\n"), "/repo", "")
	}()

	eventually(t, func() bool {
		return len(conn.cancelledSnapshot()) == 1
	}, time.Second, 10*time.Millisecond)
	if !eqSessionIDs(conn.cancelledSnapshot(), []acp.SessionId{"session-1"}) {
		t.Fatalf("cancelled = %v, want [session-1]", conn.cancelledSnapshot())
	}

	close(release)

	mustNoError(t, <-done)
	if !conn.closedSnapshot() {
		t.Fatal("session was not closed")
	}
}

func TestRunChatContextCancelQuitsCleanly(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	conn := &fakeAgentConnection{}
	done := make(chan error, 1)

	go func() {
		done <- runChat(ctx, conn, newChatUI(io.Discard), reader, "/repo", "")
	}()

	cancel()

	mustNoError(t, <-done)
	if !conn.closedSnapshot() {
		t.Fatal("session was not closed")
	}
}

func TestRunChatErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		conn   *fakeAgentConnection
		input  io.Reader
		prompt string
	}{
		{name: "initialize", conn: &fakeAgentConnection{initErr: errors.New("init")}, input: strings.NewReader(""), prompt: ""},
		{name: "new session", conn: &fakeAgentConnection{newErr: errors.New("new")}, input: strings.NewReader(""), prompt: ""},
		{name: "initial prompt", conn: &fakeAgentConnection{promptErr: errors.New("prompt")}, input: strings.NewReader(""), prompt: "initial"},
		{name: "reader", conn: &fakeAgentConnection{}, input: errReader{}, prompt: ""},
		{name: "loop prompt", conn: &fakeAgentConnection{promptErr: errors.New("prompt")}, input: strings.NewReader("again\n"), prompt: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := runChat(context.Background(), tc.conn, newChatUI(io.Discard), tc.input, "/repo", tc.prompt)
			mustError(t, err)
		})
	}
}

func TestRunPrompt(t *testing.T) {
	t.Parallel()

	conn := &fakeAgentConnection{}
	var output bytes.Buffer
	ui := newChatUI(&output)
	mustNoError(t, runPrompt(context.Background(), conn, ui, "session-1", "hello"))
	if !eqStrings(conn.promptsSnapshot(), []string{"hello"}) {
		t.Fatalf("prompts = %v, want [hello]", conn.promptsSnapshot())
	}
	mustContain(t, output.String(), "amp> ")

	conn.promptErr = errors.New("prompt")
	mustError(t, runPrompt(context.Background(), conn, ui, "session-1", "again"))

	conn.promptErr = context.Canceled
	mustError(t, runPrompt(context.Background(), conn, ui, "session-1", "cancel"))
	mustContain(t, output.String(), "stop> cancelled")
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
	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
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
	getwd = func() (string, error) { return "/repo", nil }

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}

	if !eqStrings(conn.promptsSnapshot(), []string{"hello"}) {
		t.Fatalf("prompts = %v, want [hello]", conn.promptsSnapshot())
	}

	if !closed {
		t.Fatal("agent was not closed")
	}

	if !waited {
		t.Fatal("agent was not waited on")
	}

	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}

	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{}}, nil
	}

	code = run(context.Background(), nil, strings.NewReader(""), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
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

	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
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
	os.Args = []string{"interactive-chat", "hello"}

	main()

	if gotCode != 0 {
		t.Fatalf("exit code = %d, want 0", gotCode)
	}
}

func TestRunErrors(t *testing.T) {
	originalStartAgent := startAgent
	originalGetwd := getwd
	t.Cleanup(func() {
		startAgent = originalStartAgent
		getwd = originalGetwd
	})

	getwd = func() (string, error) { return "", errors.New("cwd") }

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr); code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	mustContain(t, stderr.String(), "cwd")

	getwd = func() (string, error) { return "/repo", nil }
	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return nil, errors.New("start")
	}

	stderr.Reset()
	if code := run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr); code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	mustContain(t, stderr.String(), "start")

	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{initErr: errors.New("init")}}, nil
	}

	stderr.Reset()
	if code := run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr); code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	mustContain(t, stderr.String(), "init")

	startAgent = func(context.Context, *chatUI, io.Writer) (*startedAgent, error) {
		return &startedAgent{conn: &fakeAgentConnection{initErr: context.Canceled}}, nil
	}

	stderr.Reset()
	if code := run(context.Background(), []string{"hello"}, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestStartAgentProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses a POSIX shell script")
	}

	binDir := t.TempDir()
	goPath := filepath.Join(binDir, "go")
	err := os.WriteFile(goPath, []byte("#!/bin/sh\nwhile IFS= read -r _; do :; done\n"), 0o755)
	mustNoError(t, err)

	t.Setenv("PATH", binDir)

	var output bytes.Buffer
	agent, err := startAgentProcess(context.Background(), newChatUI(&output), io.Discard)
	mustNoError(t, err)
	if agent.conn == nil {
		t.Fatal("agent.conn is nil")
	}

	agent.close()
	mustNoError(t, agent.wait())
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

	agent, err := startAgentProcess(context.Background(), newChatUI(io.Discard), io.Discard)
	mustNoError(t, err)
	if agent.conn == nil {
		t.Fatal("agent.conn is nil")
	}

	agent.close()
	mustNoError(t, agent.wait())
	if gotName != "go" {
		t.Fatalf("command name = %q, want go", gotName)
	}

	if !eqStrings(gotArgs, []string{"run", agentPackage}) {
		t.Fatalf("command args = %v, want [run %s]", gotArgs, agentPackage)
	}
}

func TestStartAgentProcessStartError(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	agent, err := startAgentProcess(context.Background(), newChatUI(io.Discard), io.Discard)
	mustError(t, err)
	if agent != nil {
		t.Fatal("agent is not nil after start error")
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
	agent, err := startAgentProcess(context.Background(), newChatUI(io.Discard), io.Discard)
	mustError(t, err)
	if agent != nil {
		t.Fatal("agent is not nil after stdin pipe error")
	}

	commandContext = func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "sh", "-c", "cat")
		cmd.Stdout = io.Discard

		return cmd
	}
	agent, err = startAgentProcess(context.Background(), newChatUI(io.Discard), io.Discard)
	mustError(t, err)
	if agent != nil {
		t.Fatal("agent is not nil after stdout pipe error")
	}
}

func TestQuitCommand(t *testing.T) {
	t.Parallel()

	if !quitCommand(" /QUIT ") {
		t.Fatal("quitCommand did not recognize /QUIT")
	}

	if quitCommand("keep going") {
		t.Fatal("quitCommand matched a normal prompt")
	}
}

func TestPrintError(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	printError(&stderr, fmt.Errorf("bad"))
	mustContain(t, stderr.String(), "interactive-chat: bad")
}
