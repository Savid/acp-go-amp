package ampacp

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/savid/acp-go-amp/internal/amp"
)

func TestMain(m *testing.M) {
	if root := os.Getenv("ACP_GO_AMP_TEST_NATIVE"); root != "" {
		os.Exit(fakeAmp(root, os.Args[1:]))
	}
	os.Exit(m.Run())
}

// continueFlags is the exact flag set the adapter passes to threads continue.
// Dropping --execute would make the native CLI interactive and dropping
// --stream-json-input would make it read the prompt from argv, so the fixture
// refuses an invocation that does not carry them, and any other subcommand
// that does.
var continueFlags = []string{"--execute", "--stream-json-thinking", "--stream-json-input", "--no-archive-after-execute", "--plugin-ready-timeout"}

const subcommandContinue = "continue"

func fakeFlagsMatch(subcommand string, args []string) bool {
	for _, flag := range continueFlags {
		if slices.Contains(args, flag) != (subcommand == subcommandContinue) {
			return false
		}
	}

	return true
}

func fakeAmp(root string, args []string) int {
	if slices.Contains(args, "--version") {
		return fakeVersion()
	}
	index := slices.Index(args, "threads")
	if index < 0 || index+1 >= len(args) {
		return 2
	}
	if !fakeFlagsMatch(args[index+1], args) {
		fmt.Fprintln(os.Stderr, "unexpected native argv: "+strings.Join(args, " "))

		return 2
	}
	mode := modeMedium
	if at := slices.Index(args, "--mode"); at >= 0 {
		mode = args[at+1]
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		panic(err)
	}
	if args[index+1] == "new" {
		id := "T-" + fakeUUID()
		snapshot := nativeSnapshot{ID: id, Messages: []nativeMessage{}}
		fakeSave(root, snapshot)
		base := os.Getenv("AMP_URL")
		if base == "" {
			base = "https://ampcode.com"
		}
		fmt.Println(strings.TrimRight(base, "/") + "/threads/" + id)

		return 0
	}
	if index+2 >= len(args) {
		return 2
	}
	id := args[index+2]
	data, err := os.ReadFile(filepath.Join(root, id+".json"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Thread does not exist")

		return 1
	}
	if args[index+1] == "export" {
		return fakeExport(root, id, data)
	}
	if args[index+1] == "delete" {
		fakeWrite(filepath.Join(root, id+".deleted"), nil)
		if err := os.Remove(filepath.Join(root, id+".json")); err != nil {
			panic(err)
		}

		return 0
	}

	if args[index+1] != subcommandContinue {
		return 2
	}

	return fakeContinue(root, id, data, mode)
}

func fakeContinue(root, id string, data []byte, mode string) int {
	var snapshot nativeSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		panic(err)
	}
	state := "idle"
	if value, err := os.ReadFile(filepath.Join(root, id+".state")); err == nil {
		state = string(value)
	}
	messages := fakePluginMessages(snapshot.Messages)
	if err := os.Remove(filepath.Join(root, id+".initializing")); err == nil {
		messages = []amp.Message{}
	}
	fakeBridgeEvent(id, "ready", map[string]any{"state": state, "messages": messages})
	input, scanner, ok := fakeInput()
	if !ok {
		return 0
	}
	var prompt strings.Builder
	for _, part := range input.Message.Content {
		if textValue(part["type"]) == "text" {
			prompt.WriteString(textValue(part["text"]))
		}
	}
	text := prompt.String()
	fakePrint(map[string]any{"type": "system", "subtype": "init", "session_id": id, "agent_mode": mode})
	if text == "COMPACT" {
		// Native compaction inserts its summary before the prompt that
		// triggered it; the plugin message view of this process omits it.
		summary := map[string]any{"type": "summary", "summary": map[string]any{"type": "message", "summary": "earlier turns summarized"}}
		snapshot.Messages = append(snapshot.Messages, nativeMessage{ID: int64(len(snapshot.Messages) + 1), Role: roleInfo, Content: []map[string]any{summary}})
	}
	user := nativeMessage{ID: int64(len(snapshot.Messages) + 1), Role: "user", Content: input.Message.Content, AgentMode: mode}
	snapshot.Messages = append(snapshot.Messages, user)
	fakeSave(root, snapshot)
	startID := snapshot.Messages[len(snapshot.Messages)-1].ProtocolID
	// The process that compacts does not list the summary; a later attachment
	// lists it without content.
	omitInfo := text == "COMPACT"
	startAt := len(fakeViewMessages(snapshot.Messages, omitInfo)) - 1
	fakeBridgeEvent(id, "start", map[string]any{"id": startID})
	fakePrint(map[string]any{"type": "user", "session_id": id, "message": map[string]any{"content": input.Message.Content}})
	if text == "SLOW" {
		for !fakeCancelled() {
			time.Sleep(5 * time.Millisecond)
		}
		fakeBridgeEvent(id, "cancel.ack", nil)
		fakeBridgeEnd(snapshot, startID, startAt, "cancelled", omitInfo)
		// Native cancellation can complete without a stream-json result.
		for scanner.Scan() {
		}

		return 0
	}
	noiseIfAsked(text)
	if gate, ok := strings.CutPrefix(text, "GATE "); ok {
		fakeWaitForGate(gate)
	}
	if text == "CRASH" {
		return 17
	}
	if text == "DETACH_RUNNING" {
		fakeWrite(filepath.Join(root, id+".state"), []byte("running"))

		return 17
	}
	if text == "NO_IDENTITY" {
		fakePrint(map[string]any{"type": "assistant", "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "orphan"}}}})
		// The adapter refuses the frame and kills this process; sleeping keeps
		// the exit status the kill's rather than a race with a clean exit.
		time.Sleep(time.Hour)
	}
	if text == "DRIFT" {
		fakePrint(map[string]any{"type": "assistant", "session_id": "T-00000000-0000-0000-0000-000000000000", "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "wrong"}}}})

		return 0
	}
	reply := "hello " + text
	if text == "ENV" {
		data, err := json.Marshal(map[string]string{"env": os.Getenv("SESSION_VALUE"), "path": os.Getenv("PATH"), "home": os.Getenv("HOME"), "internal": os.Getenv("ACP_GO_AMP_INTERNAL_CALLER")})
		if err != nil {
			panic(err)
		}
		reply = string(data)
	}
	if text == "RECALL" {
		data, _ := json.Marshal(snapshot.Messages)
		reply = string(data)
	}
	if text == "TOOL" || strings.HasPrefix(text, "IMAGE") {
		call := "tool-" + fakeUUID()
		tool := map[string]any{"type": "tool_use", "id": call, "name": "Bash", "input": map[string]any{"cmd": "printf fixture"}}
		assistant := nativeMessage{ID: int64(len(snapshot.Messages) + 1), Role: "assistant", Content: []map[string]any{tool}, State: map[string]any{"type": "complete"}}
		snapshot.Messages = append(snapshot.Messages, assistant)
		fakePrint(map[string]any{"type": "assistant", "session_id": id, "message": map[string]any{"content": assistant.Content, "stop_reason": "tool_use"}})
		var result any = "fixture"
		if strings.HasPrefix(text, "IMAGE") {
			var buffer bytes.Buffer
			if err := png.Encode(&buffer, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
				panic(err)
			}
			result = map[string]any{"content": []any{map[string]any{"type": "image", "data": base64.StdEncoding.EncodeToString(buffer.Bytes()), "mimeType": "image/png"}}}
		}
		nativeResult := map[string]any{"type": "tool_result", "toolUseID": call, "run": map[string]any{"status": "done", "result": result}}
		snapshot.Messages = append(snapshot.Messages, nativeMessage{ID: int64(len(snapshot.Messages) + 1), Role: "user", Content: []map[string]any{nativeResult}})
		encoded, _ := json.Marshal(result)
		fakePrint(map[string]any{"type": "user", "session_id": id, "message": map[string]any{"content": []any{map[string]any{"type": "tool_result", "tool_use_id": call, "content": string(encoded)}}}})
	}
	content := []map[string]any{{"type": "thinking", "thinking": "considering"}, {"type": "text", "text": reply}}
	assistant := nativeMessage{ID: int64(len(snapshot.Messages) + 1), Role: "assistant", Content: content, State: map[string]any{"type": "complete"}, Usage: map[string]any{"inputTokens": 10, "outputTokens": 3, "model": "native-model", "maxInputTokens": 123456}}
	snapshot.Messages = append(snapshot.Messages, assistant)
	fakeSave(root, snapshot)
	if text == "DETACH_DONE" {
		return 17
	}
	fakeHoldExport(root, snapshot, text)
	fakePrint(map[string]any{"type": "assistant", "session_id": id, "message": map[string]any{"content": content, "stop_reason": "end_turn", "usage": map[string]any{"input_tokens": 10, "output_tokens": 3, "max_tokens": 123456}}})

	status := "done"
	if strings.HasSuffix(text, "PROVIDER_ERROR") {
		status = "error"
	}
	fakeBridgeEnd(snapshot, startID, startAt, status, omitInfo)

	return fakeTerminal(id, text, reply)
}

func fakeTerminal(id, text, reply string) int {
	terminal := map[string]any{"type": "result", "subtype": "success", "session_id": id, "result": reply}
	if strings.HasSuffix(text, "PROVIDER_ERROR") {
		terminal["is_error"] = true
		terminal["error"] = "provider fixture refusal"
	}
	if text == "TURN_LIMIT" {
		terminal["is_error"] = true
		terminal["subtype"] = resultMaxTurns
	}
	fakePrint(terminal)
	if strings.HasSuffix(text, "PROVIDER_ERROR") || text == "TURN_LIMIT" {
		return 1
	}
	if text == "DUPLICATE" {
		fakePrint(terminal)
		fakePrint(map[string]any{"type": "assistant", "session_id": id, "message": map[string]any{"content": []any{map[string]any{"type": "text", "text": "late output"}}}})
	}

	return 0
}

// fakeWaitForGate blocks until the test creates the named file, so a test can
// hold a turn open and release it without a sleep.
func fakeWaitForGate(path string) {
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func fakeUUID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func fakeSave(root string, snapshot nativeSnapshot) {
	for i := range snapshot.Messages {
		if len(snapshot.Messages[i].ProtocolID) == 0 {
			snapshot.Messages[i].ProtocolID = json.RawMessage(fmt.Sprintf("\"M-%022d\"", snapshot.Messages[i].ID))
		}
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(root, snapshot.ID+".json"), data, 0o600); err != nil {
		panic(err)
	}
}

// noiseIfAsked writes one non-frame line to stdout and one to stderr for the
// NOISE prompt.
func noiseIfAsked(text string) {
	if text == "NOISE" {
		fmt.Fprintln(os.Stderr, "chatter on stderr")
		fmt.Println("not a json record at all")
	}
}

func fakePrint(value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func fakeCancelled() bool {
	_, err := os.Stat(filepath.Join(os.Getenv(amp.InternalEnvPrefix+"BRIDGE"), "cancel"))

	return err == nil
}

func fakeBridgeEvent(id, kind string, fields map[string]any) {
	directory := os.Getenv(amp.InternalEnvPrefix + "BRIDGE")
	if directory == "" {
		panic("native bridge is required")
	}
	data := map[string]any{"type": kind, "threadId": id}
	maps.Copy(data, fields)
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	file, err := os.OpenFile(filepath.Join(directory, "events"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	if _, err = file.Write(append(encoded, '\n')); err != nil {
		panic(err)
	}
	if err = file.Close(); err != nil {
		panic(err)
	}
}

func fakeBridgeEnd(snapshot nativeSnapshot, id json.RawMessage, from int, status string, omitInfo bool) {
	messages := fakeViewMessages(snapshot.Messages, omitInfo)
	fakeBridgeEvent(snapshot.ID, "end", map[string]any{"id": id, "status": status, "messages": messages[from:]})
	fakeBridgeEvent(snapshot.ID, "settled", map[string]any{"state": "idle", "messages": messages})
}

func fakePluginMessages(messages []nativeMessage) []amp.Message {
	return fakeViewMessages(messages, false)
}

// fakeViewMessages renders the plugin message view: images and summary parts
// are never exposed, and an info message is omitted entirely when asked.
func fakeViewMessages(messages []nativeMessage, omitInfo bool) []amp.Message {
	result := make([]amp.Message, 0, len(messages))
	for _, message := range messages {
		if omitInfo && message.Role == roleInfo {
			continue
		}
		parts := make([]map[string]any, 0, len(message.Content))
		for _, part := range message.Content {
			switch part["type"] {
			case "image", "summary":
				continue
			case "tool_result":
				run, _ := part["run"].(map[string]any)
				encoded, _ := json.Marshal(run["result"])
				part = map[string]any{"type": "tool_result", "toolUseID": part["toolUseID"], "status": run["status"], "output": string(encoded)}
			}
			parts = append(parts, part)
		}
		result = append(result, amp.Message{ID: message.ProtocolID, Role: message.Role, Content: parts})
	}

	return result
}

func fakeInput() (input struct {
	Message struct {
		Content []map[string]any `json:"content"`
	} `json:"message"`
}, scanner *bufio.Scanner, ok bool) {
	scanner = bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 65536), amp.MaxNativeBytes)
	if !scanner.Scan() {
		return input, scanner, false
	}
	ok = json.Unmarshal(scanner.Bytes(), &input) == nil

	return input, scanner, ok
}

func fakeVersion() int {
	version := os.Getenv("ACP_GO_AMP_TEST_VERSION")
	if version == "" {
		version = amp.MinimumVersion + "-gfixture"
	}
	fmt.Println(version)

	return 0
}

func fakeWrite(path string, data []byte) {
	if err := os.WriteFile(path, data, 0o600); err != nil {
		panic(err)
	}
}

func fakeExport(root, id string, data []byte) int {
	path := filepath.Join(root, id+".stale")
	if stale, err := os.ReadFile(path); err == nil {
		data = stale
		file, err := os.OpenFile(path+".reads", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			panic(err)
		}
		if _, err = file.WriteString("read\n"); err != nil {
			panic(err)
		}
		if err = file.Close(); err != nil {
			panic(err)
		}
	}
	fmt.Println(string(data))

	return 0
}

func fakeHoldExport(root string, snapshot nativeSnapshot, prompt string) {
	if prompt != "STALE_EXPORT" {
		return
	}
	snapshot.Messages = snapshot.Messages[:len(snapshot.Messages)-1]
	data, err := json.Marshal(snapshot)
	if err != nil {
		panic(err)
	}
	fakeWrite(filepath.Join(root, snapshot.ID+".stale"), data)
}
