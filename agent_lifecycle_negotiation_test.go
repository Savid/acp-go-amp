package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

// invalidParamsCode is the JSON-RPC code every refused reserved value carries,
// and methodNotFoundCode the one an unrouted method carries.
const (
	invalidParamsCode  = -32602
	methodNotFoundCode = -32601
)

// lifecycleOffer is the object a host stamps on initialize. It carries exactly
// one member, and the decoded wire form of a JSON number is a float64.
func lifecycleOffer(version any) map[string]any {
	return map[string]any{lifecycle.MetaKey: map[string]any{"version": version}}
}

// lifecycleCorrelation is the value a host stamps on every session/prompt while
// version 1 is negotiated.
func lifecycleCorrelation(submissionID, clientNonce string) map[string]any {
	return map[string]any{lifecycle.MetaKey: map[string]any{
		"version": 1.0,
		"submission": map[string]any{
			"submissionId": submissionID,
			"clientNonce":  clientNonce,
		},
	}}
}

// lifecycleClient records every notification the agent emits so the emitted
// stream can be reduced back through the same reducer the canonical vectors
// drive.
type lifecycleClient struct {
	mu      sync.Mutex
	updates []acp.SessionNotification
	// onAgentChunk fires once the turn has streamed visible content, which is the
	// point a cancel can land mid-turn.
	onAgentChunk func()
}

func (c *lifecycleClient) Done() <-chan struct{} { return nil }

func (c *lifecycleClient) SessionUpdate(_ context.Context, notification acp.SessionNotification) error {
	c.mu.Lock()
	c.updates = append(c.updates, notification)
	notify := c.onAgentChunk
	c.mu.Unlock()

	if notify != nil && notification.Update.AgentMessageChunk != nil {
		c.mu.Lock()
		c.onAgentChunk = nil
		c.mu.Unlock()
		notify()
	}

	return nil
}

func (c *lifecycleClient) NotifyExtension(context.Context, string, any) error { return nil }

func (c *lifecycleClient) snapshot() []acp.SessionNotification {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]acp.SessionNotification(nil), c.updates...)
}

// envelopes returns the lifecycle envelopes in delivery order, each paired with
// the notification that carried it.
func (c *lifecycleClient) envelopes(t *testing.T) []map[string]any {
	t.Helper()

	carried := make([]map[string]any, 0)

	for _, notification := range c.snapshot() {
		envelope, ok := notification.Meta[lifecycle.MetaKey].(map[string]any)
		if !ok {
			continue
		}

		require.NotNil(t, notification.Update.SessionInfoUpdate, "the envelope rode an ineligible carrier")
		carried = append(carried, envelope)
	}

	return carried
}

// eventTypes names the emitted events in order.
func (c *lifecycleClient) eventTypes(t *testing.T) []string {
	t.Helper()

	envelopes := c.envelopes(t)
	types := make([]string, 0, len(envelopes))

	for _, envelope := range envelopes {
		event, ok := envelope["event"].(map[string]any)
		require.True(t, ok)

		name, ok := event["type"].(string)
		require.True(t, ok)

		types = append(types, name)
	}

	return types
}

// reduceEmittedStream drives every recorded notification through the reducer,
// which is the only measure that proves the emitted bytes are wire-legal.
func reduceEmittedStream(t *testing.T, client *lifecycleClient, negotiated lifecycle.Negotiated) *lifecycle.Reducer {
	t.Helper()

	reducer := lifecycle.NewReducer(lifecycle.Options{Negotiated: negotiated})

	for index, notification := range client.snapshot() {
		params, err := json.Marshal(notification)
		require.NoError(t, err)

		err = reducer.ReduceSessionUpdate(params)
		if err == nil {
			continue
		}

		require.ErrorIs(t, err, lifecycle.ErrNoEnvelope, "notification %d refused: %v", index, err)
	}

	return reducer
}

// negotiatedAnswer is the answer a degenerate prompt-contained configuration
// gives. Every test agent here runs the ordinary same-identity mode, which
// proves no whole-tree vacancy.
func negotiatedAnswer() lifecycle.Negotiated {
	return lifecycle.Negotiated{Version: 1, ActivityKinds: []lifecycle.ActivityKind{}}
}

// lifecycleHarnessSource is a native harness whose terminal shape is selected by
// the prompt text, so one fixture drives every ending a turn can have.
const lifecycleHarnessSource = `package main

import (
	"io"
	"os"
	"strings"
	"time"
)

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "version" {
			os.Stdout.WriteString("0.0.1784765892-gfake\n")
			return
		}
	}

	for i, arg := range os.Args[1:] {
		if arg == "T-00000000-0000-0000-0000-000000000000" {
			os.Stderr.WriteString("Thread not found\n")
			os.Exit(1)
		}

		if arg == "threads" && i+1 < len(os.Args[1:]) && os.Args[1:][i+1] == "list" {
			os.Stdout.WriteString("[]\n")
			return
		}
	}

	input, _ := io.ReadAll(os.Stdin)
	os.Stdout.WriteString("{\"type\":\"system\",\"subtype\":\"init\",\"cwd\":\"/tmp/project\",\"session_id\":\"T-lifecycle-thread\"}\n")

	stop := "end_turn"
	if strings.Contains(string(input), "token-ceiling") {
		stop = "max_tokens"
	}
	os.Stdout.WriteString("{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"answer\"}],\"stop_reason\":\"" + stop + "\"},\"session_id\":\"T-lifecycle-thread\"}\n")

	if strings.Contains(string(input), "hang") {
		time.Sleep(30 * time.Second)
	}

	switch {
	case strings.Contains(string(input), "turn-ceiling"):
		os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"error_max_turns\",\"is_error\":true,\"session_id\":\"T-lifecycle-thread\"}\n")
	case strings.Contains(string(input), "provider-failure"):
		os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"error_during_execution\",\"is_error\":true,\"error\":\"upstream refused\",\"session_id\":\"T-lifecycle-thread\"}\n")
	default:
		os.Stdout.WriteString("{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"done\",\"session_id\":\"T-lifecycle-thread\"}\n")
	}
}
`

func lifecycleHarness(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	source := filepath.Join(dir, "lifecycle_amp.go")
	require.NoError(t, os.WriteFile(source, []byte(lifecycleHarnessSource), 0o600))

	// The name carries a platform-correct extension: a host that honours
	// PATHEXT does not resolve an extensionless file as an executable, so a
	// harness written without one is unreachable rather than merely unusual.
	path := filepath.Join(dir, testExecutableName("amp"))

	out, err := exec.Command("go", "build", "-o", path, source).CombinedOutput()
	require.NoError(t, err, "build lifecycle harness: %s", out)

	return path
}

// lifecycleAgent establishes an agent whose host offered version 1, with a
// recording client attached and one session open.
func lifecycleAgent(t *testing.T, offer map[string]any) (*Agent, *lifecycleClient, acp.SessionId) {
	t.Helper()

	client := &lifecycleClient{}
	agent, sessionID := lifecycleAgentWithClient(t, offer, client)

	return agent, client, sessionID
}

// lifecycleAgentWithClient establishes an agent whose host offered version 1,
// with the given connection attached and one session open.
func lifecycleAgentWithClient(t *testing.T, offer map[string]any, client agentClient) (*Agent, acp.SessionId) {
	t.Helper()

	t.Setenv("AMP_API_KEY", "conformance-key")

	agent := NewAgent(testContainmentOptions([]Option{
		WithExecutablePath(lifecycleHarness(t)),
		WithScratchDir(testScratchDir(t)),
	})...)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })

	agent.setConnection(client)

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: offer})
	require.NoError(t, err)

	session, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	return agent, session.SessionId
}

func lifecyclePrompt(sessionID acp.SessionId, text, submissionID, nonce string) acp.PromptRequest {
	request := TextPromptRequest(sessionID, "lifecycle-turn", text)
	request.Meta = lifecycleCorrelation(submissionID, nonce)

	return request
}

// TestLifecycleAnswerIsExact pins the version-1 scalar advertisement and the
// facts ordinary same-identity execution can prove.
func TestLifecycleAnswerIsExact(t *testing.T) {
	resp, err := newTestAgent().Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"version":                 1,
		"updatesOutsidePrompt":    false,
		"authoritativeQuiescence": false,
		"activityKinds":           []string{},
	}, resp.Meta[lifecycle.MetaKey])
}

func TestLifecycleAuthorityAnswerIsAuthoritative(t *testing.T) {
	agent := NewAgent(WithHostAuthority(newRecordingAuthority()))
	resp, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"version":                 1,
		"updatesOutsidePrompt":    false,
		"authoritativeQuiescence": true,
		"quiescenceSource":        "process-containment",
		"activityKinds":           []string{},
	}, resp.Meta[lifecycle.MetaKey])
}

// TestLifecycleAnswerNeverRidesAgentCapabilities pins the placement: the answer
// is on the response's own _meta, because later protocol work relocates
// capability objects and initialize _meta survives that move unchanged.
func TestLifecycleAnswerNeverRidesAgentCapabilities(t *testing.T) {
	resp, err := newTestAgent().Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)
	require.Contains(t, resp.Meta, lifecycle.MetaKey)
	require.NotContains(t, resp.AgentCapabilities.Meta, lifecycle.MetaKey)
}

// TestLifecycleNoOfferExposesNoSurface pins that an absent offer means the host
// asked for nothing: no key on the response and no envelope on the connection.
func TestLifecycleNoOfferExposesNoSurface(t *testing.T) {
	agent, client, sessionID := lifecycleAgent(t, nil)

	resp, err := agent.Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)
	require.NotContains(t, resp.Meta, lifecycle.MetaKey)
	require.False(t, agent.negotiatedLifecycle().Present())

	_, err = agent.Prompt(t.Context(), TextPromptRequest(sessionID, "unnegotiated", "hello"))
	require.NoError(t, err)
	require.Empty(t, client.envelopes(t))
}

// TestLifecycleUnsupportedOfferIsRefused pins the exact version scalar.
func TestLifecycleUnsupportedOfferIsRefused(t *testing.T) {
	_, err := newTestAgent().Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(2.0)})
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, invalidParamsCode, requestErr.Code)
}

// TestLifecycleOfferStrictness pins the one family literal this adapter
// validates on initialize itself: every refusal names the exact member path.
func TestLifecycleOfferStrictness(t *testing.T) {
	for _, tc := range []struct {
		name  string
		offer map[string]any
		field string
	}{
		{"non-object", map[string]any{lifecycle.MetaKey: "v1"}, `_meta["acp-go.dev/lifecycle"]`},
		{"unknown member", map[string]any{lifecycle.MetaKey: map[string]any{
			"version": 1.0, "vendorExtension": true,
		}}, `_meta["acp-go.dev/lifecycle"].vendorExtension`},
		{"missing version", map[string]any{lifecycle.MetaKey: map[string]any{}}, `_meta["acp-go.dev/lifecycle"].version`},
		{"array version", lifecycleOffer([]any{1.0}), `_meta["acp-go.dev/lifecycle"].version`},
		{"non-integer version", lifecycleOffer(1.5), `_meta["acp-go.dev/lifecycle"].version`},
		{"non-numeric version", lifecycleOffer("1"), `_meta["acp-go.dev/lifecycle"].version`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newTestAgent().Initialize(t.Context(), acp.InitializeRequest{Meta: tc.offer})

			var requestErr *acp.RequestError

			require.ErrorAs(t, err, &requestErr)
			require.Equal(t, invalidParamsCode, requestErr.Code)
			require.Equal(t, map[string]any{jsonFieldError: valUnsupported, jsonFieldField: tc.field}, requestErr.Data)
		})
	}
}

// TestLifecycleAcceptsAnEmbeddedHostsIntegerOffer pins that an embedding Go host
// writing a Go int offers the same version a decoded wire float64 does.
func TestLifecycleAcceptsAnEmbeddedHostsIntegerOffer(t *testing.T) {
	resp, err := newTestAgent().Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1)})
	require.NoError(t, err)
	require.Contains(t, resp.Meta, lifecycle.MetaKey)

	resp, err = newTestAgent().Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(json.Number("1"))})
	require.NoError(t, err)
	require.Contains(t, resp.Meta, lifecycle.MetaKey)
}

// TestLifecyclePromptIncarnationShape pins the ordered stream one prompt emits:
// the snapshot opens it before acceptance, acceptance echoes the submission
// verbatim, running precedes the transcript, and the terminal idle carries the
// truthful stop reason and outcome.
func TestLifecyclePromptIncarnationShape(t *testing.T) {
	agent, client, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	resp, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-1", "nonce-1"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)

	require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update"}, client.eventTypes(t))

	envelopes := client.envelopes(t)
	streamID, ok := envelopes[0]["streamId"].(string)
	require.True(t, ok)

	for index, envelope := range envelopes {
		require.Equal(t, 1, envelope["version"])
		require.Equal(t, streamID, envelope["streamId"], "one prompt is one incarnation")
		require.Equal(t, uint64(index+1), envelope["sequence"])
	}

	accepted, ok := envelopes[1]["event"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "sub-1", accepted["submissionId"])
	require.Equal(t, "nonce-1", accepted["clientNonce"])
	require.NotContains(t, accepted, "runId")

	idle, ok := envelopes[3]["event"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "idle", idle["state"])
	require.Equal(t, "end_turn", idle["stopReason"])
	require.Equal(t, "success", idle["outcome"])

	state := reduceEmittedStream(t, client, negotiatedAnswer()).State()
	require.Equal(t, streamID, state.StreamID)
	require.Equal(t, uint64(4), state.ReducedThrough)
	require.Len(t, state.Turns, 1)
	require.True(t, state.Turns[0].Terminal)
	require.Equal(t, lifecycle.OutcomeSuccess, state.Turns[0].Outcome)
}

// TestLifecycleSnapshotOpensAFullShapeSessionsPrompt pins the opening snapshot
// against the params a host actually sends rather than the minimal ones a test
// can get away with: a populated mcpServers array and an object-valued `_meta`
// carrying this adapter's own object beside a foreign namespace's, handed over
// as wire bytes so the connection's own decode runs. A session's shape is no
// part of the incarnation boundary — the boundary is the prompt — so the stream
// still opens with the snapshot before acceptance.
func TestLifecycleSnapshotOpensAFullShapeSessionsPrompt(t *testing.T) {
	t.Setenv("AMP_API_KEY", "conformance-key")

	agent := NewAgent(testContainmentOptions([]Option{
		WithExecutablePath(lifecycleHarness(t)),
		WithScratchDir(testScratchDir(t)),
	})...)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })

	client := &lifecycleClient{}
	agent.setConnection(client)

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)

	connection := &localAgentConnection{agent: agent}
	connection.initialized.Store(true)

	opened, err := json.Marshal(NewSessionRequest(t.TempDir(),
		WithSessionMCPServers(
			StdioMCPServer("stdio", "printf", []string{"ok"}, map[string]string{"A": "B"}),
			HTTPMCPServer("http", "https://example.com/mcp", map[string]string{"H": "V"}),
		),
		WithSessionAdditionalDirectories(t.TempDir()),
		WithSessionRawEvents(true),
		WithSessionAmpOptions(NewAmpOptions(
			WithAmpEnv(map[string]string{"AMP_URL": "https://amp.example.test"}),
			WithAmpMode("high"),
		)),
		WithSessionMeta(map[string]any{"example.com/trace": map[string]any{"traceId": "t-1"}}),
	))
	require.NoError(t, err)

	created, requestErr := connection.handle(t.Context(), acp.AgentMethodSessionNew, opened)
	require.Nil(t, requestErr)

	session, ok := created.(acp.NewSessionResponse)
	require.True(t, ok)
	require.Empty(t, client.envelopes(t), "opening a session owes the stream nothing")

	submitted, err := json.Marshal(lifecyclePrompt(session.SessionId, "hello", "sub-1", "nonce-1"))
	require.NoError(t, err)

	answered, requestErr := connection.handle(t.Context(), acp.AgentMethodSessionPrompt, submitted)
	require.Nil(t, requestErr)

	settled, ok := answered.(acp.PromptResponse)
	require.True(t, ok)
	require.Equal(t, acp.StopReasonEndTurn, settled.StopReason)

	require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update"}, client.eventTypes(t))

	state := reduceEmittedStream(t, client, negotiatedAnswer()).State()
	require.Len(t, state.Turns, 1)
	require.True(t, state.Turns[0].Terminal)
	require.Equal(t, lifecycle.OutcomeSuccess, state.Turns[0].Outcome)
}

// TestLifecycleOpensOneIncarnationPerPrompt pins that each prompt is a distinct
// stream with its own snapshot and sequence space.
func TestLifecycleOpensOneIncarnationPerPrompt(t *testing.T) {
	agent, client, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	for _, submission := range []string{"sub-1", "sub-2"} {
		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", submission, "nonce-"+submission))
		require.NoError(t, err)
	}

	envelopes := client.envelopes(t)
	require.Len(t, envelopes, 8)

	first, second := envelopes[0]["streamId"], envelopes[4]["streamId"]
	require.NotEqual(t, first, second, "each incarnation is a distinct stream")
	require.Equal(t, uint64(1), envelopes[4]["sequence"], "each stream owns its sequence space")

	// The second incarnation inherits nothing: its projection holds only its own
	// turn.
	state := reduceEmittedStream(t, client, negotiatedAnswer()).State()
	require.Equal(t, second, state.StreamID)
	require.Len(t, state.Turns, 1)
}

// TestLifecycleEnvelopeNeverRidesAnEntityPatch pins the carrier rule against the
// vendor metadata this adapter already stamps on entity patches: those
// notifications carry per-entity reduction semantics a conformant client may
// coalesce, so an envelope placed there would be unrecoverable.
func TestLifecycleEnvelopeNeverRidesAnEntityPatch(t *testing.T) {
	_, client, _ := lifecycleAgent(t, lifecycleOffer(1.0))

	agent, _, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))
	agent.setConnection(client)

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-1", "nonce-1"))
	require.NoError(t, err)

	transcript := 0

	for _, notification := range client.snapshot() {
		if notification.Update.SessionInfoUpdate != nil {
			require.Contains(t, notification.Meta, lifecycle.MetaKey)

			continue
		}

		transcript++

		require.NotContains(t, notification.Meta, lifecycle.MetaKey)
		require.Nil(t, notification.Update.AvailableCommandsUpdate)

		for _, entity := range []map[string]any{
			entityMeta(notification.Update.AgentMessageChunk),
			entityMeta(notification.Update.UserMessageChunk),
		} {
			require.NotContains(t, entity, lifecycle.MetaKey)
		}
	}

	require.Positive(t, transcript, "the turn emitted ordinary updates to guard")
}

func entityMeta(chunk any) map[string]any {
	switch typed := chunk.(type) {
	case *acp.SessionUpdateAgentMessageChunk:
		if typed != nil {
			return typed.Meta
		}
	case *acp.SessionUpdateUserMessageChunk:
		if typed != nil {
			return typed.Meta
		}
	}

	return nil
}

// TestLifecyclePromptCorrelationStrictness pins that a prompt this adapter
// cannot correlate is refused before dispatch, so no frame reaches the harness.
func TestLifecyclePromptCorrelationStrictness(t *testing.T) {
	agent, client, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	for _, tc := range []struct {
		name  string
		meta  map[string]any
		field string
	}{
		{"missing key", nil, `_meta["acp-go.dev/lifecycle"]`},
		{"non-object", map[string]any{lifecycle.MetaKey: 1.0}, `_meta["acp-go.dev/lifecycle"]`},
		{"unknown member", map[string]any{lifecycle.MetaKey: map[string]any{
			"version": 1.0, "submission": map[string]any{"submissionId": "s", "clientNonce": "n"}, "streamId": "x",
		}}, `_meta["acp-go.dev/lifecycle"].streamId`},
		{"unsupported version", map[string]any{lifecycle.MetaKey: map[string]any{
			"version": 2.0, "submission": map[string]any{"submissionId": "s", "clientNonce": "n"},
		}}, `_meta["acp-go.dev/lifecycle"].version`},
		{"missing version", map[string]any{lifecycle.MetaKey: map[string]any{
			"submission": map[string]any{"submissionId": "s", "clientNonce": "n"},
		}}, `_meta["acp-go.dev/lifecycle"].version`},
		{"missing submission", map[string]any{lifecycle.MetaKey: map[string]any{"version": 1.0}}, `_meta["acp-go.dev/lifecycle"].submission`},
		{"unknown submission member", map[string]any{lifecycle.MetaKey: map[string]any{
			"version": 1.0, "submission": map[string]any{"submissionId": "s", "clientNonce": "n", "turnId": "t"},
		}}, `_meta["acp-go.dev/lifecycle"].submission.turnId`},
		{"missing nonce", map[string]any{lifecycle.MetaKey: map[string]any{
			"version": 1.0, "submission": map[string]any{"submissionId": "s"},
		}}, `_meta["acp-go.dev/lifecycle"].submission.clientNonce`},
		{"empty submission id", map[string]any{lifecycle.MetaKey: map[string]any{
			"version": 1.0, "submission": map[string]any{"submissionId": "", "clientNonce": "n"},
		}}, `_meta["acp-go.dev/lifecycle"].submission.submissionId`},
		{"non-string run id", map[string]any{lifecycle.MetaKey: map[string]any{
			"version": 1.0, "submission": map[string]any{"submissionId": "s", "clientNonce": "n", "runId": 1.0},
		}}, `_meta["acp-go.dev/lifecycle"].submission.runId`},
		{"over-bound identifier", map[string]any{lifecycle.MetaKey: map[string]any{
			"version": 1.0, "submission": map[string]any{"submissionId": strings.Repeat("s", 4097), "clientNonce": "n"},
		}}, `_meta["acp-go.dev/lifecycle"].submission.submissionId`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := TextPromptRequest(sessionID, "lifecycle-turn", "hello")
			request.Meta = tc.meta

			_, err := agent.Prompt(t.Context(), request)

			var requestErr *acp.RequestError

			require.ErrorAs(t, err, &requestErr)
			require.Equal(t, invalidParamsCode, requestErr.Code)
			require.Equal(t, map[string]any{jsonFieldError: valUnsupported, jsonFieldField: tc.field}, requestErr.Data)
		})
	}

	require.Empty(t, client.envelopes(t), "a refused prompt opens no incarnation")
}

// TestLifecycleKeyRejectedWhenNotNegotiated pins the other half of the
// correlation rule: with the extension not negotiated the key must be absent,
// and a present one is rejected the same way.
func TestLifecycleKeyRejectedWhenNotNegotiated(t *testing.T) {
	agent, _, sessionID := lifecycleAgent(t, nil)

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-1", "nonce-1"))

	var requestErr *acp.RequestError

	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, invalidParamsCode, requestErr.Code)
	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, `_meta["acp-go.dev/lifecycle"]`, data[jsonFieldField])
}

// TestLifecycleStopReasonIsTheHarnessOwn pins that a turn stopped by a ceiling
// says so rather than reporting a clean end or a provider failure, on both the
// v1 response and the lifecycle turn's recorded outcome.
func TestLifecycleStopReasonIsTheHarnessOwn(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prompt     string
		stopReason acp.StopReason
		outcome    lifecycle.Outcome
	}{
		{"token ceiling", "token-ceiling", acp.StopReasonMaxTokens, lifecycle.OutcomeLimit},
		{"turn ceiling", "turn-ceiling", acp.StopReasonMaxTurnRequests, lifecycle.OutcomeLimit},
		{"ordinary end", "hello", acp.StopReasonEndTurn, lifecycle.OutcomeSuccess},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agent, client, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

			resp, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, tc.prompt, "sub-1", "nonce-1"))
			require.NoError(t, err)
			require.Equal(t, tc.stopReason, resp.StopReason)

			state := reduceEmittedStream(t, client, negotiatedAnswer()).State()
			require.Equal(t, tc.outcome, state.Turns[0].Outcome)
			require.Equal(t, string(tc.stopReason), state.Turns[0].StopReason)
		})
	}
}

// TestLifecycleFailedTurnStatesNoStopReason pins that a failure records its
// outcome and borrows no stop reason: no ACP v1 stop reason names a failure.
func TestLifecycleFailedTurnStatesNoStopReason(t *testing.T) {
	agent, client, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "provider-failure", "sub-1", "nonce-1"))
	require.Error(t, err)

	idle, ok := client.envelopes(t)[3]["event"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "idle", idle["state"])
	require.NotContains(t, idle, "stopReason")
	require.Equal(t, "failed", idle["outcome"])

	state := reduceEmittedStream(t, client, negotiatedAnswer()).State()
	require.Equal(t, lifecycle.OutcomeFailed, state.Turns[0].Outcome)
}

// TestLifecycleFailedTurnPersistsWhatItStreamed pins the single commit point: a
// turn that ends in a failure still commits the frames the client already saw,
// so durable and emitted state never diverge.
func TestLifecycleFailedTurnPersistsWhatItStreamed(t *testing.T) {
	agent, client, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "provider-failure", "sub-1", "nonce-1"))
	require.Error(t, err)

	stored, err := agent.store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, stored, "a failed turn persists the frames it streamed")

	emitted := 0

	for _, notification := range client.snapshot() {
		if notification.Update.AgentMessageChunk != nil {
			emitted++
		}
	}

	require.Positive(t, emitted)
	require.Len(t, stored, 3, "every native frame the turn streamed reached the store")
}

// TestLifecycleOutcomeForTranslatesEveryV1Ending pins the derivation the ending
// idle carries, including the refusal ending this harness never produces.
func TestLifecycleOutcomeForTranslatesEveryV1Ending(t *testing.T) {
	for _, tc := range []struct {
		reason acp.StopReason
		want   lifecycleOutcome
	}{
		{acp.StopReasonEndTurn, lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess}},
		{acp.StopReasonCancelled, lifecycleOutcome{stopReason: "cancelled", outcome: lifecycle.OutcomeCancelled}},
		{acp.StopReasonMaxTokens, lifecycleOutcome{stopReason: "max_tokens", outcome: lifecycle.OutcomeLimit}},
		{acp.StopReasonMaxTurnRequests, lifecycleOutcome{stopReason: "max_turn_requests", outcome: lifecycle.OutcomeLimit}},
		{acp.StopReasonRefusal, lifecycleOutcome{stopReason: "refusal", outcome: lifecycle.OutcomeRefused}},
	} {
		require.Equal(t, tc.want, lifecycleOutcomeFor(acp.PromptResponse{StopReason: tc.reason}, nil))
	}

	require.Equal(t,
		lifecycleOutcome{outcome: lifecycle.OutcomeFailed},
		lifecycleOutcomeFor(acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, context.Canceled),
	)
}

// TestLifecycleKeyRefusedOnEverySurfaceThatNeverCarriesIt pins that a family
// literal is never foreign and never a no-op.
func TestLifecycleKeyRefusedOnEverySurfaceThatNeverCarriesIt(t *testing.T) {
	agent, _, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))
	key := map[string]any{lifecycle.MetaKey: map[string]any{"version": 1.0}}

	calls := map[string]func() error{
		"session/new": func() error {
			request := NewSessionRequest(t.TempDir())
			request.Meta = key
			_, err := agent.NewSession(t.Context(), request)

			return err
		},
		"session/load": func() error {
			_, err := agent.LoadSession(t.Context(), acp.LoadSessionRequest{SessionId: sessionID, Cwd: t.TempDir(), Meta: key})

			return err
		},
		"session/resume": func() error {
			_, err := agent.ResumeSession(t.Context(), acp.ResumeSessionRequest{SessionId: sessionID, Cwd: t.TempDir(), Meta: key})

			return err
		},
		"session/list": func() error {
			_, err := agent.ListSessions(t.Context(), acp.ListSessionsRequest{Meta: key})

			return err
		},
		"session/close": func() error {
			_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: sessionID, Meta: key})

			return err
		},
		"session/delete": func() error {
			_, err := agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: sessionID, Meta: key})

			return err
		},
		"session/set_config_option": func() error {
			_, err := agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{
				ValueId: &acp.SetSessionConfigOptionValueId{SessionId: sessionID, ConfigId: "mode", Value: "medium", Meta: key},
			})

			return err
		},
		// The variant a request chose decides where its `_meta` lives. Amp takes
		// no boolean option, but "this adapter has no such option" and "the key is
		// not read here" are different answers, and the family literal is refused
		// by name before the discriminator is judged.
		"session/set_config_option boolean": func() error {
			_, err := agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{
				Boolean: &acp.SetSessionConfigOptionBoolean{SessionId: sessionID, ConfigId: "mode", Type: "boolean", Value: true, Meta: key},
			})

			return err
		},
		// The method this adapter does not route is still an inbound surface: the
		// key is rejected by name rather than swallowed with the method.
		"session/set_mode": func() error {
			_, err := agent.SetSessionMode(t.Context(), acp.SetSessionModeRequest{SessionId: sessionID, ModeId: "high", Meta: key})

			return err
		},
		"logout": func() error {
			_, err := agent.Logout(t.Context(), acp.LogoutRequest{Meta: key})

			return err
		},
		// Amp advertises no auth method, so authenticate has its own refusal to
		// give. The family literal is rejected by name first: "the method does
		// not exist" and "the key is not read here" are different answers.
		"authenticate": func() error {
			_, err := agent.Authenticate(t.Context(), acp.AuthenticateRequest{MethodId: "amp", Meta: key})

			return err
		},
	}

	for method, call := range calls {
		t.Run(method, func(t *testing.T) {
			var requestErr *acp.RequestError

			require.ErrorAs(t, call(), &requestErr)
			require.Equal(t, invalidParamsCode, requestErr.Code)
			data, ok := requestErr.Data.(map[string]any)
			require.True(t, ok)
			require.Equal(t, `_meta["acp-go.dev/lifecycle"]`, data[jsonFieldField])
		})
	}
}

func TestLifecycleKeyRefusedBeforeExtensionDispatch(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	flow := fixture.mustAuthorizeMethod("lifecycle-refusal", authMethodAPIKey)
	record, ok, err := fixture.broker.ledger.read(authProviderID, "lifecycle-refusal")
	require.NoError(t, err)
	require.True(t, ok)

	fixture.broker.mu.Lock()
	baselineGeneration := fixture.broker.generation
	baselineFlow := fixture.broker.byID[flow.FlowID]
	baselineFlowState := baselineFlow.state
	baselineFlowReason := baselineFlow.reason
	baselineCredential := append([]byte(nil), baselineFlow.credential...)
	baselineFlows := len(fixture.broker.flows)
	baselineByID := len(fixture.broker.byID)
	baselineRetained := len(fixture.broker.retained)
	fixture.broker.mu.Unlock()

	fixture.agent.mu.Lock()
	baselineSessions := len(fixture.agent.sessions)
	fixture.agent.mu.Unlock()

	meta := map[string]any{lifecycle.MetaKey: map[string]any{"version": 1.0}}
	cases := []struct {
		name   string
		method string
		params map[string]any
	}{
		{"auth methods", AuthMethodsMethod, map[string]any{authFieldSessionID: string(fixture.session.id), "_meta": meta}},
		{"auth authorize", AuthAuthorizeMethod, map[string]any{
			authFieldSessionID:          string(fixture.session.id),
			authFieldProviderID:         authProviderID,
			authFieldConnectionID:       "lifecycle-refusal-new",
			authFieldMethodsGeneration:  baselineGeneration,
			authFieldMethod:             authMethodAPIKey,
			authFieldAuthorizeRequestID: "lifecycle-refusal-new",
			"_meta":                     meta,
		}},
		{"auth callback", AuthCallbackMethod, map[string]any{
			authFieldSessionID:  string(fixture.session.id),
			authFieldProviderID: authProviderID,
			authFieldMethod:     authMethodAPIKey,
			authFieldFlowID:     flow.FlowID,
			authFieldInput:      manualAmpKeyCanary,
			"_meta":             meta,
		}},
		{"auth status", AuthStatusMethod, map[string]any{
			authFieldSessionID:  string(fixture.session.id),
			authFieldProviderID: authProviderID,
			authFieldFlowID:     flow.FlowID,
			"_meta":             meta,
		}},
		{"auth cancel", AuthCancelMethod, map[string]any{
			authFieldSessionID:  string(fixture.session.id),
			authFieldProviderID: authProviderID,
			authFieldFlowID:     flow.FlowID,
			"_meta":             meta,
		}},
		{"auth inventory", AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id), "_meta": meta}},
		{"auth credential", AuthCredentialMethod, map[string]any{
			authFieldSessionID:  string(fixture.session.id),
			authFieldProviderID: authProviderID,
			authFieldFlowID:     flow.FlowID,
			"_meta":             meta,
		}},
		{"auth disconnect", AuthDisconnectMethod, map[string]any{
			authFieldSessionID:         string(fixture.session.id),
			authFieldProviderID:        authProviderID,
			authFieldConnectionID:      record.ConnectionID,
			authFieldBindingGeneration: record.BindingGeneration,
			"_meta":                    meta,
		}},
		{"fork", ForkSessionMethod, map[string]any{
			jsonFieldSessionID: string(fixture.session.id),
			"cwd":              fixture.session.cwd,
			"_meta":            meta,
		}},
	}

	authMethods := make([]string, 0, len(cases)-1)
	for _, testCase := range cases[:len(cases)-1] {
		authMethods = append(authMethods, testCase.method)
	}
	require.Equal(t, authMethodNames(), authMethods)

	connection := &localAgentConnection{agent: fixture.agent}
	connection.initialized.Store(true)

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			raw, err := json.Marshal(testCase.params)
			require.NoError(t, err)

			_, requestErr := connection.handle(t.Context(), testCase.method, raw)
			require.NotNil(t, requestErr)
			require.Equal(t, invalidParamsCode, requestErr.Code)
			data, ok := requestErr.Data.(map[string]any)
			require.True(t, ok)
			require.Equal(t, lifecycle.MetaPath, data[jsonFieldField])

			fixture.broker.mu.Lock()
			generation := fixture.broker.generation
			currentFlow := fixture.broker.byID[flow.FlowID]
			flowState := currentFlow.state
			flowReason := currentFlow.reason
			credential := append([]byte(nil), currentFlow.credential...)
			flows := len(fixture.broker.flows)
			byID := len(fixture.broker.byID)
			retained := len(fixture.broker.retained)
			fixture.broker.mu.Unlock()

			require.Equal(t, baselineGeneration, generation)
			require.Same(t, baselineFlow, currentFlow)
			require.Equal(t, baselineFlowState, flowState)
			require.Equal(t, baselineFlowReason, flowReason)
			require.Equal(t, baselineCredential, credential)
			require.Equal(t, baselineFlows, flows)
			require.Equal(t, baselineByID, byID)
			require.Equal(t, baselineRetained, retained)

			currentRecord, present, err := fixture.broker.ledger.read(authProviderID, record.ConnectionID)
			require.NoError(t, err)
			require.True(t, present)
			require.Equal(t, record, currentRecord)

			fixture.agent.mu.Lock()
			sessions := len(fixture.agent.sessions)
			fixture.agent.mu.Unlock()
			require.Equal(t, baselineSessions, sessions)
		})
	}
}

// TestLifecycleKeyRefusedOnTheUnconfiguredAuthSurface pins the refusal on the
// provider-auth surface nobody configured. Without a usable ledger root every
// `_amp/auth/*` leg answers method-not-found, and "the method does not exist" is
// not the answer a family literal is owed: the key is refused by name first, on
// the same field path the configured surface refuses it on, and the refusal is
// the whole effect.
func TestLifecycleKeyRefusedOnTheUnconfiguredAuthSurface(t *testing.T) {
	bare := newTestAgent()
	require.Nil(t, bare.providerAuth, "this table is the surface nobody configured")

	connection := &localAgentConnection{agent: bare}
	connection.initialized.Store(true)

	legs := authMethodNames()
	require.Len(t, legs, 8, "every leg of the advertised surface is in this table")

	stamped, err := json.Marshal(map[string]any{
		"_meta": map[string]any{lifecycle.MetaKey: map[string]any{"version": 1.0}},
	})
	require.NoError(t, err)

	for _, method := range legs {
		t.Run(method, func(t *testing.T) {
			// The leg's own answer, so the ordering below is a real precedence and
			// not a method that happened to refuse everything.
			result, absent := connection.handle(t.Context(), method, json.RawMessage(`{}`))
			require.Nil(t, result)
			require.NotNil(t, absent)
			require.Equal(t, methodNotFoundCode, absent.Code)

			result, refusal := connection.handle(t.Context(), method, stamped)
			require.Nil(t, result)
			require.NotNil(t, refusal)
			require.Equal(t, invalidParamsCode, refusal.Code)

			data, ok := refusal.Data.(map[string]any)
			require.True(t, ok)
			require.Equal(t, valUnsupported, data[jsonFieldError])
			require.Equal(t, lifecycle.MetaPath, data[jsonFieldField])
		})
	}

	// No leg ran, so nothing exists that did not exist before: no broker appeared,
	// no session opened, and the surface is still unadvertised.
	require.Nil(t, bare.providerAuth)

	bare.mu.Lock()
	sessions := len(bare.sessions)
	bare.mu.Unlock()
	require.Empty(t, sessions)

	resp, err := bare.Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)

	ampMeta, _ := resp.AgentCapabilities.Meta[ampMetaKey].(map[string]any)
	require.NotContains(t, ampMeta, providerAuthCapabilityKey)
}

// TestLifecycleKeyFailsACancelClosed pins the cancel rule: the key fails the
// cancel before native interrupt, the cancel is never applied, and because a
// notification carries no response frame the refusal is wire-silent.
func TestLifecycleKeyFailsACancelClosed(t *testing.T) {
	agent, _, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	err := agent.Cancel(t.Context(), acp.CancelNotification{
		SessionId: sessionID,
		Meta:      map[string]any{lifecycle.MetaKey: map[string]any{"version": 1.0}},
	})

	var requestErr *acp.RequestError

	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, invalidParamsCode, requestErr.Code)
	require.NoError(t, agent.Cancel(t.Context(), acp.CancelNotification{SessionId: sessionID}))
}

// orderingStore records the moment a durable commit lands in the same ledger the
// containment release and the emitted stream write to, so the settlement order is
// observed rather than asserted. Each commit is recorded with the number of
// transcript frames the generation published, which is what distinguishes the
// adoption generation from the turn's own.
type orderingStore struct {
	SessionStore
	record func(string)
}

func (s orderingStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	frames := 0

	for _, replacement := range replacements {
		if replacement.Key.Subpath == transcriptSubpath {
			frames = len(replacement.Entries)
		}
	}

	s.record("commit:" + strconv.Itoa(frames))

	return s.SessionStore.Replace(ctx, key, replacements)
}

// orderingClient records the terminal idle and the quiescence fact into the same
// ledger.
type orderingClient struct {
	lifecycleClient
	record      func(string)
	idleStarted chan<- struct{}
	idleRelease <-chan struct{}
	idleErr     error
}

func (c *orderingClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if envelope, ok := notification.Meta[lifecycle.MetaKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok {
			if event["type"] == "state_update" && event["state"] == "idle" {
				c.record("idle")
				if c.idleErr != nil {
					return c.idleErr
				}
			}
		}
	}

	return c.lifecycleClient.SessionUpdate(ctx, notification)
}

// TestLifecycleSettlementOrder pins the close-fenced order on the prompt path:
// the native terminal is past, then the containment and vacancy proof completes,
// then the durable commit, then the terminal idle, and only then the v1 response.
// The commit used to run before the boundary.
func TestLifecycleSettlementOrder(t *testing.T) {
	t.Setenv("AMP_API_KEY", "conformance-key")

	var (
		mu     sync.Mutex
		ledger []string
	)

	record := func(step string) {
		mu.Lock()
		defer mu.Unlock()

		ledger = append(ledger, step)
	}

	agent := NewAgent(testContainmentOptions([]Option{
		WithExecutablePath(lifecycleHarness(t)),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(orderingStore{SessionStore: NewInMemorySessionStore(), record: record}),
	})...)
	t.Cleanup(func() { require.NoError(t, agent.Close()) })

	client := &orderingClient{record: record}
	agent.setConnection(client)

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)

	session, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	mu.Lock()
	ledger = nil
	mu.Unlock()

	_, err = agent.Prompt(t.Context(), lifecyclePrompt(session.SessionId, "hello", "sub-1", "nonce-1"))
	require.NoError(t, err)
	record("response")

	mu.Lock()
	defer mu.Unlock()
	// The first commit is the adoption generation: it records the thread amp just
	// minted so a mid-turn death can never leave a created server-side thread
	// unrecorded, and it publishes the state already committed — no frame from the
	// turn in flight. The turn own commit is the one the boundary precedes.
	require.Equal(t, []string{"commit:0", "commit:3", "idle", "response"}, ledger)
}

// TestLifecycleCancelPersistsWhatStreamed pins that a cancelled turn commits the
// frames it streamed before the fence and settles its cycle as cancelled: durable
// state never diverges from what the client was already shown.
func TestLifecycleCancelPersistsWhatStreamed(t *testing.T) {
	agent, client, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	streamed := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }

	done := make(chan correctionResult[acp.PromptResponse], 1)

	go func() {
		resp, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hang", "sub-1", "nonce-1"))
		done <- correctionResult[acp.PromptResponse]{value: resp, err: err}
	}()

	<-streamed
	require.NoError(t, agent.Cancel(t.Context(), acp.CancelNotification{SessionId: sessionID}))

	cancelled := receiveCorrection(t, done, "cancelled prompt result")
	require.NoError(t, cancelled.err)
	require.Equal(t, acp.StopReasonCancelled, cancelled.value.StopReason)

	stored, err := agent.store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, stored, "a cancelled turn persists what streamed before the fence")

	state := reduceEmittedStream(t, client, negotiatedAnswer()).State()
	require.Len(t, state.Turns, 1)
	require.True(t, state.Turns[0].Terminal)
	require.Equal(t, lifecycle.OutcomeCancelled, state.Turns[0].Outcome)
}

// TestPromptFailsWhenTheIncarnationCannotOpen pins that a prompt whose stream
// cannot be identified never reaches the harness.
func TestPromptFailsWhenTheIncarnationCannotOpen(t *testing.T) {
	agent, _, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	original := randRead
	randRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }

	t.Cleanup(func() { randRead = original })

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-1", "nonce-1"))
	require.ErrorContains(t, err, "no entropy")
}

// TestPromptSettlesATurnWhoseAcceptanceCouldNotBePublished pins that once the
// native dispatcher owns the frame the turn exists and is settled, even when the
// acceptance itself could not be delivered.
func TestPromptSettlesATurnWhoseAcceptanceCouldNotBePublished(t *testing.T) {
	client := &failingLifecycleClient{failAt: 2}
	agent, sessionID := lifecycleAgentWithClient(t, lifecycleOffer(1.0), client)

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-1", "nonce-1"))
	require.ErrorContains(t, err, "transport refused")

	stored, storeErr := agent.store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, storeErr)
	require.Empty(t, stored, "an unaccepted turn streamed nothing to commit")
}

// TestPromptRetainsAnIncompleteContainmentBoundary pins that a boundary that did
// not complete fails the prompt and commits nothing: the durable commit is a
// step the boundary precedes.
func TestPromptRetainsAnIncompleteContainmentBoundary(t *testing.T) {
	t.Setenv("AMP_API_KEY", "conformance-key")

	agent := NewAgent(testContainmentOptions([]Option{
		WithExecutablePath(lifecycleHarness(t)),
		WithScratchDir(testScratchDir(t)),
	})...)
	// A session whose boundary never completed is never released, so this agent
	// reports the incomplete boundary for the rest of its life.
	t.Cleanup(func() { require.ErrorIs(t, agent.Close(), nativeamp.ErrContainmentIncomplete) })

	client := &lifecycleClient{}
	agent.setConnection(client)
	agent.options.runtime.settleTurn = func(turn *nativeamp.Turn) error {
		_ = turn.Close()

		return nativeamp.ErrContainmentIncomplete
	}

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)

	session, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	_, err = agent.Prompt(t.Context(), lifecyclePrompt(session.SessionId, "hello", "sub-1", "nonce-1"))
	require.ErrorIs(t, err, nativeamp.ErrContainmentIncomplete)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update"}, client.eventTypes(t),
		"no turn settles behind an unproven boundary")
}

// idleRefusingClient refuses exactly the terminal idle, so the settlement step
// after the durable commit can be driven deterministically.
type idleRefusingClient struct {
	lifecycleClient
	refuse bool
}

func (c *idleRefusingClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if envelope, ok := notification.Meta[lifecycle.MetaKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok && event["state"] == "idle" && c.refuse {
			return errors.New("transport refused the terminal idle")
		}
	}

	return c.lifecycleClient.SessionUpdate(ctx, notification)
}

// TestPromptFailsWhenTheTurnCannotSettle pins that a turn whose terminal
// transition cannot be published fails the prompt rather than returning a
// response the host's projection would never reach.
func TestPromptFailsWhenTheTurnCannotSettle(t *testing.T) {
	client := &idleRefusingClient{refuse: true}
	agent, sessionID := lifecycleAgentWithClient(t, lifecycleOffer(1.0), client)

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-1", "nonce-1"))
	require.ErrorContains(t, err, "transport refused the terminal idle")

	stored, storeErr := agent.store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, storeErr)
	require.NotEmpty(t, stored, "the commit precedes the terminal idle")

	// Agent.Close is the final retry owner. Heal the injected transport outage so
	// the helper's shutdown proves it retransmits the retained terminal state.
	client.refuse = false
}

// cancellingIdleClient cancels the request context from inside the terminal
// idle's own delivery, which is the one deterministic point where a cancel and a
// settled turn overlap: the boundary is proven, the commit is durable, the
// lifecycle turn has just recorded how it ended, and the v1 response has not
// been written yet.
type cancellingIdleClient struct {
	lifecycleClient
	cancel func()
}

func (c *cancellingIdleClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if envelope, ok := notification.Meta[lifecycle.MetaKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok && event["outcome"] == "failed" {
			c.cancel()
		}
	}

	return c.lifecycleClient.SessionUpdate(ctx, notification)
}

// TestSettledFailureIsNeverRewrittenAsCancelled pins that a turn's recorded
// terminal identity is the one its v1 response states. A cancel landing while a
// turn is already failing does not convert that failure into a clean cancel: the
// lifecycle idle the host was just shown recorded `failed`, and no ACP v1 stop
// reason names a failure.
func TestSettledFailureIsNeverRewrittenAsCancelled(t *testing.T) {
	promptCtx, cancelPrompt := context.WithCancel(t.Context())
	defer cancelPrompt()

	client := &cancellingIdleClient{cancel: cancelPrompt}
	agent, sessionID := lifecycleAgentWithClient(t, lifecycleOffer(1.0), client)

	resp, err := agent.Prompt(promptCtx, lifecyclePrompt(sessionID, "provider-failure", "sub-1", "nonce-1"))
	require.Error(t, err, "a settled native failure answers with the failure it recorded")
	require.NotEqual(t, acp.StopReasonCancelled, resp.StopReason)

	var requestErr *acp.RequestError

	require.ErrorAs(t, err, &requestErr)

	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, turnFailedError, data[jsonFieldError])

	idle, ok := client.envelopes(t)[3]["event"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "failed", idle["outcome"])
	require.NotContains(t, idle, "stopReason")

	state := reduceEmittedStream(t, &client.lifecycleClient, negotiatedAnswer()).State()
	require.Equal(t, lifecycle.OutcomeFailed, state.Turns[0].Outcome)
}

// fencedDeleteStore fails the tombstone write, and runs a hook at the exact
// moment the delete fence is up: the session has stopped writing, and the
// tombstone that would have justified dropping what it holds never lands.
type fencedDeleteStore struct {
	SessionStore
	fenced func()
}

func (s *fencedDeleteStore) Delete(ctx context.Context, _ SessionKey) error {
	if s.fenced != nil {
		hook := s.fenced
		s.fenced = nil
		hook()
	}

	return errors.New("tombstone write refused")
}

// TestFailedDeleteRetainsTheSettlingTurnsFrames pins that a delete whose
// tombstone never lands hands the host back a live session whose mirror still
// says what it holds. The fence stops the commit; it does not authorize
// forgetting the frames the client was already shown, because the row those
// frames belong to is still there.
func TestFailedDeleteRetainsTheSettlingTurnsFrames(t *testing.T) {
	t.Setenv("AMP_API_KEY", "conformance-key")

	deleteEntered := make(chan struct{})
	deleteRelease := make(chan struct{})
	store := &fencedDeleteStore{
		SessionStore: NewInMemorySessionStore(),
	}
	agent := NewAgent(testContainmentOptions([]Option{
		WithExecutablePath(lifecycleHarness(t)),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
	})...)
	agent.options.runtime.beforeDeleteTombstone = func() {
		close(deleteEntered)
		awaitCorrectionCallback(t, deleteRelease, "failed-delete tombstone release")
	}
	t.Cleanup(func() { require.NoError(t, agent.Close()) })

	client := &lifecycleClient{}
	agent.setConnection(client)

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)

	session, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	streamed := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }

	promptResult := make(chan error, 1)
	go func() {
		_, promptErr := agent.Prompt(t.Context(), lifecyclePrompt(session.SessionId, "hang", "sub-1", "nonce-1"))
		promptResult <- promptErr
	}()

	awaitCorrectionSignal(t, streamed, "failed-delete prompt chunk")

	deleteResult := make(chan error, 1)
	go func() {
		_, deleteErr := agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
		deleteResult <- deleteErr
	}()

	// The external caller, not the store callback, settles the turn inside the
	// published delete fence. The callback remains a pure barrier and cannot
	// synchronously re-enter its own operation.
	select {
	case <-deleteEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("delete did not reach the fenced tombstone write")
	}
	require.NoError(t, agent.Cancel(t.Context(), acp.CancelNotification{SessionId: session.SessionId}))
	require.ErrorIs(t, receiveCorrection(t, promptResult, "fenced prompt result"), errPersistenceFenced)
	close(deleteRelease)
	require.ErrorContains(t, receiveCorrection(t, deleteResult, "failed tombstone result"), "tombstone write refused")

	stored, err := agent.store.Load(t.Context(), SessionKey{SessionID: string(session.SessionId), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.Empty(t, stored, "the fence forbids the write")

	live, err := agent.session(session.SessionId)
	require.NoError(t, err)

	live.mu.Lock()
	retained := len(live.unsyncedFrames)
	live.mu.Unlock()
	require.NotZero(t, retained, "the frames the client was shown are retained, not dropped")

	// The mirror is unsynced, and the session the host still owns re-commits
	// exactly those frames rather than reporting itself clean over a gap.
	require.NoError(t, live.ensureMirrorSynced(t.Context()))

	stored, err = agent.store.Load(t.Context(), SessionKey{SessionID: string(session.SessionId), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.Len(t, stored, retained)
}

// TestPromptAdmissionIsTheClosureLinearization pins the window a prompt and a
// teardown race in. A prompt that already passed its own readiness gate is
// admitted under the same lock a close or delete fences one with, so a teardown
// that observed an empty slot is a teardown no later prompt slips past.
func TestPromptAdmissionIsTheClosureLinearization(t *testing.T) {
	t.Setenv("AMP_API_KEY", "conformance-key")

	agent := newTestAgent(WithExecutablePath(lifecycleHarness(t)), WithScratchDir(testScratchDir(t)))
	t.Cleanup(func() { require.NoError(t, agent.Close()) })

	closing, err := newAgentSession(t.Context(), agent, "T-closing", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)

	// The prompt's own gate passes first, exactly as it does in flight.
	require.NoError(t, closing.ready())
	// The close then runs whole: it marks the session closed and observes an
	// empty prompt slot, so it waits for nothing.
	require.NoError(t, closing.Close(t.Context()))
	require.ErrorIs(t, closing.admitPrompt(newPromptTurnState()), errSessionClosed)

	deleting, err := newAgentSession(t.Context(), agent, "T-deleting", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, deleting.Close(t.Context())) })

	require.NoError(t, deleting.ready())
	// A session fenced for delete keeps nothing it writes, so a turn admitted
	// after the fence would stream frames its own commit is forbidden to take.
	deleting.fencePersistence()
	require.ErrorIs(t, deleting.admitPrompt(newPromptTurnState()), errSessionClosed)

	deleting.resumePersistence()

	admitted := newPromptTurnState()
	require.NoError(t, deleting.admitPrompt(admitted))
	// A prompt the session admitted is one its teardown waits out, so the
	// fixture settles it exactly as a real prompt does.
	admitted.complete(nil)
	deleting.clearActivePrompt(admitted)

	// The whole prompt path refuses the same way. Readiness alone does not see a
	// delete fence, so admission is the gate that keeps a fenced session from
	// launching native work whose frames its own commit may not take.
	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	live, err := agent.session(created.SessionId)
	require.NoError(t, err)

	live.fencePersistence()
	require.NoError(t, live.ready())

	_, err = agent.Prompt(t.Context(), TextPromptRequest(created.SessionId, "fenced-turn", "hello"))
	require.ErrorIs(t, err, errSessionClosed)
}

// TestIncompleteLaunchPublishesItsBoundaryOnTheLatch pins that a launch which
// started a process and could not deliver its input reports an unsettled prompt.
// The completion latch is what a close or delete waits on, so an incomplete
// boundary published there as nil would let a teardown call the session settled
// and remove the scratch state a surviving tree still runs against.
func TestIncompleteLaunchPublishesItsBoundaryOnTheLatch(t *testing.T) {
	t.Setenv("AMP_API_KEY", "conformance-key")

	agent := NewAgent(testContainmentOptions([]Option{
		WithExecutablePath(lifecycleHarness(t)),
		WithScratchDir(testScratchDir(t)),
	})...)
	// The boundary never completed, so this agent reports it for the rest of its
	// life and never releases the session's scratch.
	t.Cleanup(func() { require.ErrorIs(t, agent.Close(), nativeamp.ErrContainmentIncomplete) })

	client := &lifecycleClient{}
	agent.setConnection(client)

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	session, err := agent.session(created.SessionId)
	require.NoError(t, err)

	var latch *promptTurnState

	agent.options.runtime.executeThread = func(context.Context, *nativeamp.Client, any) (*nativeamp.Turn, error) {
		// The prompt is already published here, which is what a concurrent close
		// or delete would find and wait on.
		latch = session.activePromptState()

		return nil, fmt.Errorf("%w: input delivery refused", nativeamp.ErrContainmentIncomplete)
	}

	_, err = agent.Prompt(t.Context(), lifecyclePrompt(created.SessionId, "hello", "sub-1", "nonce-1"))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.Equal(t, []string{"lifecycle_snapshot"}, client.eventTypes(t), "nothing was accepted")

	require.NotNil(t, latch, "the prompt is admitted before it launches native work")
	require.ErrorIs(t, latch.awaitCompletion(t.Context()), nativeamp.ErrContainmentIncomplete,
		"the latch carries the boundary this launch lost")

	// The teardown every close and delete performs refuses to reclaim scratch
	// state behind an unproven boundary.
	require.ErrorIs(t, session.Close(t.Context()), nativeamp.ErrContainmentIncomplete)
	require.DirExists(t, session.settingsDir)
}

// TestCloseEmitsNothingOnAFencedOrNeverOpenedIncarnation pins the close ladder's
// emission rungs against the only incarnations this configuration ever has. A
// prompt-contained source opens one incarnation per prompt and fences it when the
// contained process exits, so a close always meets a dead or never-opened stream:
// it emits nothing on it, and the terminal state was already reported by whatever
// fenced it. The non-emission rungs — the containment proof and the durable
// commit — ran inside the prompt and stand.
func TestCloseEmitsNothingOnAFencedOrNeverOpenedIncarnation(t *testing.T) {
	t.Setenv("AMP_API_KEY", "conformance-key")

	client := &lifecycleClient{}
	agent, neverPrompted := lifecycleAgentWithClient(t, lifecycleOffer(1.0), client)

	_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: neverPrompted})
	require.NoError(t, err, "a session whose incarnation never opened still closes")
	require.Empty(t, client.eventTypes(t), "a never-opened incarnation has no stream to emit on")

	prompted, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	streamed := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }

	done := make(chan correctionResult[acp.PromptResponse], 1)

	go func() {
		resp, promptErr := agent.Prompt(t.Context(), lifecyclePrompt(prompted.SessionId, "hang", "sub-1", "nonce-1"))
		done <- correctionResult[acp.PromptResponse]{value: resp, err: promptErr}
	}()

	<-streamed
	require.NoError(t, agent.Cancel(t.Context(), acp.CancelNotification{SessionId: prompted.SessionId}))

	cancelled := receiveCorrection(t, done, "cancelled prompt result")
	require.NoError(t, cancelled.err)
	require.Equal(t, acp.StopReasonCancelled, cancelled.value.StopReason)

	fenced := client.eventTypes(t)
	require.Equal(t,
		[]string{"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update"},
		fenced, "the cancelled turn settled on its own stream")

	stored, err := agent.store.Load(t.Context(), SessionKey{
		SessionID: string(prompted.SessionId), Subpath: transcriptSubpath,
	})
	require.NoError(t, err)
	require.NotEmpty(t, stored, "the durable commit landed before the stream was fenced")

	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: prompted.SessionId})
	require.NoError(t, err, "a cancel-then-close succeeds")
	require.Equal(t, fenced, client.eventTypes(t), "the close emitted nothing on the fenced stream")

	after, err := agent.store.Load(t.Context(), SessionKey{
		SessionID: string(prompted.SessionId), Subpath: transcriptSubpath,
	})
	require.NoError(t, err)
	require.Equal(t, stored, after, "the close rewrote nothing the prompt already committed")
}

// TestCloseNeverRewritesALossTerminalizedFailure pins what a close does to a turn
// the incarnation's own loss already terminalized as `failed`. The reduction runs
// first and is the test's premise, not its finding: it establishes that the
// settled stream really does record `failed`, so the two equalities that follow
// the close are the assertions with content.
//
// Both are load-bearing because this close carries a durable rung. The close
// emits nothing on the fenced stream, and its commit of whatever the settlement
// retained writes nothing over the frames the failure already committed: neither
// the stream nor the store restates the ending.
//
// This adapter holds no durable lifecycle entity state — every entity lives in
// the prompt-contained incarnation and dies with it, and the store holds Amp's
// own stream-json frames only — so the durable clause of the no-rewrite rule is
// enforced here by construction. The store equality is what stands in for it: the
// only durable record a close could rewrite is the transcript, and it does not.
func TestCloseNeverRewritesALossTerminalizedFailure(t *testing.T) {
	client := &lifecycleClient{}
	agent, sessionID := lifecycleAgentWithClient(t, lifecycleOffer(1.0), client)

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "provider-failure", "sub-1", "nonce-1"))
	require.Error(t, err)

	settled := client.eventTypes(t)

	state := reduceEmittedStream(t, client, negotiatedAnswer()).State()
	require.Len(t, state.Turns, 1)
	require.Equal(t, lifecycle.OutcomeFailed, state.Turns[0].Outcome,
		"the incarnation's own loss terminalized the turn as failed")

	stored, err := agent.store.Load(t.Context(), SessionKey{
		SessionID: string(sessionID), Subpath: transcriptSubpath,
	})
	require.NoError(t, err)
	require.NotEmpty(t, stored)

	// The settlement's own commit landed, so the close's durable rung has nothing
	// of its own to write: the store equality below is earned rather than lucky.
	live, lookupErr := agent.session(sessionID)
	require.NoError(t, lookupErr)
	live.mu.Lock()
	retained := len(live.unsyncedFrames)
	live.mu.Unlock()
	require.Zero(t, retained, "a settlement that committed retains nothing for the close to commit")

	require.NoError(t, agent.Cancel(t.Context(), acp.CancelNotification{SessionId: sessionID}))

	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)
	require.Equal(t, settled, client.eventTypes(t), "the close emitted nothing on the fenced stream")

	after, err := agent.store.Load(t.Context(), SessionKey{
		SessionID: string(sessionID), Subpath: transcriptSubpath,
	})
	require.NoError(t, err)
	require.Equal(t, stored, after, "the close's own commit rewrote no frame the failure committed")
}

// TestLifecycleKeyRefusedBeforeExtensionMethodResolution pins the precedence on
// the extension surface: the reserved key is answered before the method name is
// resolved, so a method this adapter does not dispatch reports the key rather
// than its own absence. The same names without the key still answer
// method-not-found, so the refusal is a real precedence and not a door that
// refuses everything, and a method the adapter does dispatch reports the key on
// the key's field path rather than on its own.
func TestLifecycleKeyRefusedBeforeExtensionMethodResolution(t *testing.T) {
	agent := newTestAgent()
	require.Nil(t, agent.providerAuth, "no configured extension leg answers ahead of the key")

	connection := &localAgentConnection{agent: agent}
	connection.initialized.Store(true)

	stamped, err := json.Marshal(map[string]any{
		"_meta": map[string]any{lifecycle.MetaKey: map[string]any{"version": 1.0}},
	})
	require.NoError(t, err)

	for _, method := range []string{"_amp/unknown", "_amp/auth/nonexistent", "_other/vendor/method"} {
		t.Run(method, func(t *testing.T) {
			result, absent := connection.handle(t.Context(), method, json.RawMessage(`{}`))
			require.Nil(t, result)
			require.NotNil(t, absent)
			require.Equal(t, methodNotFoundCode, absent.Code)
			require.Equal(t, map[string]any{jsonFieldMethod: method}, absent.Data)

			result, refusal := connection.handle(t.Context(), method, stamped)
			require.Nil(t, result)
			require.NotNil(t, refusal)
			require.Equal(t, invalidParamsCode, refusal.Code)

			data, ok := refusal.Data.(map[string]any)
			require.True(t, ok)
			require.Equal(t, valUnsupported, data[jsonFieldError])
			require.Equal(t, lifecycle.MetaPath, data[jsonFieldField])
		})
	}

	// The dispatched method's own refusal names the fork surface; stamped, the
	// same call names the key instead, which is what "before resolution" means.
	_, unsupported := connection.handle(t.Context(), ForkSessionMethod, json.RawMessage(`{}`))
	require.NotNil(t, unsupported)
	require.Equal(t, invalidParamsCode, unsupported.Code)
	require.Equal(t, map[string]any{
		jsonFieldError: valUnsupported,
		jsonFieldField: ForkSessionMethod,
	}, unsupported.Data)

	_, refusal := connection.handle(t.Context(), ForkSessionMethod, stamped)
	require.NotNil(t, refusal)
	require.Equal(t, invalidParamsCode, refusal.Code)
	require.Equal(t, map[string]any{
		jsonFieldError: valUnsupported,
		jsonFieldField: lifecycle.MetaPath,
	}, refusal.Data)

	// The refusal is the whole effect: no session opened and no broker appeared
	// behind a method name that never resolved.
	agent.mu.Lock()
	sessions := len(agent.sessions)
	agent.mu.Unlock()
	require.Empty(t, sessions)
	require.Nil(t, agent.providerAuth)
}

func TestSetConfigOptionUseAdmissionResidualBranch(t *testing.T) {
	agent := NewAgent()
	id := acp.SessionId("T-config")
	use := &agentSessionUse{done: make(chan struct{})}
	agent.sessionUses[id] = use
	_, err := agent.SetSessionConfigOption(residualCancelledContext(), acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{
		SessionId: id,
		ConfigId:  "mode",
		Value:     "medium",
	}})
	require.Error(t, err)
}
