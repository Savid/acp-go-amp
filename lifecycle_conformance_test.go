package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

// invalidParamsCode is the JSON-RPC code every refused reserved value carries.
const invalidParamsCode = -32602

// lifecycleOffer is the object a host stamps on initialize. It carries exactly
// one member, and the decoded wire form of a JSON number is a float64.
func lifecycleOffer(versions ...any) map[string]any {
	return map[string]any{lifecycle.MetaKey: map[string]any{"versions": versions}}
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
	return lifecycle.Negotiated{Versions: []int{1}, ActivityKinds: []lifecycle.ActivityKind{}}
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

	path := filepath.Join(dir, "amp")

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

// TestLifecycleAnswerIsPerConfiguration pins the advertisement truth table. The
// answer is resolved from the same code path that enforces containment, so a
// configuration that cannot prove whole-tree vacancy advertises neither
// authoritative quiescence nor a proof class.
func TestLifecycleAnswerIsPerConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mode        RuntimeContainmentMode
		quiescence  bool
		proofSource string
	}{
		{"authoritative", RuntimeContainmentAuthoritative, true, "process-containment"},
		{"shared identity", RuntimeContainmentSharedIdentity, false, ""},
		{"best effort", RuntimeContainmentBestEffort, false, ""},
		{"unavailable", RuntimeContainmentUnavailable, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agent := newTestAgent()
			agent.containmentMode = tc.mode

			resp, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
			require.NoError(t, err)

			answer, ok := resp.Meta[lifecycle.MetaKey].(map[string]any)
			require.True(t, ok, "the answer rides the response's own _meta")
			require.Equal(t, []int{1}, answer["versions"])
			require.Equal(t, false, answer["updatesOutsidePrompt"])
			require.Equal(t, []string{}, answer["activityKinds"])
			require.Equal(t, tc.quiescence, answer["authoritativeQuiescence"])

			if tc.proofSource == "" {
				require.NotContains(t, answer, "quiescenceSource")

				return
			}

			require.Equal(t, tc.proofSource, answer["quiescenceSource"])
		})
	}
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

// TestLifecycleUnsupportedOfferOmitsTheKey pins that an empty intersection omits
// the whole key rather than answering with an empty array.
func TestLifecycleUnsupportedOfferOmitsTheKey(t *testing.T) {
	resp, err := newTestAgent().Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(2.0, 3.0)})
	require.NoError(t, err)
	require.NotContains(t, resp.Meta, lifecycle.MetaKey)
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
			"versions": []any{1.0}, "updatesOutsidePrompt": true,
		}}, `_meta["acp-go.dev/lifecycle"].updatesOutsidePrompt`},
		{"missing versions", map[string]any{lifecycle.MetaKey: map[string]any{}}, `_meta["acp-go.dev/lifecycle"].versions`},
		{"empty versions", lifecycleOffer(), `_meta["acp-go.dev/lifecycle"].versions`},
		{"non-integer version", lifecycleOffer(1.5), `_meta["acp-go.dev/lifecycle"].versions`},
		{"non-numeric version", lifecycleOffer("1"), `_meta["acp-go.dev/lifecycle"].versions`},
		{"non-array versions", map[string]any{lifecycle.MetaKey: map[string]any{"versions": 1.0}}, `_meta["acp-go.dev/lifecycle"].versions`},
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

// TestLifecycleOpensOneIncarnationPerPrompt pins the shape a configuration
// answering updatesOutsidePrompt false owes: no channel exists between prompts,
// so each prompt is a distinct stream with its own snapshot and sequence space.
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
		"logout": func() error {
			_, err := agent.Logout(t.Context(), acp.LogoutRequest{Meta: key})

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
// observed rather than asserted.
type orderingStore struct {
	SessionStore
	record func(string)
}

func (s orderingStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	s.record("commit")

	return s.SessionStore.Replace(ctx, key, replacements)
}

// orderingClient records the terminal idle and the quiescence fact into the same
// ledger.
type orderingClient struct {
	lifecycleClient
	record func(string)
}

func (c *orderingClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if envelope, ok := notification.Meta[lifecycle.MetaKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok {
			if event["type"] == "state_update" && event["state"] == "idle" {
				c.record("idle")
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
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				return func() {
					if kind == RuntimeResourcePrompt {
						record("containment")
					}
				}, nil
			},
		}),
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
	// The first commit is the early manifest persist that records the thread amp
	// just minted, so a mid-turn death can never leave a created server-side
	// thread unrecorded. The turn own commit is the one the boundary precedes.
	require.Equal(t, []string{"commit", "containment", "commit", "idle", "response"}, ledger)
}

// TestLifecycleCancelPersistsWhatStreamed pins that a cancelled turn commits the
// frames it streamed before the fence and settles its cycle as cancelled: durable
// state never diverges from what the client was already shown.
func TestLifecycleCancelPersistsWhatStreamed(t *testing.T) {
	agent, client, sessionID := lifecycleAgent(t, lifecycleOffer(1.0))

	streamed := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }

	done := make(chan acp.PromptResponse, 1)

	go func() {
		resp, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hang", "sub-1", "nonce-1"))
		require.NoError(t, err)
		done <- resp
	}()

	<-streamed
	require.NoError(t, agent.Cancel(t.Context(), acp.CancelNotification{SessionId: sessionID}))
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)

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
	t.Cleanup(func() { require.ErrorIs(t, agent.Close(), nativeamp.ErrProcessContainmentIncomplete) })

	client := &lifecycleClient{}
	agent.setConnection(client)
	agent.options.runtime.settleTurn = func(turn *nativeamp.Turn) (nativeamp.ContainmentProof, error) {
		_ = turn.Close()

		return turn.ContainmentProof(), nativeamp.ErrProcessContainmentIncomplete
	}

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)

	session, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	_, err = agent.Prompt(t.Context(), lifecyclePrompt(session.SessionId, "hello", "sub-1", "nonce-1"))
	require.ErrorIs(t, err, nativeamp.ErrProcessContainmentIncomplete)
	require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update"}, client.eventTypes(t),
		"no turn settles behind an unproven boundary")
}

// idleRefusingClient refuses exactly the terminal idle, so the settlement step
// after the durable commit can be driven deterministically.
type idleRefusingClient struct {
	lifecycleClient
}

func (c *idleRefusingClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if envelope, ok := notification.Meta[lifecycle.MetaKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok && event["state"] == "idle" {
			return errors.New("transport refused the terminal idle")
		}
	}

	return c.lifecycleClient.SessionUpdate(ctx, notification)
}

// TestPromptFailsWhenTheTurnCannotSettle pins that a turn whose terminal
// transition cannot be published fails the prompt rather than returning a
// response the host's projection would never reach.
func TestPromptFailsWhenTheTurnCannotSettle(t *testing.T) {
	client := &idleRefusingClient{}
	agent, sessionID := lifecycleAgentWithClient(t, lifecycleOffer(1.0), client)

	_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-1", "nonce-1"))
	require.ErrorContains(t, err, "transport refused the terminal idle")

	stored, storeErr := agent.store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, storeErr)
	require.NotEmpty(t, stored, "the commit precedes the terminal idle")
}
