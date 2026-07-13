package ampacp

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/coder/acp-go-sdk"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// recordedRawEvent is the decoded shape of a `_amp/rawEvent` notification.
type recordedRawEvent struct {
	SessionId string          `json:"sessionId"`
	Sequence  int64           `json:"sequence"`
	Source    string          `json:"source"`
	Event     json.RawMessage `json:"event"`
}

func decodeRawEvents(t *testing.T, raw []json.RawMessage) []recordedRawEvent {
	t.Helper()
	out := make([]recordedRawEvent, 0, len(raw))
	for _, entry := range raw {
		var event recordedRawEvent
		if err := json.Unmarshal(entry, &event); err != nil {
			t.Fatalf("decode raw event %s: %v", entry, err)
		}
		out = append(out, event)
	}

	return out
}

func sumInt64Metric(metrics metricdata.ResourceMetrics, name string) int64 {
	var sum int64
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			data, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range data.DataPoints {
				sum += point.Value
			}
		}
	}

	return sum
}

// oversizedMessage marshals to a raw-event payload well over the 64 KiB cap.
func oversizedMessage() fakeAmpMessage {
	return fakeAmpMessage{raw: map[string]any{"type": "assistant", "blob": strings.Repeat("y", rawEventMaxBytes+4096)}}
}

// TestRawEventOversizeEmitsMarker pins raw-event spec case 1: an oversized event
// is never dropped; it consumes its sequence and emits the fixed oversize marker
// with the envelope intact.
func TestRawEventOversizeEmitsMarker(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	session := &agentSession{agent: agent, id: "T-oversize", rawEvents: true}

	if err := session.emitRawEvent(ctx, "stream-json", oversizedMessage()); err != nil {
		t.Fatalf("emit oversize: %v", err)
	}
	waitForRecorded(t, func() bool { return len(client.rawSnapshot()) == 1 })

	events := decodeRawEvents(t, client.rawSnapshot())
	if len(events) != 1 {
		t.Fatalf("raw events = %d, want exactly 1", len(events))
	}
	event := events[0]
	if event.SessionId != "T-oversize" || event.Sequence != 1 || event.Source != "stream-json" {
		t.Fatalf("envelope not intact: %#v", event)
	}
	var marker map[string]any
	if err := json.Unmarshal(event.Event, &marker); err != nil {
		t.Fatalf("decode marker: %v", err)
	}
	if marker["truncated"] != true || marker["reason"] != "oversize" || marker["maxBytes"] != float64(rawEventMaxBytes) {
		t.Fatalf("oversize marker = %#v", marker)
	}
	size, ok := marker["sizeBytes"].(float64)
	if !ok || int(size) <= rawEventMaxBytes {
		t.Fatalf("sizeBytes = %#v, want > %d", marker["sizeBytes"], rawEventMaxBytes)
	}
	if len(marker) != 4 {
		t.Fatalf("oversize marker has extra fields: %#v", marker)
	}
}

// TestRawEventContiguousPerSessionSequence pins case 2: a mix of normal and
// oversized events in one session yields a contiguous 1..N sequence with no gaps.
func TestRawEventContiguousPerSessionSequence(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	session := &agentSession{agent: agent, id: "T-seq", rawEvents: true}

	messages := []fakeAmpMessage{
		{raw: map[string]any{"type": "system"}},
		oversizedMessage(),
		{raw: map[string]any{"type": "assistant"}},
		{raw: map[string]any{"type": "result"}},
	}
	for _, message := range messages {
		if err := session.emitRawEvent(ctx, "stream-json", message); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}
	waitForRecorded(t, func() bool { return len(client.rawSnapshot()) == len(messages) })

	events := decodeRawEvents(t, client.rawSnapshot())
	for i, event := range events {
		if event.Sequence != int64(i+1) {
			t.Fatalf("sequence[%d] = %d, want %d (contiguous)", i, event.Sequence, i+1)
		}
	}
}

// TestRawEventCrossSessionIsolation pins case 3: two concurrent sessions each
// draw an independent sequence stream starting at 1 (kills any agent-global
// counter).
func TestRawEventCrossSessionIsolation(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	sessionA := &agentSession{agent: agent, id: "T-A", rawEvents: true}
	sessionB := &agentSession{agent: agent, id: "T-B", rawEvents: true}

	const perSessionCount = 3
	var wg sync.WaitGroup
	for _, session := range []*agentSession{sessionA, sessionB} {
		wg.Add(1)
		go func(session *agentSession) {
			defer wg.Done()
			for i := 0; i < perSessionCount; i++ {
				if err := session.emitRawEvent(ctx, "stream-json", fakeAmpMessage{raw: map[string]any{"type": "x"}}); err != nil {
					t.Errorf("emit for %s: %v", session.id, err)
				}
			}
		}(session)
	}
	wg.Wait()
	waitForRecorded(t, func() bool { return len(client.rawSnapshot()) == 2*perSessionCount })

	perSession := make(map[string][]int64, 2)
	for _, event := range decodeRawEvents(t, client.rawSnapshot()) {
		perSession[event.SessionId] = append(perSession[event.SessionId], event.Sequence)
	}
	if len(perSession) != 2 {
		t.Fatalf("sessions seen = %d, want 2", len(perSession))
	}
	for id, sequences := range perSession {
		sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
		for i, sequence := range sequences {
			if sequence != int64(i+1) {
				t.Fatalf("session %s sequence = %v, want contiguous 1..%d", id, sequences, perSessionCount)
			}
		}
	}
}

// TestRawEventAlwaysValidJSON pins case 4: every emitted event field is valid
// JSON, including an over-limit event and one that fails to marshal.
func TestRawEventAlwaysValidJSON(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	session := &agentSession{agent: agent, id: "T-valid", rawEvents: true}

	messages := []fakeAmpMessage{
		{raw: map[string]any{"type": "normal", "n": 1}},
		oversizedMessage(),
		{raw: map[string]any{"type": "unserializable", "bad": func() {}}},
	}
	for _, message := range messages {
		if err := session.emitRawEvent(ctx, "stream-json", message); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}
	waitForRecorded(t, func() bool { return len(client.rawSnapshot()) == len(messages) })

	events := decodeRawEvents(t, client.rawSnapshot())
	if len(events) != len(messages) {
		t.Fatalf("events = %d, want %d", len(events), len(messages))
	}
	for i, event := range events {
		if !json.Valid(event.Event) {
			t.Fatalf("event[%d] not valid JSON: %s", i, event.Event)
		}
	}
	var marker map[string]any
	if err := json.Unmarshal(events[len(events)-1].Event, &marker); err != nil {
		t.Fatalf("decode unserializable marker: %v", err)
	}
	if marker["truncated"] != true || marker["reason"] != "unserializable" || marker["maxBytes"] != float64(rawEventMaxBytes) {
		t.Fatalf("unserializable marker = %#v", marker)
	}
	if _, ok := marker["sizeBytes"]; ok {
		t.Fatalf("unserializable marker must not carry sizeBytes: %#v", marker)
	}
}

// TestRawEventEmitFailureDoesNotFailTurn pins case 5: a raw-event delivery
// failure is recorded on the observer hook and the turn still succeeds. The
// system-then-result stream emits raw events but no authoritative session
// updates, so the closed connection fails only raw notifications.
func TestRawEventEmitFailureDoesNotFailTurn(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "system-then-result")
	reader := sdkmetric.NewManualReader()
	agent := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
	)
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionRawEvents(true)))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	agent.setConnection(newClosedAgentConnection(t))

	promptResp, err := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
	if err != nil {
		t.Fatalf("raw emit failure aborted the turn: %v", err)
	}
	if promptResp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("stop reason = %q, want end_turn", promptResp.StopReason)
	}

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	if got := sumInt64Metric(metrics, "acp_go_amp.raw_event.emit.failure.count"); got < 1 {
		t.Fatalf("raw emit failure count = %d, want >= 1 (recorded on observer)", got)
	}
}

// TestRawEventDefaultOff pins case 6: without raw events enabled, no
// `_amp/rawEvent` notification is emitted regardless of native event volume.
func TestRawEventDefaultOff(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	promptResp, err := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if promptResp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("stop reason = %q, want end_turn", promptResp.StopReason)
	}
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) > 0 })
	if got := len(client.rawSnapshot()); got != 0 {
		t.Fatalf("raw events emitted while default-off: %d", got)
	}
}
