package amp

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/savid/acp-go-core/process"
)

//go:embed bridge.ts
var bridgeSource string

const (
	bridgeTimeout = 30 * time.Second
	stateIdle     = "idle"
	roleAssistant = "assistant"
	roleUser      = "user"
	roleInfo      = "info"
)

// Native terminal statuses are shared by lifecycle receipts and tool results.
const (
	StatusDone      = "done"
	StatusError     = "error"
	StatusCancelled = "cancelled"
)

// Message is Amp's plugin-facing message projection. Raw exports remain the backup.
type Message struct {
	ID      json.RawMessage  `json:"id"`
	Role    string           `json:"role"`
	Content []map[string]any `json:"content"`
}

// View is an observed native thread state and its full, uncompacted message list.
type View struct {
	ThreadID string    `json:"threadId"`
	State    string    `json:"state"`
	Messages []Message `json:"messages"`
}

// Receipt identifies the messages completed by a native agent.end event.
type Receipt struct {
	ID       json.RawMessage `json:"id"`
	Status   string          `json:"status"`
	Messages []Message       `json:"messages"`
}

// Outcome retains native evidence even when the stream or process fails.
type Outcome struct {
	View      *View
	Receipt   *Receipt
	Submitted bool
	Stopped   bool
}

type bridgeEvent struct {
	Type     string          `json:"type"`
	ThreadID string          `json:"threadId"`
	State    string          `json:"state"`
	Messages []Message       `json:"messages"`
	ID       json.RawMessage `json:"id"`
	Status   string          `json:"status"`
}

func (e bridgeEvent) view() View {
	return View{ThreadID: e.ThreadID, State: e.State, Messages: e.Messages}
}
func (e bridgeEvent) receipt() Receipt {
	return Receipt{ID: e.ID, Status: e.Status, Messages: e.Messages}
}

type bridge struct {
	directory string
	plugin    string
	events    *os.File
	pending   []byte
}

// installBridge preserves the inherited configuration and other native plugins.
// Unique entry files are removed only after their owning process has been reaped.
func installBridge(request *process.Request, scratch, thread string) (*bridge, error) {
	if scratch != "" {
		if err := os.MkdirAll(scratch, 0o700); err != nil {
			return nil, err
		}
	}

	directory, err := os.MkdirTemp(scratch, "acp-go-amp-")
	if err != nil {
		return nil, err
	}

	b := &bridge{directory: directory}

	complete := false
	defer func() {
		if !complete {
			b.close()
		}
	}()

	directory, err = filepath.Abs(directory)
	if err != nil {
		return nil, err
	}

	b.directory = directory

	b.events, err = os.OpenFile(filepath.Join(directory, "events"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}

	root, _ := process.Lookup(request.Env, "XDG_CONFIG_HOME")
	if root == "" {
		home, _ := process.Lookup(request.Env, "HOME")
		if home == "" {
			return nil, errors.New("amp plugin config root is missing")
		}

		root = filepath.Join(home, ".config")
	}

	if !filepath.IsAbs(root) {
		root = filepath.Join(request.Dir, root)
	}

	root = filepath.Join(root, "amp", "plugins")
	if mkdirErr := os.MkdirAll(root, 0o700); mkdirErr != nil {
		return nil, mkdirErr
	}

	file, err := os.CreateTemp(root, ".acp-go-amp-*")
	if err != nil {
		return nil, err
	}

	temporary := file.Name()
	defer func() { _ = os.Remove(temporary) }()

	token := filepath.Base(temporary)

	_, writeErr := file.WriteString(strings.ReplaceAll(bridgeSource, "ACP_GO_AMP_LAUNCH_TOKEN", token))
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return nil, err
	}

	plugin := filepath.Join(root, token[1:]+".ts")
	if err := os.Link(temporary, plugin); err != nil {
		return nil, err
	}

	b.plugin = plugin

	request.Env = append(request.Env, InternalEnvPrefix+"PLUGIN="+token, InternalEnvPrefix+"BRIDGE="+directory, InternalEnvPrefix+"THREAD="+thread)
	request.Args = append(request.Args, "--plugin-ready-timeout", "20")
	complete = true

	return b, nil
}

func (b *bridge) close() {
	if b.events != nil {
		_ = b.events.Close()
	}

	if b.plugin != "" {
		_ = os.Remove(b.plugin)
	}

	_ = os.RemoveAll(b.directory)
}

func (b *bridge) read() ([]bridgeEvent, error) {
	data, err := io.ReadAll(io.LimitReader(b.events, MaxNativeBytes+1))
	if err != nil {
		return nil, err
	}

	b.pending = append(b.pending, data...)
	if len(b.pending) > MaxNativeBytes {
		return nil, errors.New("native lifecycle event exceeds maximum size")
	}

	var events []bridgeEvent

	for {
		line, rest, found := bytes.Cut(b.pending, []byte{'\n'})
		if !found {
			break
		}

		var event bridgeEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, err
		}

		events = append(events, event)
		b.pending = rest
	}

	return events, nil
}

// RunAttached observes the existing thread before submitting input, then joins
// native cancellation and terminal receipts before closing its local observer.
// Nil input reads native state without submitting a prompt or spending tokens.
func RunAttached(ctx context.Context, request process.Request, scratch, thread string, input []byte, prepare func(View) error, consume func([]byte) error) (Outcome, process.Result, string, error) {
	if err := ctx.Err(); err != nil {
		return Outcome{}, process.Result{}, "", err
	}

	b, err := installBridge(&request, scratch, thread)
	if err != nil {
		return Outcome{}, process.Result{}, "", err
	}
	defer b.close()

	var outcome Outcome

	_, result, stderr, err := run(context.WithoutCancel(ctx), request, func(p *process.Process) error {
		return b.control(ctx, p, thread, input, prepare, &outcome)
	}, consume)
	if err != nil {
		outcome.View = nil
	}

	return outcome, result, stderr, err
}

type attachedState struct {
	outcome    *Outcome
	start      json.RawMessage
	deadline   time.Time
	cancelSent bool
	cancelAck  bool
	inputDone  chan error
}

func (b *bridge) control(ctx context.Context, p *process.Process, thread string, input []byte, prepare func(View) error, outcome *Outcome) error {
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()

	state := attachedState{outcome: outcome, deadline: time.Now().Add(bridgeTimeout)}
	defer func() {
		if state.inputDone != nil {
			_ = p.Stdin().Close()

			<-state.inputDone
		}
	}()

	cancelSignal := ctx.Done()
	exited := false

	for {
		events, err := b.read()
		if err != nil {
			return err
		}

		for i := range events {
			if events[i].ThreadID != thread {
				return errors.New("native lifecycle thread identity drift")
			}

			if err := state.accept(ctx, p, &events[i], input, prepare); err != nil {
				return err
			}
		}

		if outcome.View != nil && (!state.cancelSent || state.cancelAck) {
			return stopObserver(p, outcome)
		}

		if exited {
			return nil
		}

		if !state.deadline.IsZero() && time.Now().After(state.deadline) {
			return errors.New("native lifecycle acknowledgement timed out")
		}

		select {
		case inputErr := <-state.inputDone:
			state.inputDone = nil

			if inputErr != nil {
				return inputErr
			}
		case <-p.Done():
			exited = true
		case <-cancelSignal:
			cancelSignal = nil

			if !outcome.Submitted {
				return stopObserver(p, outcome)
			}

			if err := os.WriteFile(filepath.Join(b.directory, "cancel"), nil, 0o600); err != nil {
				return err
			}

			state.cancelSent = true
			state.deadline = time.Now().Add(bridgeTimeout)
		case <-timer.C:
		}
	}
}

func (s *attachedState) accept(ctx context.Context, p *process.Process, event *bridgeEvent, input []byte, prepare func(View) error) error {
	switch event.Type {
	case "ready":
		if !quiet(event.State) {
			return errors.New("remote Amp thread is still active")
		}

		view := event.view()
		if err := validateView(view); err != nil {
			return err
		}

		if prepare != nil {
			if err := prepare(view); err != nil {
				return err
			}
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if len(input) == 0 {
			s.outcome.View = &view

			return nil
		}

		if s.outcome.Submitted {
			return errors.New("duplicate native readiness event")
		}

		s.inputDone = make(chan error, 1)
		go func(done chan<- error) { _, err := p.Stdin().Write(input); done <- err }(s.inputDone)

		s.outcome.Submitted = true
		s.deadline = time.Time{}
	case "start":
		if !s.outcome.Submitted || len(s.start) != 0 {
			return errors.New("unexpected native turn start")
		}

		s.start = event.ID
	case "end":
		if len(s.start) == 0 || !bytes.Equal(s.start, event.ID) {
			return errors.New("native turn receipt identity drift")
		}

		if event.Status != StatusDone && event.Status != StatusError && event.Status != StatusCancelled {
			return errors.New("invalid native turn status")
		}

		receipt := event.receipt()
		s.outcome.Receipt = &receipt
		s.deadline = time.Now().Add(bridgeTimeout)
	case "settled":
		if s.outcome.Receipt == nil || !quiet(event.State) {
			return nil
		}

		view := event.view()
		if err := validateView(view); err != nil {
			return err
		}

		if err := containsReceipt(view, *s.outcome.Receipt); err != nil {
			return err
		}

		s.outcome.View = &view
	case "cancel.ack":
		s.cancelAck = true
	case "error":
		return errors.New("native lifecycle bridge failed")
	default:
		return errors.New("unknown native lifecycle event")
	}

	return nil
}

func stopObserver(p *process.Process, outcome *Outcome) error {
	outcome.Stopped = true
	_ = p.Stdin().Close()

	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-p.Done():
		return nil
	case <-timer.C:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return p.Shutdown(ctx, time.Second)
}

func quiet(state string) bool { return state == stateIdle || state == StatusError }

func containsReceipt(view View, receipt Receipt) error {
	if len(receipt.Messages) == 0 || len(view.Messages) < len(receipt.Messages) || !bytes.Equal(receipt.ID, receipt.Messages[0].ID) {
		return errors.New("native receipt messages missing from settled thread")
	}

	for i, expected := range receipt.Messages {
		actual := view.Messages[len(view.Messages)-len(receipt.Messages)+i]
		if !bytes.Equal(actual.ID, expected.ID) || actual.Role != expected.Role || !reflect.DeepEqual(uniqueToolResults(normalizePluginContent(actual.Content)), uniqueToolResults(normalizePluginContent(expected.Content))) {
			return errors.New("native receipt differs from settled messages")
		}
	}

	return nil
}

func validateView(view View) error {
	if view.Messages == nil {
		return errors.New("native thread messages missing")
	}

	seen := make(map[string]bool, len(view.Messages))
	for _, message := range view.Messages {
		var id string
		if err := json.Unmarshal(message.ID, &id); err != nil || id == "" {
			return errors.New("native message identity missing")
		}

		key := string(message.ID)
		if seen[key] || message.Content == nil {
			return errors.New("duplicate or incomplete native message")
		}

		seen[key] = true

		if message.Role != roleUser && message.Role != roleAssistant && message.Role != roleInfo {
			return errors.New("invalid native message role")
		}
	}

	return nil
}
