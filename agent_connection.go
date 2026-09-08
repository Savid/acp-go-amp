package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/lifecycle"
)

// agentClient is the client-facing connection surface the agent emits through.
// Amp never bridges permissions, elicitation, file system, or terminal calls,
// so the surface is exactly session updates plus extension notifications.
type agentClient interface {
	Done() <-chan struct{}
	SessionUpdate(context.Context, acp.SessionNotification) error
	NotifyExtension(context.Context, string, any) error
}

type localAgentConnection struct {
	agent       *Agent
	conn        *acp.Connection
	initialized atomic.Bool
}

type localAgentHandler func(context.Context, *Agent, json.RawMessage) (any, *acp.RequestError)

type localAgentParams[Req any] interface {
	*Req
	Validate() error
}

var (
	_ agentClient = (*localAgentConnection)(nil)

	localAgentHandlers = map[string]localAgentHandler{
		acp.AgentMethodAuthenticate:           localResponse((*Agent).Authenticate),
		acp.AgentMethodInitialize:             localResponse((*Agent).Initialize),
		acp.AgentMethodLogout:                 localResponse((*Agent).Logout),
		acp.AgentMethodSessionCancel:          localNotification((*Agent).Cancel),
		acp.AgentMethodSessionClose:           localResponse((*Agent).CloseSession),
		acp.AgentMethodSessionDelete:          localResponse((*Agent).UnstableDeleteSession),
		acp.AgentMethodSessionList:            localResponse((*Agent).ListSessions),
		acp.AgentMethodSessionLoad:            localResponse((*Agent).LoadSession),
		acp.AgentMethodSessionNew:             localResponse((*Agent).NewSession),
		acp.AgentMethodSessionPrompt:          localResponse((*Agent).Prompt),
		acp.AgentMethodSessionResume:          localResponse((*Agent).ResumeSession),
		acp.AgentMethodSessionSetConfigOption: localResponse((*Agent).SetSessionConfigOption),
	}
)

func newLocalAgentConnection(agent *Agent, output io.Writer, input io.Reader) *localAgentConnection {
	conn := &localAgentConnection{agent: agent}
	inputGate := newConnectionInputGate(input)
	conn.conn = acp.NewConnection(conn.handle, output, inputGate)
	conn.conn.SetLogger(agent.log)
	inputGate.open()

	return conn
}

// connectionInputGate blocks the SDK receive goroutine until the connection
// logger is installed. The SDK starts receiving inside NewConnection.
type connectionInputGate struct {
	reader io.Reader
	ready  chan struct{}
	once   sync.Once
}

func newConnectionInputGate(reader io.Reader) *connectionInputGate {
	return &connectionInputGate{reader: reader, ready: make(chan struct{})}
}

func (g *connectionInputGate) open() {
	g.once.Do(func() { close(g.ready) })
}

func (g *connectionInputGate) Read(p []byte) (int, error) {
	<-g.ready

	return g.reader.Read(p)
}

func (c *localAgentConnection) Done() <-chan struct{} {
	return c.conn.Done()
}

func (c *localAgentConnection) handle(ctx context.Context, method string, params json.RawMessage) (any, *acp.RequestError) {
	// Closed is the outermost dispatch state. Once set it wins over the
	// initialize gate, method lookup, and parameter decoding for every request.
	if err := c.agent.ensureOpen(); err != nil {
		return nil, requestError(ctx, err)
	}

	if method != acp.AgentMethodInitialize && !c.initialized.Load() {
		return nil, acp.NewInvalidRequest(map[string]any{
			jsonFieldMethod: method,
			jsonFieldError:  "initialize must be called before other ACP methods",
		})
	}

	if strings.HasPrefix(method, "_") {
		result, err := c.agent.HandleExtensionMethod(ctx, method, params)

		return result, requestError(ctx, err)
	}

	handler, ok := localAgentHandlers[method]
	if !ok {
		return nil, acp.NewMethodNotFound(method)
	}

	result, reqErr := handler(ctx, c.agent, params)
	if method == acp.AgentMethodInitialize && reqErr == nil {
		c.initialized.Store(true)
	}

	return result, reqErr
}

func localResponse[Req any, ReqPtr localAgentParams[Req], Resp any](
	call func(*Agent, context.Context, Req) (Resp, error),
) localAgentHandler {
	return func(ctx context.Context, agent *Agent, params json.RawMessage) (any, *acp.RequestError) {
		value, reqErr := decodeLocalAgentParams[Req, ReqPtr](params)
		if reqErr != nil {
			return nil, reqErr
		}

		resp, err := call(agent, ctx, value)
		if err != nil {
			return nil, requestError(ctx, err)
		}

		return resp, nil
	}
}

func localNotification[Req any, ReqPtr localAgentParams[Req]](
	call func(*Agent, context.Context, Req) error,
) localAgentHandler {
	return func(ctx context.Context, agent *Agent, params json.RawMessage) (any, *acp.RequestError) {
		value, reqErr := decodeLocalAgentParams[Req, ReqPtr](params)
		if reqErr != nil {
			return nil, reqErr
		}

		if err := call(agent, ctx, value); err != nil {
			return nil, requestError(ctx, err)
		}

		return nil, nil
	}
}

func decodeLocalAgentParams[Req any, ReqPtr localAgentParams[Req]](params json.RawMessage) (Req, *acp.RequestError) {
	var value Req

	masked := lifecycle.MaskWireMeta(params, lifecycle.MetaKey)
	if _, prompt := any(&value).(*acp.PromptRequest); prompt {
		masked = maskPromptHandoffMeta(masked)
	}

	if err := json.Unmarshal(masked, &value); err != nil {
		return value, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	if err := ReqPtr(&value).Validate(); err != nil {
		return value, acp.NewInvalidParams(map[string]any{jsonFieldError: err.Error()})
	}

	preserveLocalLifecycleMeta(params, &value)

	if prompt, ok := any(&value).(*acp.PromptRequest); ok {
		preservePromptHandoffMeta(params, prompt)
	}

	return value, nil
}

func preserveLocalLifecycleMeta(params json.RawMessage, value any) {
	// Config-option requests are the SDK's union; the other typed requests
	// carry their metadata directly on the request struct.
	if option, ok := value.(*acp.SetSessionConfigOptionRequest); ok {
		if option.Boolean != nil {
			option.Boolean.Meta = lifecycle.PreserveWireMeta(params, option.Boolean.Meta)
		}

		if option.ValueId != nil {
			option.ValueId.Meta = lifecycle.PreserveWireMeta(params, option.ValueId.Meta)
		}

		return
	}

	field := reflect.ValueOf(value).Elem().FieldByName("Meta")
	if field.IsValid() {
		meta, _ := field.Interface().(map[string]any)
		field.Set(reflect.ValueOf(lifecycle.PreserveWireMeta(params, meta)))
	}
}

func maskPromptHandoffMeta(params json.RawMessage) json.RawMessage {
	return lifecycle.RewriteWireObject(params, func(name string, raw json.RawMessage) json.RawMessage {
		if !strings.EqualFold(name, "prompt") {
			return raw
		}

		var blocks []json.RawMessage
		if json.Unmarshal(raw, &blocks) != nil {
			return raw
		}

		for index, block := range blocks {
			var kind struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(block, &kind) == nil && kind.Type == valImage {
				blocks[index] = lifecycle.MaskWireMeta(block, metaHandoffKey)
			}
		}

		encoded, _ := json.Marshal(blocks)

		return encoded
	})
}

func preservePromptHandoffMeta(params json.RawMessage, prompt *acp.PromptRequest) {
	var original struct {
		Prompt []json.RawMessage `json:"prompt"`
	}

	_ = json.Unmarshal(params, &original)

	for index, block := range prompt.Prompt {
		if block.Image == nil {
			continue
		}

		var carrier struct {
			Meta map[string]json.RawMessage `json:"_meta"` //nolint:tagliatelle // ACP metadata field.
		}

		_ = json.Unmarshal(original.Prompt[index], &carrier)
		if raw, present := carrier.Meta[metaHandoffKey]; present {
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()

			var value any

			_ = decoder.Decode(&value)
			block.Image.Meta[metaHandoffKey] = value
		}
	}
}

func (c *localAgentConnection) SessionUpdate(ctx context.Context, params acp.SessionNotification) error {
	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()

	return c.conn.SendNotification(ctx, acp.ClientMethodSessionUpdate, params)
}

func (c *localAgentConnection) NotifyExtension(ctx context.Context, method string, params any) error {
	if method == "" || !strings.HasPrefix(method, "_") {
		return fmt.Errorf("extension method name must start with '_' (got %q)", method)
	}

	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()

	return c.conn.SendNotification(ctx, method, params)
}

// requestError maps a handler failure onto the wire error the peer receives.
//
// An honored $/cancel_request is the only thing that cancels a request context
// with cause context.Canceled: connection teardown cancels the parent with the
// transport cause, and an adapter deadline yields context.DeadlineExceeded, so
// neither is ever reported as cancelled and a deadline stays an internal
// failure. The cause is therefore what identifies a cancel, and it is read
// ahead of the error: work aborted by a cancel routinely joins a typed
// RequestError on the way out, and reporting that instead of -32800 would
// answer a request the peer withdrew with an error about its parameters.
// errors.Is(err, context.Canceled) cannot make that distinction — it also
// matches a request that merely wrapped an unrelated cancellation — so it is
// deliberately not consulted.
func requestError(ctx context.Context, err error) *acp.RequestError {
	if err == nil {
		return nil
	}

	if context.Cause(ctx) == context.Canceled {
		return acp.NewRequestCancelled(map[string]any{jsonFieldError: err.Error()})
	}

	var reqErr *acp.RequestError
	if errors.As(err, &reqErr) {
		return reqErr
	}

	// Nothing above gave this failure a shape, so it answers the unclassified
	// token. The Go text stays in the connection log rather than on the wire.
	return internalFailure(classHandlerFailed)
}

func (a *Agent) setConnection(conn agentClient) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
}

func (a *Agent) connection() agentClient {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.conn
}

func (a *Agent) acquireClientCall(ctx context.Context) (func(), error) {
	select {
	case a.clientCalls <- struct{}{}:
		return func() { <-a.clientCalls }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, backpressureError("client_calls")
	}
}

func backpressureError(limit string) error {
	return acp.NewInvalidRequest(map[string]any{jsonFieldError: "backpressure", "limit": limit})
}
