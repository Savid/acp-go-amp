//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	ampacp "github.com/savid/acp-go-amp"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

const (
	envRunIntegration = "ACP_GO_AMP_RUN_INTEGRATION"
	envRunLiveTokens  = "ACP_GO_AMP_RUN_LIVE_TOKENS"
	envHarnessPath    = "ACP_GO_AMP_HARNESS_PATH"

	testTimeout = 120 * time.Second
)

func requireIntegration(t *testing.T) {
	t.Helper()

	if os.Getenv(envRunIntegration) != "1" {
		t.Skipf("set %s=1 to run integration tests", envRunIntegration)
	}
}

func requireLive(t *testing.T) {
	t.Helper()
	requireIntegration(t)

	if os.Getenv(envRunLiveTokens) != "1" {
		t.Skipf("set %s=1 to run token-spending tests", envRunLiveTokens)
	}
}

// harnessPath resolves the amp binary; a missing amp skips the smoke tier and
// fails the live tier.
func harnessPath(t *testing.T, live bool) string {
	t.Helper()

	path := os.Getenv(envHarnessPath)
	if path == "" {
		path = "amp"
	}

	resolved, err := exec.LookPath(path)
	if err != nil {
		if live {
			t.Fatalf("amp not found: %v", err)
		}

		t.Skipf("amp not installed: %v", err)
	}

	return resolved
}

// recorder is the ACP client the tests observe the agent through.
type recorder struct {
	mu      sync.Mutex
	updates []acp.SessionNotification
	changed chan struct{}
}

var (
	_ acp.Client                 = (*recorder)(nil)
	_ acp.ExtensionMethodHandler = (*recorder)(nil)
)

func newRecorder() *recorder {
	return &recorder{changed: make(chan struct{}, 1)}
}

func (r *recorder) signal() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *recorder) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	r.mu.Lock()
	r.updates = append(r.updates, params)
	r.mu.Unlock()
	r.signal()

	return nil
}

// RequestPermission exists because acp.Client requires it. Amp has no
// permission surface and never sends one.
func (*recorder) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{}, errors.New("unsupported")
}

func (r *recorder) HandleExtensionMethod(_ context.Context, method string, _ json.RawMessage) (any, error) {
	if method == ampacp.RawEventMethod {
		r.signal()
	}

	return map[string]any{}, nil
}

func (*recorder) NotifyExtension(context.Context, string, any) error { return nil }

func (*recorder) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, errors.New("unsupported")
}

func (*recorder) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, errors.New("unsupported")
}

func (*recorder) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, errors.New("unsupported")
}

func (*recorder) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, errors.New("unsupported")
}

// snapshot returns the notifications recorded so far.
func (r *recorder) snapshot() []acp.SessionNotification {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]acp.SessionNotification(nil), r.updates...)
}

// waitFor blocks until condition holds over the recorded notifications.
func (r *recorder) waitFor(t *testing.T, condition func([]acp.SessionNotification) bool) {
	t.Helper()

	deadline := time.After(testTimeout)

	for {
		if condition(r.snapshot()) {
			return
		}

		select {
		case <-r.changed:
		case <-deadline:
			t.Fatalf("condition not met; %d notifications recorded", len(r.snapshot()))
		}
	}
}

// harness serves an agent over pipes to a recording client.
type harness struct {
	t        *testing.T
	conn     *acp.ClientSideConnection
	rec      *recorder
	stopOnce sync.Once
	stop     func()
}

func newHarness(t *testing.T, extra ...ampacp.Option) *harness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	clientReader, agentWriter := io.Pipe()
	agentReader, clientWriter := io.Pipe()
	rec := newRecorder()
	served := make(chan error, 1)

	go func() { served <- ampacp.Serve(ctx, agentReader, agentWriter, extra...) }()

	conn := acp.NewClientSideConnection(rec, clientWriter, clientReader)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	h := &harness{t: t, conn: conn, rec: rec}

	h.stop = func() {
		h.stopOnce.Do(func() {
			cancel()
			_ = clientWriter.Close()
			select {
			case <-served:
			case <-time.After(testTimeout):
				t.Error("ampacp.Serve did not return")
			}
		})
	}
	t.Cleanup(h.stop)

	return h
}

func (h *harness) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	h.t.Cleanup(cancel)

	return ctx
}

func (h *harness) initialize(opts ...func(*acp.InitializeRequest)) acp.InitializeResponse {
	h.t.Helper()

	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	for _, opt := range opts {
		opt(&request)
	}

	resp, err := h.conn.Initialize(h.ctx(), request)
	require.NoError(h.t, err)

	return resp
}

func withLifecycle() func(*acp.InitializeRequest) {
	return func(request *acp.InitializeRequest) {
		request.Meta = map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}
	}
}

func (h *harness) prompt(sessionID acp.SessionId, text string, meta map[string]any) (acp.PromptResponse, error) {
	h.t.Helper()

	request := wire.TextPromptRequest(sessionID, text)
	request.Meta = meta

	return h.conn.Prompt(h.ctx(), request)
}

// promptMeta stamps the lifecycle prompt correlation.
func promptMeta(n int) map[string]any {
	return map[string]any{wire.LifecycleKey: map[string]any{
		"version": 1, "submission": map[string]any{"submissionId": fmt.Sprintf("sub-%d", n), "clientNonce": fmt.Sprintf("non-%d", n)},
	}}
}

// agentText concatenates streamed agent message text.
func agentText(updates []acp.SessionNotification) string {
	var text strings.Builder

	for _, update := range updates {
		if chunk := update.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
			text.WriteString(chunk.Content.Text.Text)
		}
	}

	return text.String()
}
