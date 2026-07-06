//go:build integration

package integration

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	ampacp "github.com/savid/acp-go-amp"
)

const (
	envRunIntegration = "ACP_GO_AMP_RUN_INTEGRATION"
	envRunLiveTokens  = "ACP_GO_AMP_RUN_LIVE_TOKENS"
	envAgentBinary    = "ACP_GO_AMP_AGENT_BINARY"
	envAmpHome        = "ACP_GO_AMP_HOME"
	envAmpAPIKey      = "AMP_API_KEY" //nolint:gosec // Environment variable name, not a credential value.
	envAmpURL         = "AMP_URL"
)

var integrationLogger = slog.New(slog.DiscardHandler)

func TestMain(m *testing.M) {
	previousLogger := slog.Default()
	slog.SetDefault(integrationLogger)

	code := m.Run()

	slog.SetDefault(previousLogger)
	os.Exit(code)
}

// requireIntegration gates every integration-tier test behind the explicit
// opt-in env var so an ungated `go test` never reaches the native binary even
// with the integration build tag compiled in.
func requireIntegration(t *testing.T) {
	t.Helper()

	if os.Getenv(envRunIntegration) != "1" {
		t.Skipf("set %s=1 to run the Amp integration tier", envRunIntegration)
	}
}

// requireLiveTokens additionally gates token-spending live tests. Once the
// integration tier is enabled, a missing live opt-in is a clean skip; a broken
// prerequisite inside an enabled live test is a hard failure (see requireAmpAPIKey).
func requireLiveTokens(t *testing.T) {
	t.Helper()

	requireIntegration(t)
	if os.Getenv(envRunLiveTokens) != "1" {
		t.Skipf("set %s=1 to run token-spending live Amp tests", envRunLiveTokens)
	}
}

// integrationAmpPath resolves the local amp binary for smoke coverage, skipping
// cleanly when it is absent.
func integrationAmpPath(t *testing.T) string {
	t.Helper()

	requireIntegration(t)
	path, err := exec.LookPath("amp")
	if err != nil {
		t.Skipf("amp binary absent: %v", err)
	}

	return path
}

// requireAmpAPIKey fails (not skips) an already-opted-in live test whose token
// credential is missing, so a live suite never goes silently green.
func requireAmpAPIKey(t *testing.T) string {
	t.Helper()

	apiKey := os.Getenv(envAmpAPIKey)
	if apiKey == "" {
		t.Fatalf("live Amp tests require %s", envAmpAPIKey)
	}

	return apiKey
}

// recordingClient is the generic ACP client stub used across the integration
// suite: it captures streamed text, updates, and command advertisements while
// satisfying the full acp.Client surface.
type recordingClient struct {
	mu sync.Mutex

	textChunks []string
	updates    []acp.SessionUpdate
	commands   []acp.AvailableCommand
}

var _ acp.Client = (*recordingClient)(nil)

func (c *recordingClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{Content: ""}, nil
}

func (c *recordingClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, nil
}

func (c *recordingClient) RequestPermission(
	_ context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	for _, option := range params.Options {
		if option.Kind == acp.PermissionOptionKindAllowOnce || option.Kind == acp.PermissionOptionKindAllowAlways {
			return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(option.OptionId)}, nil
		}
	}

	return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeCancelled()}, nil
}

func (c *recordingClient) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.updates = append(c.updates, params.Update)

	switch {
	case params.Update.AvailableCommandsUpdate != nil:
		c.commands = append(c.commands, params.Update.AvailableCommandsUpdate.AvailableCommands...)
	case params.Update.AgentMessageChunk != nil && params.Update.AgentMessageChunk.Content.Text != nil:
		c.textChunks = append(c.textChunks, params.Update.AgentMessageChunk.Content.Text.Text)
	case params.Update.UserMessageChunk != nil && params.Update.UserMessageChunk.Content.Text != nil:
		c.textChunks = append(c.textChunks, params.Update.UserMessageChunk.Content.Text.Text)
	}

	return nil
}

func (c *recordingClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{TerminalId: "terminal-1"}, nil
}

func (c *recordingClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, nil
}

func (c *recordingClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{Output: "", Truncated: false}, nil
}

func (c *recordingClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, nil
}

func (c *recordingClient) WaitForTerminalExit(
	context.Context,
	acp.WaitForTerminalExitRequest,
) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, nil
}

func (c *recordingClient) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return strings.Join(c.textChunks, "")
}

// lockedBuffer is a concurrency-safe buffer for capturing agent stderr.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

type liveAgentPipes struct {
	clientInput io.Writer
	agentOutput io.Reader
}

// serveLiveAgentForTest serves the in-process amp ACP agent over pipes and
// returns a client-side connection wired to the recording client.
func serveLiveAgentForTest(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	opts ...ampacp.Option,
) *acp.ClientSideConnection {
	t.Helper()

	pipes := serveLiveAgentRawForTest(t, ctx, opts...)

	return acp.NewClientSideConnection(client, pipes.clientInput, pipes.agentOutput)
}

func serveLiveAgentRawForTest(
	t *testing.T,
	ctx context.Context,
	opts ...ampacp.Option,
) liveAgentPipes {
	t.Helper()

	ampPath := integrationAmpPath(t)
	base := []ampacp.Option{
		ampacp.WithExecutablePath(ampPath),
		ampacp.WithHome(t.TempDir()),
		ampacp.WithLogger(integrationLogger),
	}

	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	serveCtx, stopServe := context.WithCancel(ctx)

	serveErr := make(chan error, 1)
	go func() {
		options := append(base, opts...)
		serveErr <- ampacp.Serve(serveCtx, c2aR, a2cW, options...)
	}()

	t.Cleanup(func() {
		stopServe()
		_ = c2aR.Close()
		_ = c2aW.Close()
		_ = a2cR.Close()
		_ = a2cW.Close()

		select {
		case err := <-serveErr:
			if err != nil && ctx.Err() == nil {
				t.Logf("live agent serve returned: %v", err)
			}
		case <-time.After(time.Second):
			t.Log("live agent serve did not stop within cleanup timeout")
		}
	})

	return liveAgentPipes{clientInput: c2aW, agentOutput: a2cR}
}

// connectLiveAgentBinary drives the compiled acp-go-amp binary over its stdio,
// which is how test-integration-cover measures real binary coverage. It skips
// when the prebuilt binary override is absent.
func connectLiveAgentBinary(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	initReq acp.InitializeRequest,
	ampPath string,
) *acp.ClientSideConnection {
	t.Helper()

	agentPath := os.Getenv(envAgentBinary)
	if agentPath == "" {
		t.Skipf("set %s to run compiled binary integration coverage", envAgentBinary)
	}

	args := []string{"-path", ampPath, "-home", t.TempDir()}

	cmd := exec.Command(agentPath, args...) // #nosec G204 -- path is the test-built agent binary.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}

	var stderr lockedBuffer
	cmd.Stderr = &stderr
	if startErr := cmd.Start(); startErr != nil {
		t.Fatal(startErr)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	t.Cleanup(func() {
		_ = stdin.Close()
		select {
		case err := <-done:
			if err != nil && ctx.Err() == nil {
				t.Logf("compiled agent exited with error: %v; stderr: %s", err, stderr.String())
			}
		case <-time.After(5 * time.Second):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			if err := <-done; err != nil && ctx.Err() == nil {
				t.Logf("compiled agent killed during cleanup: %v; stderr: %s", err, stderr.String())
			}
		}
	})

	clientConn := acp.NewClientSideConnection(client, stdin, stdout)
	if initReq.ProtocolVersion == 0 {
		initReq.ProtocolVersion = acp.ProtocolVersionNumber
	}
	if _, initErr := clientConn.Initialize(ctx, initReq); initErr != nil {
		t.Fatalf("initialize compiled agent: %v; stderr: %s", initErr, stderr.String())
	}

	return clientConn
}

// isolatedAmpEnv builds a hermetic native HOME/XDG environment rooted under a
// temp directory so live tests never touch the developer's real Amp config.
// Auth is injected explicitly via AMP_API_KEY (and AMP_URL when set).
func isolatedAmpEnv(t *testing.T, root string, apiKey string) (map[string]string, string) {
	t.Helper()
	paths := map[string]string{
		"HOME":            filepath.Join(root, "home"),
		"XDG_CONFIG_HOME": filepath.Join(root, "xdg-config"),
		"XDG_CACHE_HOME":  filepath.Join(root, "xdg-cache"),
		"XDG_DATA_HOME":   filepath.Join(root, "xdg-data"),
		"XDG_STATE_HOME":  filepath.Join(root, "xdg-state"),
		envAmpHome:        filepath.Join(root, "wrapper-home"),
		envAmpAPIKey:      apiKey,
	}
	if ampURL := os.Getenv(envAmpURL); ampURL != "" {
		paths[envAmpURL] = ampURL
	}
	for _, path := range paths {
		if strings.HasPrefix(path, root) {
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}
	}

	return paths, paths[envAmpHome]
}

// fakeAmpBinary writes a deterministic POSIX shell stand-in for the amp binary
// that speaks just enough of the stream-json surface to drive a full ACP turn
// without a real installation or model tokens.
func fakeAmpBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "amp")
	script := `#!/bin/sh
if [ "$1" = "version" ]; then
  echo "0.0.1783155105-gfake"
  exit 0
fi
last=""
for arg in "$@"; do last="$arg"; done
if [ "$last" = "--help" ]; then
  echo "--settings-file --mcp-config -m --effort --json --stream-json-input threads continue threads export threads delete"
  exit 0
fi
for arg in "$@"; do
  if [ "$arg" = "T-00000000-0000-0000-0000-000000000000" ]; then
    echo "Thread not found" >&2
    exit 1
  fi
done
prev=""
sub=""
for arg in "$@"; do
  if [ "$prev" = "threads" ]; then sub="$arg"; break; fi
  prev="$arg"
done
case "$sub" in
  new) echo "T-smoke-thread" ;;
  list) echo '[]' ;;
  export) echo '{"thread":"T-smoke-thread"}' ;;
  delete) echo "deleted" ;;
  continue)
    cat >/dev/null
    echo '{"type":"assistant","message":{"content":[{"type":"text","text":"ok"}]},"session_id":"T-smoke-thread"}'
    echo '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"T-smoke-thread"}'
    ;;
  *) echo "bad args: $*" >&2; exit 2 ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil { // #nosec G306 -- executable test stub.
		t.Fatal(err)
	}

	return path
}
