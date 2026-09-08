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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	ampacp "github.com/savid/acp-go-amp"
	"github.com/savid/acp-go-amp/internal/amp"
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

func newIntegrationAgent(options ...ampacp.Option) *ampacp.Agent {
	return ampacp.NewAgent(options...)
}

func newIntegrationAmpClient(
	t *testing.T,
	logger *slog.Logger,
	options amp.Options,
) *amp.Client {
	t.Helper()

	return amp.NewClient(logger, options)
}

func TestMain(m *testing.M) {
	previousLogger := slog.Default()
	slog.SetDefault(integrationLogger)

	code := m.Run()
	cleanupIntegrationBinary()

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
	if os.Getenv(envRunIntegration) != "1" {
		t.Skipf("set %s=1 to run amp integration tests", envRunIntegration)
	}
	path := "amp"
	resolved, err := exec.LookPath(path)
	if err != nil {
		if os.Getenv(envRunLiveTokens) == "1" || os.Getenv("ACP_GO_AMP_RUN_ATTENDED") == "1" || os.Getenv("ACP_GO_AMP_RUN_KEYSTORE") == "1" {
			t.Fatalf("requested amp integration tier requires the CLI: %v", err)
		}
		t.Skipf("amp CLI absent from PATH for smoke (%s=1): %v", envRunIntegration, err)
	}
	return resolved
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

func (c *recordingClient) ampMessageIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ids := make([]string, 0)
	for _, update := range c.updates {
		if update.AgentMessageChunk == nil {
			continue
		}

		messageID, ok := ampMessageID(update.AgentMessageChunk.Meta)
		if ok {
			ids = append(ids, messageID)
		}
	}

	return ids
}

func ampMessageID(meta map[string]any) (string, bool) {
	ampMeta, ok := meta["amp"].(map[string]any)
	if !ok {
		return "", false
	}

	messageID, ok := ampMeta["messageId"].(string)

	return messageID, ok && messageID != ""
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
		ampacp.WithScratchDir(t.TempDir()),
		ampacp.WithLogger(integrationLogger),
	}

	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	serveCtx, stopServe := context.WithCancel(ctx)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- ampacp.Serve(serveCtx, c2aR, a2cW, append(base, opts...)...)
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
// which is how test-integration-cover measures real binary coverage. Without a
// prebuilt override, the package builds one adapter binary for the run.
func connectLiveAgentBinary(
	t *testing.T,
	ctx context.Context,
	client acp.Client,
	initReq acp.InitializeRequest,
	ampPath string,
) *acp.ClientSideConnection {
	t.Helper()

	agentPath := integrationBinaryPath(t)

	args := []string{"-path", ampPath}

	cmd := exec.CommandContext(ctx, agentPath, args...) // #nosec G204,G702 -- test-built adapter.

	cmd.Env = append(os.Environ(), envAmpAPIKey+"=fake-integration-key")
	process := startIntegrationProcess(t, cmd)
	stdin, stdout := process.stdin, process.stdout
	stderr := &process.stderr

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
  echo "0.0.1784765892-gfake"
  exit 0
fi
last=""
for arg in "$@"; do last="$arg"; done
if [ "$last" = "--help" ]; then
  echo "--settings-file --mcp-config -m --json --stream-json-input threads continue threads export threads delete"
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
if [ -z "$sub" ]; then
  for arg in "$@"; do
    if [ "$arg" = "-x" ]; then sub="continue"; break; fi
  done
fi
case "$sub" in
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

func TestIntegrationHarnessPrerequisites(t *testing.T) {
	if os.Args[len(os.Args)-1] == "harness-prerequisite-child" {
		path := integrationAmpPath(t)
		t.Log("resolved harness " + path)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, integration, tier, value, outcome string
		available                               bool
	}{
		{name: "ungated", outcome: "SKIP"},
		{name: "disabled", integration: "0", outcome: "SKIP"},
		{name: "invalid_gate", integration: "true", outcome: "SKIP"},
		{name: "missing_smoke", integration: "1", outcome: "SKIP"},
		{name: "disabled_live", integration: "1", tier: "RUN_LIVE_TOKENS", value: "0", outcome: "SKIP"},
		{name: "missing_live", integration: "1", tier: "RUN_LIVE_TOKENS", value: "1", outcome: "FAIL"},
		{name: "missing_attended", integration: "1", tier: "RUN_ATTENDED", value: "1", outcome: "FAIL"},
		{name: "missing_keystore", integration: "1", tier: "RUN_KEYSTORE", value: "1", outcome: "FAIL"},
		{name: "fake_path", integration: "1", outcome: "PASS", available: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, suffix := range []string{"RUN_INTEGRATION", "RUN_LIVE_TOKENS", "RUN_ATTENDED", "RUN_KEYSTORE"} {
				t.Setenv("ACP_GO_AMP_"+suffix, "0")
			}
			t.Setenv("ACP_GO_AMP_RUN_INTEGRATION", tc.integration)
			if tc.tier != "" {
				t.Setenv("ACP_GO_AMP_"+tc.tier, tc.value)
			}
			dir := t.TempDir()
			harness := filepath.Join(dir, "amp")
			if runtime.GOOS == "windows" {
				harness += ".exe"
			}
			if tc.available {
				// Resolution only: this file is never executed.
				if err := os.WriteFile(harness, []byte("fake harness path"), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("PATH", dir)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestIntegrationHarnessPrerequisites$", "-test.v", "--", "harness-prerequisite-child")
			cmd.WaitDelay = time.Second
			output, runErr := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			if (runErr != nil) != (tc.outcome == "FAIL") {
				t.Fatalf("unexpected child result: %v\n%s", runErr, output)
			}
			if !strings.Contains(string(output), "--- "+tc.outcome+": TestIntegrationHarnessPrerequisites") {
				t.Fatalf("want child %s:\n%s", tc.outcome, output)
			}
			if tc.available && !strings.Contains(string(output), "resolved harness "+harness) {
				t.Fatalf("fake harness selection was lost:\n%s", output)
			}
		})
	}
}
