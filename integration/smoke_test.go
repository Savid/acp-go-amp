package integration

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	ampacp "github.com/savid/acp-go-amp"
	"github.com/savid/acp-go-amp/internal/amp"
)

func TestSmokeAmpVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := exec.LookPath("amp"); err != nil {
		t.Skipf("amp binary absent: %v", err)
	}
	client := amp.NewClient(slog.Default(), amp.Options{})
	version, err := client.Version(ctx)
	if err != nil {
		t.Fatalf("amp binary present but version probe failed: %v", err)
	}
	if strings.TrimSpace(version) == "" {
		t.Fatal("empty amp version")
	}
}

func TestSmokeFakeACPLifecycleAndMCP(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fake is POSIX-only")
	}
	path := fakeSmokeAmpPath(t)
	agent := ampacp.NewAgent(
		ampacp.WithExecutablePath(path),
		ampacp.WithHome(t.TempDir()),
		ampacp.WithSessionStore(ampacp.NewInMemorySessionStore()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cwd := t.TempDir()
	resp, err := agent.NewSession(ctx, ampacp.NewSessionRequest(cwd,
		ampacp.WithSessionMCPServers(
			ampacp.StdioMCPServer("stdio", "true", nil, nil),
			ampacp.HTTPMCPServer("http", "https://example.test/mcp", nil),
		),
	))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, sseErr := agent.NewSession(ctx, ampacp.NewSessionRequest(cwd, ampacp.WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://example.test/sse"}}))); sseErr == nil {
		t.Fatal("SSE MCP accepted")
	}
	promptResp, err := agent.Prompt(ctx, ampacp.TextPromptRequest(resp.SessionId, "smoke"))
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if promptResp.StopReason != "end_turn" {
		t.Fatalf("prompt stop reason = %q", promptResp.StopReason)
	}
	if _, err := agent.UnstableDeleteSession(ctx, ampacp.DeleteSessionRequest(resp.SessionId)); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
}

func TestLiveThreadTurn(t *testing.T) {
	if os.Getenv("ACP_GO_AMP_LIVE") != "1" {
		t.Skip("set ACP_GO_AMP_LIVE=1 to run live Amp prompt test")
	}
	dir := t.TempDir()
	settings := filepath.Join(dir, "settings.json")
	if err := os.WriteFile(settings, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := amp.NewClient(slog.Default(), amp.Options{SettingsFile: settings, Mode: "smart", Effort: "high"})
	thread, err := client.NewThread(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.DeleteThread(context.Background(), thread) }()
	turn, err := client.Continue(ctx, thread, map[string]any{
		"type": "user",
		"message": map[string]any{
			"role": "user",
			"content": []map[string]any{{
				"type": "text",
				"text": "Reply with exactly: acp-go-amp-live-ok",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var gotResult bool
	for msg := range turn.Messages() {
		if result, ok := msg.(*amp.ResultMessage); ok {
			gotResult = strings.Contains(result.Result, "acp-go-amp-live-ok")
		}
	}
	if !gotResult {
		t.Fatal("missing expected live result")
	}
	if _, err := client.ExportThread(ctx, thread); err != nil {
		t.Fatal(err)
	}
}

func TestLiveRestoreAfterLocalStateWipe(t *testing.T) {
	if os.Getenv("ACP_GO_AMP_LIVE") != "1" {
		t.Skip("set ACP_GO_AMP_LIVE=1 to run live Amp restore test")
	}
	apiKey := os.Getenv("AMP_API_KEY")
	if apiKey == "" {
		t.Skip("set AMP_API_KEY for isolated live restore test")
	}

	root := t.TempDir()
	env, homeParent := isolatedAmpEnv(t, root, apiKey)
	store := ampacp.NewInMemorySessionStore()
	cwd := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	agent := ampacp.NewAgent(
		ampacp.WithHome(homeParent),
		ampacp.WithEnv(env),
		ampacp.WithSessionStore(store),
	)
	newResp, err := agent.NewSession(ctx, ampacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatal(err)
	}
	threadID := string(newResp.SessionId)
	cleanupEnv := env
	defer func() {
		if threadID != "" {
			deleteClient := amp.NewClient(slog.Default(), amp.Options{Env: cleanupEnv, Cwd: cwd})
			_ = deleteClient.DeleteThread(context.Background(), threadID)
		}
	}()

	if _, promptErr := agent.Prompt(ctx, ampacp.TextPromptRequest(newResp.SessionId, "Reply with exactly: acp-go-amp-restore-seed")); promptErr != nil {
		t.Fatalf("seed prompt: %v", promptErr)
	}
	_ = agent.Close()

	if removeErr := os.RemoveAll(root); removeErr != nil {
		t.Fatal(removeErr)
	}
	env, homeParent = isolatedAmpEnv(t, root, apiKey)
	cleanupEnv = env

	restored := ampacp.NewAgent(
		ampacp.WithHome(homeParent),
		ampacp.WithEnv(env),
		ampacp.WithSessionStore(store),
	)
	defer func() { _ = restored.Close() }()
	if _, loadErr := restored.LoadSession(ctx, ampacp.LoadSessionRequest(newResp.SessionId, cwd)); loadErr != nil {
		t.Fatalf("load after local wipe: %v", loadErr)
	}
	resp, promptErr := restored.Prompt(ctx, ampacp.TextPromptRequest(newResp.SessionId, "Reply with exactly: acp-go-amp-restore-ok"))
	if promptErr != nil {
		t.Fatalf("continue after local wipe: %v", promptErr)
	}
	if resp.StopReason != "end_turn" {
		t.Fatalf("restore prompt stop reason = %q", resp.StopReason)
	}
	if _, err := restored.UnstableDeleteSession(ctx, ampacp.DeleteSessionRequest(newResp.SessionId)); err != nil {
		t.Fatalf("delete restored thread: %v", err)
	}
	threadID = ""
}

func isolatedAmpEnv(t *testing.T, root string, apiKey string) (map[string]string, string) {
	t.Helper()
	paths := map[string]string{
		"HOME":            filepath.Join(root, "home"),
		"XDG_CONFIG_HOME": filepath.Join(root, "xdg-config"),
		"XDG_CACHE_HOME":  filepath.Join(root, "xdg-cache"),
		"XDG_DATA_HOME":   filepath.Join(root, "xdg-data"),
		"XDG_STATE_HOME":  filepath.Join(root, "xdg-state"),
		"ACP_GO_AMP_HOME": filepath.Join(root, "wrapper-home"),
		"AMP_API_KEY":     apiKey,
	}
	if ampURL := os.Getenv("AMP_URL"); ampURL != "" {
		paths["AMP_URL"] = ampURL
	}
	for _, path := range paths {
		if strings.HasPrefix(path, root) {
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}
	}

	return paths, paths["ACP_GO_AMP_HOME"]
}

func fakeSmokeAmpPath(t *testing.T) string {
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
prev=""
sub=""
for arg in "$@"; do
  if [ "$prev" = "threads" ]; then sub="$arg"; break; fi
  prev="$arg"
done
case "$sub" in
  new) echo "T-smoke-thread" ;;
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
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	return path
}
