package integration

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/savid/acp-go-amp/internal/amp"
)

func TestSmokeAmpVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := amp.NewClient(slog.Default(), amp.Options{})
	version, err := client.Version(ctx)
	if err != nil {
		t.Skipf("amp unavailable: %v", err)
	}
	if strings.TrimSpace(version) == "" {
		t.Fatal("empty amp version")
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
