package ampacp

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/coder/acp-go-sdk"
)

// TestReconcileNativeConfigReadBack pins R5-7: when amp's stream-json init frame
// reports a mode/effort that diverges from what the host requested, the wrapper
// reconciles session state to amp's truth, emits a config_option_update, and
// subsequent config-option reads report the native values rather than the echoed
// request.
func TestReconcileNativeConfigReadBack(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "reconcile-config")
	conn, client, cleanup := startTestServe(t,
		WithExecutablePath(path),
		WithHome(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	defer cleanup()
	ctx := context.Background()

	if _, err := conn.Initialize(ctx, acp.InitializeRequest{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	cwd := t.TempDir()
	newResp, err := conn.NewSession(ctx, NewSessionRequest(cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpMode("smart"), WithAmpEffort("low"))),
	))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// Requested surface is echoed before any native report.
	requireConfigValues(t, newResp.ConfigOptions, "smart", "low")

	if _, promptErr := conn.Prompt(ctx, acp.PromptRequest{
		SessionId: newResp.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock("hello")},
	}); promptErr != nil {
		t.Fatalf("Prompt: %v", promptErr)
	}

	// The turn's init frame reports deep/max: a config_option_update carries the
	// reconciled native truth to the host.
	var reconciled []acp.SessionConfigOption
	for _, notification := range client.updatesSnapshot() {
		if update := notification.Update.ConfigOptionUpdate; update != nil {
			reconciled = update.ConfigOptions
		}
	}
	if reconciled == nil {
		t.Fatalf("no config_option_update emitted; updates = %#v", client.updatesSnapshot())
	}
	requireConfigValues(t, reconciled, "deep", "max")

	// A subsequent read-back (resume of the active session) reports amp's truth,
	// not the originally requested smart/low.
	resumeResp, err := conn.ResumeSession(ctx, ResumeSessionRequest(newResp.SessionId, cwd))
	if err != nil {
		t.Fatalf("ResumeSession: %v", err)
	}
	requireConfigValues(t, resumeResp.ConfigOptions, "deep", "max")
}

// TestEffortDefaultOmittedWhenUnset pins R5-8: when the host does not set effort,
// the wrapper omits --effort entirely and lets amp choose its own default; the
// mode flag is still passed.
func TestEffortDefaultOmittedWhenUnset(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "")
	conn, _, cleanup := startTestServe(t,
		WithExecutablePath(path),
		WithHome(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	defer cleanup()
	ctx := context.Background()

	if _, err := conn.Initialize(ctx, acp.InitializeRequest{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	cwd := t.TempDir()
	newResp, err := conn.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, promptErr := conn.Prompt(ctx, acp.PromptRequest{
		SessionId: newResp.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock("hello")},
	}); promptErr != nil {
		t.Fatalf("Prompt: %v", promptErr)
	}

	argsRecords := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	var continueArgs []string
	for _, args := range argsRecords {
		if slices.Contains(args, "continue") && slices.Contains(args, string(newResp.SessionId)) {
			continueArgs = args
		}
	}
	if continueArgs == nil {
		t.Fatalf("no real continue invocation recorded: %#v", argsRecords)
	}
	if slices.Contains(continueArgs, "--effort") {
		t.Fatalf("--effort passed with no host-set effort: %#v", continueArgs)
	}
	if i := slices.Index(continueArgs, "-m"); i < 0 || i+1 >= len(continueArgs) || continueArgs[i+1] != "smart" {
		t.Fatalf("mode flag missing or not smart: %#v", continueArgs)
	}
}

// TestReconcileNativeConfigEmitFailureAbortsTurn covers the reconcile branch in
// the prompt loop: when the config_option_update carrying reconciled native
// mode/effort cannot be delivered, the turn aborts with the delivery error.
func TestReconcileNativeConfigEmitFailureAbortsTurn(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "reconcile-config")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	newResp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	agent.setConnection(newClosedAgentConnection(t))
	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "x")); promptErr == nil {
		t.Fatal("reconcile config update failure was ignored")
	}
}

func requireConfigValues(t *testing.T, options []acp.SessionConfigOption, wantMode, wantEffort string) {
	t.Helper()
	got := make(map[string]string, len(options))
	for _, option := range options {
		if option.Select == nil {
			continue
		}
		got[string(option.Select.Id)] = string(option.Select.CurrentValue)
	}
	if got[string(configMode)] != wantMode {
		t.Fatalf("mode current value = %q, want %q", got[string(configMode)], wantMode)
	}
	if got[string(configEffort)] != wantEffort {
		t.Fatalf("effort current value = %q, want %q", got[string(configEffort)], wantEffort)
	}
}
