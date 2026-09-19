//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	ampacp "github.com/savid/acp-go-amp"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestNativePersistence(t *testing.T) {
	requireIntegration(t)
	executable := harnessPath(t, false)
	env := nativeHome(t)
	store := acpcore.NewInMemorySessionStore()
	cwd := t.TempDir()
	a := ampacp.NewAgent(ampacp.WithEnv(env), ampacp.WithSessionStore(store), ampacp.WithExecutablePath(executable))
	t.Cleanup(func() { _ = a.Close() })
	session, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(session.SessionId), "T-"))
	require.NoError(t, a.Close())
	fresh := ampacp.NewAgent(ampacp.WithEnv(nativeHome(t)), ampacp.WithSessionStore(store), ampacp.WithExecutablePath(executable))
	t.Cleanup(func() { _ = fresh.Close() })
	_, err = fresh.LoadSession(t.Context(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
}

func TestNativeDeletedThreadRecovers(t *testing.T) {
	requireIntegration(t)
	executable := harnessPath(t, false)
	env := nativeHome(t)
	store := acpcore.NewInMemorySessionStore()
	options := []ampacp.Option{ampacp.WithEnv(env), ampacp.WithSessionStore(store), ampacp.WithExecutablePath(executable)}
	agent := ampacp.NewAgent(options...)
	t.Cleanup(func() { _ = agent.Close() })
	cwd := t.TempDir()
	session, err := agent.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	require.NoError(t, agent.Close())
	original := nativeSessionID(t, session.Meta)
	removeThread := func(id string) {
		command := exec.CommandContext(t.Context(), executable, "threads", "delete", id)
		command.Dir = cwd
		command.Env, err = (process.Environment{Process: os.Environ(), Agent: env}).Build()
		require.NoError(t, err)
		command.WaitDelay = 2 * time.Second
		output, commandErr := command.CombinedOutput()
		require.NoError(t, commandErr, "%s", output)
	}
	removeThread(original)
	fresh := ampacp.NewAgent(options...)
	t.Cleanup(func() { _ = fresh.Close() })
	loaded, err := fresh.LoadSession(t.Context(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	replacement := nativeSessionID(t, loaded.Meta)
	require.NotEqual(t, original, replacement)
	generation, err := store.Load(t.Context(), string(session.SessionId))
	require.NoError(t, err)
	var backup struct {
		ID       string            `json:"id"`
		Messages []json.RawMessage `json:"messages"`
	}
	require.NoError(t, json.Unmarshal(generation[""][0], &backup))
	require.Equal(t, replacement, backup.ID)
	require.Empty(t, backup.Messages)
	require.NoError(t, fresh.Close())
	resumed := ampacp.NewAgent(options...)
	t.Cleanup(func() { _ = resumed.Close() })
	response, err := resumed.ResumeSession(t.Context(), wire.ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Equal(t, loaded.Meta, response.Meta)
	require.NoError(t, resumed.Close())
	removeThread(replacement)
}

func TestNativeDeletedConversationRecovery(t *testing.T) {
	requireLive(t)
	executable := harnessPath(t, true)
	env, cwd := nativeHome(t), t.TempDir()
	store := acpcore.NewInMemorySessionStore()
	options := []ampacp.Option{ampacp.WithEnv(env), ampacp.WithSessionStore(store), ampacp.WithExecutablePath(executable)}
	h := newHarness(t, options...)
	h.initialize(withLifecycle())
	require.NoError(t, os.WriteFile(filepath.Join(cwd, "proof.txt"), []byte("copper-orbit-482\n"), 0600))
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, ampacp.WithSessionAmpOptions(ampacp.NewAmpOptions(ampacp.WithAmpMode(nativeMode())))))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "Use Bash to run cat proof.txt. Remember its exact contents and reply with them. Do not delegate.", promptMeta(1))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "copper-orbit-482")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	h.stop()
	require.NoError(t, os.Remove(filepath.Join(cwd, "proof.txt")))
	nativeEnv, err := (process.Environment{Process: os.Environ(), Agent: env}).Build()
	require.NoError(t, err)
	removeThread := func(id string) {
		command := exec.CommandContext(h.ctx(), executable, "threads", "delete", id)
		command.Dir = cwd
		command.Env = nativeEnv
		command.WaitDelay = 2 * time.Second
		output, commandErr := command.CombinedOutput()
		require.NoError(t, commandErr, "%s", output)
	}
	original := nativeSessionID(t, session.Meta)
	removeThread(original)
	restored := newHarness(t, options...)
	restored.initialize(withLifecycle())
	loaded, err := restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "copper-orbit-482")
	replacement := nativeSessionID(t, loaded.Meta)
	require.NotEqual(t, original, replacement)
	generation, err := store.Load(t.Context(), string(session.SessionId))
	require.NoError(t, err)
	var config struct {
		NativeSessionID string                     `json:"nativeSessionId"`
		Usage           map[string]json.RawMessage `json:"usage"`
	}
	require.NoError(t, json.Unmarshal(generation["config"][0], &config))
	require.Equal(t, replacement, config.NativeSessionID)
	require.NotEmpty(t, config.Usage)
	_, err = restored.conn.CloseSession(restored.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	restored.stop()
	command := exec.CommandContext(h.ctx(), executable, "--mode", nativeMode(), "threads", "continue", replacement, "--execute", "What exact text did you read from proof.txt earlier? Reply with that text only. Do not use tools.")
	command.Dir = cwd
	command.Env = nativeEnv
	command.WaitDelay = 2 * time.Second
	data, err := command.CombinedOutput()
	require.NoError(t, err, "%s", data)
	require.Contains(t, string(data), "copper-orbit-482")
	resumed := newHarness(t, options...)
	resumed.initialize(withLifecycle())
	response, err := resumed.conn.ResumeSession(resumed.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Equal(t, loaded.Meta, response.Meta)
	_, err = resumed.conn.CloseSession(resumed.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	resumed.stop()
	removeThread(replacement)
}

func TestNativeContinuation(t *testing.T) {
	requireLive(t)
	executable := harnessPath(t, true)
	env, cwd := nativeHome(t), t.TempDir()
	store := acpcore.NewInMemorySessionStore()
	opts := []ampacp.Option{ampacp.WithEnv(env), ampacp.WithSessionStore(store), ampacp.WithExecutablePath(executable)}
	h := newHarness(t, opts...)
	h.initialize(withLifecycle())
	mode := nativeMode()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, ampacp.WithSessionAmpOptions(ampacp.NewAmpOptions(ampacp.WithAmpMode(mode)))))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "Remember the project slug apricot-orbit. Reply with exactly apricot-orbit and nothing else. Do not use tools.", promptMeta(1))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "apricot-orbit")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	h.stop()
	command := exec.CommandContext(h.ctx(), executable, "--mode", mode, "threads", "continue", nativeSessionID(t, session.Meta), "--execute", "Remember the release label cobalt-lantern. Reply with the project slug and release label. Do not use tools.")
	command.Dir = cwd
	command.Env, err = (process.Environment{Process: os.Environ(), Agent: env}).Build()
	require.NoError(t, err)
	command.WaitDelay = 2 * time.Second
	var stderr bytes.Buffer
	command.Stderr = &stderr
	data, err := command.Output()
	require.NoError(t, err, "native continuation: %s", stderr.String())
	require.Contains(t, string(data), "apricot-orbit")
	require.Contains(t, string(data), "cobalt-lantern")
	restored := newHarness(t, opts...)
	restored.initialize(withLifecycle())
	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "cobalt-lantern")
	_, err = restored.prompt(session.SessionId, "What slug and release label did we choose? Do not use tools.", promptMeta(2))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "apricot-orbit")
	_, err = restored.conn.CloseSession(restored.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	restored.stop()
	fresh := newHarness(t, ampacp.WithEnv(nativeHome(t)), ampacp.WithSessionStore(store), ampacp.WithExecutablePath(executable))
	fresh.initialize(withLifecycle())
	_, err = fresh.conn.LoadSession(fresh.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	_, err = fresh.prompt(session.SessionId, "What slug and release label did we choose? Do not use tools.", promptMeta(3))
	require.NoError(t, err)
	require.Contains(t, agentText(fresh.rec.snapshot()), "apricot-orbit")
	require.Contains(t, agentText(fresh.rec.snapshot()), "cobalt-lantern")
}

func nativeMode() string {
	if value := os.Getenv("ACP_GO_AMP_MODE"); value != "" {
		return value
	}

	return "medium"
}

func nativeHome(t *testing.T) map[string]string {
	t.Helper()
	root := t.TempDir()
	env := map[string]string{}
	for key, dir := range map[string]string{"XDG_DATA_HOME": "data", "XDG_CONFIG_HOME": "config", "XDG_CACHE_HOME": "cache", "XDG_STATE_HOME": "state"} {
		env[key] = filepath.Join(root, dir)
	}
	env["AMP_SETTINGS_FILE"] = filepath.Join(env["XDG_CONFIG_HOME"], "amp", "settings.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(env["AMP_SETTINGS_FILE"]), 0o700))
	require.NoError(t, os.WriteFile(env["AMP_SETTINGS_FILE"], []byte(`{"amp.dangerouslyAllowAll":true,"amp.updates.mode":"disabled"}`), 0o600))
	source := os.Getenv("XDG_DATA_HOME")
	if source == "" {
		home, err := os.UserHomeDir()
		require.NoError(t, err)
		source = filepath.Join(home, ".local", "share")
	}
	// The secret store is shared by link, not copied: Amp rotates its refresh
	// token on use, so a copy would leave the developer's own login stale
	// after the first refresh.
	secrets := filepath.Join(source, "amp", "secrets.json")
	if _, err := os.Stat(secrets); err == nil {
		target := filepath.Join(env["XDG_DATA_HOME"], "amp")
		require.NoError(t, os.MkdirAll(target, 0o700))
		require.NoError(t, os.Symlink(secrets, filepath.Join(target, "secrets.json")))
	} else if !os.IsNotExist(err) {
		require.NoError(t, err)
	}

	return env
}

func TestNativePathCancellationAndDelete(t *testing.T) {
	requireLive(t)
	h := newHarness(t, ampacp.WithEnv(nativeHome(t)), ampacp.WithExecutablePath(harnessPath(t, true)))
	h.initialize(withLifecycle())
	cwd := t.TempDir()
	first, second := t.TempDir(), t.TempDir()
	for dir, text := range map[string]string{first: "path-one", second: "path-two"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "acpgogo_amp_probe"), []byte("#!/bin/sh\nprintf '"+text+"\\n'\nprintf 'ACP_PATH=%s\\n' \"$PATH\"\n"), 0o700))
	}
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, ampacp.WithSessionAmpOptions(ampacp.NewAmpOptions(ampacp.WithAmpMode(nativeMode()), ampacp.WithAmpExtraPathDirs(first)))))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "Use Bash to run acpgogo_amp_probe exactly by name. Reply with its output. Do not read or search files.", promptMeta(1))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "path-one")
	require.Contains(t, toolText(h.rec.snapshot()), "ACP_PATH="+first+string(os.PathListSeparator))
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd, ampacp.WithSessionAmpOptions(ampacp.NewAmpOptions(ampacp.WithAmpExtraPathDirs(second)))))
	require.NoError(t, err)
	beforeTools := toolText(h.rec.snapshot())
	_, err = h.prompt(session.SessionId, "Use Bash to run acpgogo_amp_probe again exactly by name. Reply with its output. Do not read or search files.", promptMeta(2))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "path-two")
	pathOutput := strings.TrimPrefix(toolText(h.rec.snapshot()), beforeTools)
	require.Contains(t, pathOutput, "ACP_PATH="+second+string(os.PathListSeparator))
	require.NotContains(t, pathOutput, first)
	done := make(chan error, 1)
	go func() {
		response, promptErr := h.prompt(session.SessionId, "Use Bash to run sleep 45 now.", promptMeta(3))
		if promptErr == nil && response.StopReason != acp.StopReasonCancelled {
			promptErr = fmt.Errorf("stop reason %s", response.StopReason)
		}
		done <- promptErr
	}()
	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		for _, u := range updates {
			if call := u.Update.ToolCall; call != nil {
				data, _ := json.Marshal(call.RawInput)
				if bytes.Contains(data, []byte("sleep 45")) {
					return true
				}
			}
		}

		return false
	})
	cancelledAt := time.Now()
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("cancel did not settle")
	}
	t.Logf("native cancellation and verified mirror completed in %s", time.Since(cancelledAt).Round(time.Millisecond))
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "ignored", promptMeta(4))
	require.Error(t, err)
}

func toolText(updates []acp.SessionNotification) string {
	var text strings.Builder
	for _, update := range updates {
		if tool := update.Update.ToolCallUpdate; tool != nil {
			for _, item := range tool.Content {
				if item.Content != nil && item.Content.Content.Text != nil {
					text.WriteString(item.Content.Content.Text.Text)
				}
			}
		}
	}

	return text.String()
}

func nativeSessionID(t *testing.T, meta map[string]any) string {
	t.Helper()
	binding, ok := meta["amp"].(map[string]any)
	require.True(t, ok)
	id, ok := binding["nativeSessionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}
