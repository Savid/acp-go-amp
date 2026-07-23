package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

func TestSessionIDMintingAndEmptyNativeLifecycle(t *testing.T) {
	ctx := context.Background()

	id, err := newSessionID()
	if err != nil || len(id) != 36 || strings.HasPrefix(id, "T-") {
		t.Fatalf("minted session id = %q, %v", id, err)
	}

	previousRand := randRead
	t.Cleanup(func() { randRead = previousRand })
	randRead = func([]byte) (int, error) { return 0, errors.New("entropy failed") }
	if _, mintErr := newSessionID(); mintErr == nil || !strings.Contains(mintErr.Error(), "entropy failed") {
		t.Fatalf("entropy failure = %v", mintErr)
	}

	t.Setenv("AMP_API_KEY", "fake")
	agent := newTestAgent(WithScratchDir(t.TempDir()))
	agent.options.runtime.startupProbe = func(context.Context, *amp.Client) error { return nil }
	if _, newErr := agent.NewSession(ctx, NewSessionRequest(t.TempDir())); newErr == nil || !strings.Contains(newErr.Error(), "entropy failed") {
		t.Fatalf("NewSession entropy failure = %v", newErr)
	}
	randRead = previousRand

	if _, sessionErr := newAgentSession(ctx, agent, "", t.TempDir(), parsedSessionMeta{}, "", nil); sessionErr == nil {
		t.Fatal("empty session id accepted")
	}

	corruptStore := NewInMemorySessionStore()
	corruptMain := SessionKey{SessionID: "s-corrupt", Subpath: SessionStoreMainSubpath}
	if replaceErr := corruptStore.Replace(ctx, corruptMain, []SessionStoreReplacement{
		{Key: corruptMain, Entries: []SessionStoreEntry{json.RawMessage(`{"format":"wrong"}`)}},
	}); replaceErr != nil {
		t.Fatal(replaceErr)
	}
	corruptAgent := newTestAgent(WithSessionStore(corruptStore))
	manifest, stored, err := corruptAgent.storedManifest(ctx, "s-corrupt")
	if err != nil || !stored || manifest.NativeSessionID != "" {
		t.Fatalf("corrupt stored manifest = %#v stored=%t err=%v", manifest, stored, err)
	}
	if _, err := corruptAgent.UnstableDeleteSession(ctx, DeleteSessionRequest("s-corrupt")); err != nil {
		t.Fatalf("corrupt manifest delete: %v", err)
	}
	if entries, loadErr := corruptStore.Load(ctx, corruptMain); loadErr != nil || len(entries) != 0 {
		t.Fatalf("corrupt manifest row survived delete: entries=%d err=%v", len(entries), loadErr)
	}

	if err := corruptAgent.deleteNativeThread(ctx, "s-none", "", nil); err != nil {
		t.Fatalf("delete without native thread launched a process: %v", err)
	}

	session := &agentSession{agent: corruptAgent, id: "s-frames"}
	if err := session.validateFrameSessionID(ctx, fakeAmpMessage{}, nil); err != nil {
		t.Fatalf("frame without session id rejected: %v", err)
	}
}

func TestFrameSessionID(t *testing.T) {
	t.Parallel()

	require.Equal(t, "S1", frameSessionID(&amp.SystemMessage{SessionID: "S1"}))
	require.Equal(t, "U1", frameSessionID(&amp.UserMessage{SessionID: "U1"}))
	require.Equal(t, "A1", frameSessionID(&amp.AssistantMessage{SessionID: "A1"}))
	require.Equal(t, "R1", frameSessionID(&amp.ResultMessage{SessionID: "R1"}))
	require.Equal(t, "", frameSessionID(&amp.UnknownMessage{Type: "other"}))
}

func TestUnknownSessionErrorShapeCanonical(t *testing.T) {
	t.Parallel()

	var reqErr *acp.RequestError
	require.ErrorAs(t, unknownSessionError(), &reqErr)
	require.Equal(t, -32602, reqErr.Code)

	data, ok := reqErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, jsonFieldSessionID, data[jsonFieldField])
	require.Equal(t, "unknown session", data[jsonFieldError])
}
