package ampacp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func newRateLimitsFixtureAgent(t *testing.T, options ...Option) *Agent {
	t.Helper()
	agent := NewAgent(append([]Option{WithEnv(map[string]string{"AMP_API_KEY": ""})}, options...)...)
	for _, id := range []acp.SessionId{"session-1", "😀: session "} {
		agent.sessions[id] = &agentSession{agent: agent, id: id, env: composeEnv(agent.options.Env), operationEnv: composeEnv(agent.options.Env), sessionEnv: map[string]string{}}
	}
	t.Cleanup(func() {
		agent.mu.Lock()
		clear(agent.sessions)
		agent.lifecycleContainmentErr = nil
		agent.mu.Unlock()
		require.NoError(t, agent.Close())
	})

	return agent
}

func TestRateLimitsProviderAndSessionSelection(t *testing.T) {
	t.Parallel()
	agent := newRateLimitsFixtureAgent(t)
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`{}`)} {
		result, err := agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, raw)
		require.NoError(t, err)
		require.Equal(t, rateLimitsUnavailable("amp", "session_required"), result)
	}
	result, err := agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"providerId":"unimplemented","sessionId":"session-1"}`))
	require.NoError(t, err)
	require.Equal(t, rateLimitsUnsupported("unimplemented"), result)
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"providerId":"unimplemented","sessionId":"absent"}`))
	requireRateLimitsRequestError(t, err, "unknown session", "sessionId")
	agent.deleted["session-1"] = struct{}{}
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"sessionId":"session-1"}`))
	requireRateLimitsRequestError(t, err, "unknown session", "sessionId")
	delete(agent.deleted, "session-1")
	session := agent.sessions["session-1"]
	session.closed = true
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"sessionId":"session-1"}`))
	requireRateLimitsRequestError(t, err, "unknown session", "sessionId")
	session.closed = false
	session.poisonCause = causeNativeIDDrift
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"sessionId":"session-1"}`))
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, -32603, requestErr.Code)
}

func TestRateLimitsCancellationAndClosedAgent(t *testing.T) {
	t.Parallel()
	agent := NewAgent()
	t.Cleanup(func() { require.NoError(t, agent.Close()) })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := agent.HandleExtensionMethod(ctx, RateLimitsMethod, json.RawMessage(`{"providerId":"amp"}`))
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, agent.sessions)
	require.NoError(t, agent.Close())
	_, err = agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, json.RawMessage(`{"providerId":"amp"}`))
	require.Error(t, err)
}
