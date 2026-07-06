package ampacp

import (
	"context"
	"strings"
	"testing"
)

func TestNewSessionFailsFastWithoutAPIKey(t *testing.T) {
	t.Setenv("AMP_API_KEY", "")
	agent := NewAgent(WithHome(t.TempDir()))
	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "AMP_API_KEY") {
		t.Fatalf("missing key error = %v", err)
	}
}

func TestNewSessionFailsFastWithEmptyAPIKeyOverride(t *testing.T) {
	t.Setenv("AMP_API_KEY", "process-key")
	agent := NewAgent(
		WithHome(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": ""}),
	)
	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "AMP_API_KEY") {
		t.Fatalf("empty override error = %v", err)
	}
}

func TestNewSessionAcceptsProcessEnvAPIKey(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if resp.SessionId == "" {
		t.Fatal("empty session id")
	}
}
