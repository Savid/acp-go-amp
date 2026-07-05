package ampacp_test

import (
	"context"
	"testing"

	"github.com/coder/acp-go-sdk"
	ampacp "github.com/savid/acp-go-amp"
)

func TestExampleInitialize(t *testing.T) {
	agent := ampacp.NewAgent()
	resp, err := agent.Initialize(context.Background(), acp.InitializeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.AgentCapabilities.LoadSession {
		t.Fatal("loadSession not advertised")
	}
	if resp.AgentCapabilities.Meta["amp"] == nil {
		t.Fatal("missing amp metadata")
	}
}
