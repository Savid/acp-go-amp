//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	ampacp "github.com/savid/acp-go-amp"
)

const envRunAttended = "ACP_GO_AMP_RUN_ATTENDED"

// requireAttended gates the attended tier. Once the gate is set a missing
// prerequisite is a hard failure rather than a skip: a silently green attended
// suite is worse than a red one.
func requireAttended(t *testing.T) string {
	t.Helper()

	requireIntegration(t)

	if os.Getenv(envRunAttended) != "1" {
		t.Skipf("set %s=1 to run the attended provider-auth tier", envRunAttended)
	}

	path, err := exec.LookPath("amp")
	if err != nil {
		t.Fatalf("%s=1 requires the amp binary: %v", envRunAttended, err)
	}

	return path
}

type authMethodsWire struct {
	Providers map[string][]struct {
		ID    string `json:"id"`
		Type  string `json:"type"`
		Label string `json:"label"`
	} `json:"providers"`
	Generation string `json:"generation"`
}

type authAuthorizeWire struct {
	Interaction   string `json:"interaction"`
	URL           string `json:"url"`
	Message       string `json:"message"`
	CallbackInput string `json:"callbackInput"`
	FlowID        string `json:"flowId"`
	FlowExpiresAt int64  `json:"flowExpiresAt"`
}

type authStatusWire struct {
	FlowID string `json:"flowId"`
	State  string `json:"state"`
	Reason string `json:"reason"`
}

type authCredentialWire struct {
	ConnectionID      string         `json:"connectionId"`
	Revision          int64          `json:"revision"`
	BindingGeneration int64          `json:"bindingGeneration"`
	Credential        map[string]any `json:"credential"`
}

type authInventoryWire struct {
	Entries []struct {
		ProviderID        string `json:"providerId"`
		ConnectionID      string `json:"connectionId"`
		BindingGeneration int64  `json:"bindingGeneration"`
		ProofSource       string `json:"proofSource"`
	} `json:"entries"`
}

func callAuthLeg(t *testing.T, ctx context.Context, conn *acp.ClientSideConnection, method string, params map[string]any, out any) error {
	t.Helper()

	raw, err := conn.CallExtension(ctx, method, params)
	if err != nil {
		return err
	}

	if out == nil {
		return nil
	}

	return json.Unmarshal(raw, out)
}

// TestAttendedProviderAuthLoginCompletes drives one real Amp login end to end.
// The operator opens the relayed URL, approves at Amp, and pastes the value
// back; the flow's effective deadline bounds the wait.
func TestAttendedProviderAuthLoginCompletes(t *testing.T) {
	ampPath := requireAttended(t)

	ctx, cancel := context.WithTimeout(context.Background(), 16*time.Minute)
	defer cancel()

	client := &recordingClient{}
	conn := serveLiveAgentForTest(t, ctx, client,
		ampacp.WithExecutablePath(ampPath),
		ampacp.WithProviderAuthRoot(t.TempDir()),
	)

	response, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}

	ampMeta, _ := response.AgentCapabilities.Meta["amp"].(map[string]any)

	capability, ok := ampMeta["providerAuth"].(map[string]any)
	if !ok {
		t.Fatalf("provider auth capability absent with a configured root: %#v", ampMeta)
	}

	names, _ := capability["methods"].([]any)
	if len(names) != 8 {
		t.Fatalf("advertised %d legs, want eight: %#v", len(names), names)
	}

	if _, injectable := capability["injectionKey"]; injectable {
		t.Fatalf("amp advertised an injection key: %#v", capability)
	}

	session, err := conn.NewSession(ctx, ampacp.NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("new session: %v", err)
	}

	sessionID := string(session.SessionId)

	var methods authMethodsWire
	if err := callAuthLeg(t, ctx, conn, "_amp/auth/methods", map[string]any{"sessionId": sessionID}, &methods); err != nil {
		t.Fatalf("_amp/auth/methods: %v", err)
	}

	entries := methods.Providers["amp"]
	if len(methods.Providers) != 1 || len(entries) != 2 {
		t.Fatalf("catalog = %#v, want hosted and manual account methods", methods.Providers)
	}

	hostedMethod := ""
	for _, entry := range entries {
		if entry.ID == "login" && entry.Type == "oauth" {
			hostedMethod = entry.ID
		}
	}
	if hostedMethod == "" {
		t.Fatalf("catalog has no hosted login: %#v", entries)
	}

	var authorization authAuthorizeWire

	err = callAuthLeg(t, ctx, conn, "_amp/auth/authorize", map[string]any{
		"sessionId":          sessionID,
		"providerId":         "amp",
		"connectionId":       "attended-connection",
		"methodsGeneration":  methods.Generation,
		"method":             hostedMethod,
		"authorizeRequestId": "attended-request",
	}, &authorization)
	if err != nil {
		t.Fatalf("_amp/auth/authorize: %v", err)
	}

	if authorization.Interaction != "callback" || authorization.CallbackInput != "code" || authorization.URL == "" {
		t.Fatalf("authorize returned no paste-back presentation: %#v", authorization)
	}

	t.Logf("open %s and approve, then copy the code Amp shows", authorization.URL)

	pasted := attendedAnswer(t, "paste the Amp login code and press enter: ")

	if err := callAuthLeg(t, ctx, conn, "_amp/auth/callback", map[string]any{
		"sessionId": sessionID, "providerId": "amp",
		"method": hostedMethod, "flowId": authorization.FlowID, "input": pasted,
	}, nil); err != nil {
		t.Fatalf("_amp/auth/callback: %v", err)
	}

	var status authStatusWire
	if err := callAuthLeg(t, ctx, conn, "_amp/auth/status", map[string]any{
		"sessionId": sessionID, "providerId": "amp", "flowId": authorization.FlowID,
	}, &status); err != nil {
		t.Fatalf("_amp/auth/status: %v", err)
	}

	if status.State != "authenticated" {
		t.Fatalf("flow reached %q/%q rather than authenticated", status.State, status.Reason)
	}

	var inventory authInventoryWire
	if err := callAuthLeg(t, ctx, conn, "_amp/auth/inventory", map[string]any{"sessionId": sessionID}, &inventory); err != nil {
		t.Fatalf("_amp/auth/inventory: %v", err)
	}

	if len(inventory.Entries) != 1 || inventory.Entries[0].ProofSource != "confirmed_present" {
		t.Fatalf("inventory = %#v", inventory.Entries)
	}

	var harvest authCredentialWire
	if err := callAuthLeg(t, ctx, conn, "_amp/auth/credential", map[string]any{
		"sessionId": sessionID, "providerId": "amp", "flowId": authorization.FlowID,
	}, &harvest); err != nil {
		t.Fatalf("_amp/auth/credential: %v", err)
	}

	key, _ := harvest.Credential["key"].(string)
	if harvest.Credential["type"] != "api" || key == "" {
		t.Fatalf("harvest = %#v", harvest.Credential)
	}

	if _, unexpected := harvest.Credential["metadata"]; unexpected {
		t.Fatalf("the account key carried metadata: %#v", harvest.Credential)
	}

	// The key is harvested at most once per flow.
	if err := callAuthLeg(t, ctx, conn, "_amp/auth/credential", map[string]any{
		"sessionId": sessionID, "providerId": "amp", "flowId": authorization.FlowID,
	}, nil); err == nil {
		t.Fatal("a second harvest succeeded")
	}

	if err := callAuthLeg(t, ctx, conn, "_amp/auth/disconnect", map[string]any{
		"sessionId": sessionID, "providerId": "amp",
		"connectionId": "attended-connection", "bindingGeneration": harvest.BindingGeneration,
	}, nil); err != nil {
		t.Fatalf("_amp/auth/disconnect: %v", err)
	}
}

// attendedAnswer blocks on the operator. An unanswered prompt is a failure, not
// a skip.
func attendedAnswer(t *testing.T, prompt string) string {
	t.Helper()

	fmt.Fprint(os.Stderr, prompt)

	answer := make(chan string, 1)

	go func() {
		reader := bufio.NewReader(os.Stdin)

		line, err := reader.ReadString('\n')
		if err != nil {
			close(answer)

			return
		}

		answer <- strings.TrimSpace(line)
	}()

	select {
	case value, ok := <-answer:
		if !ok || value == "" {
			t.Fatal("the attended tier received no answer")
		}

		return value
	case <-time.After(10 * time.Minute):
		t.Fatal("the attended tier timed out waiting for a human answer")

		return ""
	}
}
