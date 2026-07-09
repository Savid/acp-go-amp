package ampacp

import (
	"context"

	"github.com/coder/acp-go-sdk"
)

func (a *Agent) setConnection(conn *acp.AgentSideConnection) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
}

func (a *Agent) connection() *acp.AgentSideConnection {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.conn
}

func (a *Agent) acquireClientCall(ctx context.Context) (func(), error) {
	select {
	case a.clientCalls <- struct{}{}:
		return func() { <-a.clientCalls }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, backpressureError("client_calls")
	}
}

func backpressureError(limit string) error {
	return acp.NewInvalidRequest(map[string]any{jsonFieldError: "backpressure", "limit": limit})
}
