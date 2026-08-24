package ampacp

import (
	"context"
	"errors"
	"log/slog"
)

var errAgentGoroutinePanic = errors.New("agent goroutine panic")

// recoverAgentGoroutine is the deferred panic guard for agent-owned goroutines.
// It must be the deferred function itself so recover() observes the goroutine's
// panic.
func recoverAgentGoroutine(ctx context.Context, log *slog.Logger, name string) {
	handleAgentGoroutinePanic(ctx, log, name, nil, recover())
}

func handleAgentGoroutinePanic(ctx context.Context, log *slog.Logger, name string, shutdown func(any), recovered any) {
	if recovered == nil {
		return
	}

	if log == nil {
		log = slog.Default()
	}

	log.ErrorContext(ctx, "agent goroutine panic",
		slog.String("classification", "panic"),
		slog.String("goroutine", name),
	)

	if shutdown != nil {
		shutdown(errAgentGoroutinePanic)
	}
}

func agentLogger(agent *Agent) *slog.Logger {
	if agent == nil {
		return nil
	}

	return agent.log
}

// onNativeGoroutinePanic is the panic handler handed to the internal amp
// process boundary so a panic in a native-turn goroutine is recovered and
// logged instead of crashing the process.
func (a *Agent) onNativeGoroutinePanic(ctx context.Context, name string, recovered any) {
	ctx = withExactCallbackGeneration(ctx, "native:goroutine_panic")

	leave := enterExternalCallback(ctx)
	defer leave()

	handleAgentGoroutinePanic(ctx, agentLogger(a), name, nil, recovered)
}
