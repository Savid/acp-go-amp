package ampacp

import (
	"context"
	"errors"
	"sync"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func acquireNativeRoot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeResourceKind) (func(), error) {
	return acquireRuntimeResource(ctx, hooks.AcquireNativeRoot, kind, "native root")
}

func reserveScratchRoot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeResourceKind) (func(), error) {
	return acquireRuntimeResource(ctx, hooks.ReserveScratchRoot, kind, "scratch root")
}

func nativeInternalError(err error) error {
	requestErr := acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nativeamp.ErrProcessContainmentIncomplete) {
		return errors.Join(requestErr, err)
	}

	return requestErr
}

func cleanupFailureClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, errAgentGoroutinePanic):
		return "callback_panic"
	case errors.Is(err, nativeamp.ErrProcessContainmentIncomplete):
		return "containment_incomplete"
	case errors.Is(err, context.Canceled):
		return authStateCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return "cleanup_failed"
	}
}

// beginAgentCall is the admission gate shared by every embedded ACP entry
// point. Agent.Close closes admission, cancels the returned context, and joins
// every call admitted before its publication. The finish callback records an
// incomplete-boundary sentinel before releasing the wait fence so Close and
// Serve cannot report completion falsely.
func (a *Agent) beginAgentCall(ctx context.Context, sessionIDs ...acp.SessionId) (context.Context, func(error), error) {
	if contextOwnsAgentCallback(ctx, a) {
		return nil, nil, closedCallbackRefusal()
	}

	if len(sessionIDs) > 0 && a.hasActiveCallbackForSession(sessionIDs[0]) {
		return nil, nil, closedCallbackRefusal()
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return nil, nil, a.ensureOpen()
	}

	shutdown := a.callShutdown
	a.nextCallGeneration++
	generation := &agentCallGeneration{agent: a, generation: a.nextCallGeneration}
	a.callWG.Add(1)
	a.mu.Unlock()

	operationCtx, cancel := context.WithCancel(ctx)

	operationCtx = withCallbackProvenance(operationCtx, a, generation)
	if len(sessionIDs) > 0 {
		operationCtx = withCallbackSessionScope(operationCtx, a, sessionIDs[0])
	}

	stopShutdown := make(chan struct{})

	go func() {
		select {
		case <-shutdown:
			cancel()
		case <-stopShutdown:
		}
	}()

	var once sync.Once

	finish := func(err error) {
		once.Do(func() {
			close(stopShutdown)
			cancel()

			if errors.Is(err, nativeamp.ErrProcessContainmentIncomplete) {
				a.mu.Lock()
				a.lifecycleContainmentErr = errors.Join(a.lifecycleContainmentErr, err)
				a.mu.Unlock()
			}

			a.callWG.Done()
		})
	}

	// Cleanup residences are bounded quarantine, not shutdown-only garbage.
	// Every ordinary admitted API gives healed local cleanup one retry while the
	// call itself owns the callback and shutdown join authority.
	a.retryCleanupResidences(operationCtx)

	return operationCtx, finish, nil
}

func acquireRuntimeResource(ctx context.Context, acquire func(context.Context, RuntimeResourceKind) (func(), error), kind RuntimeResourceKind, resource string) (func(), error) {
	if acquire == nil {
		return func() {}, nil
	}

	release, err := acquire(ctx, kind)
	if err != nil {
		return nil, err
	}

	if release == nil {
		return nil, errors.New(resource + " hook returned nil release")
	}

	var (
		mu       sync.Mutex
		released bool
	)

	return func() {
		mu.Lock()
		if released {
			mu.Unlock()

			return
		}

		released = true
		mu.Unlock()

		release()
	}, nil
}
