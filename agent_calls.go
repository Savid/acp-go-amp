package ampacp

import (
	"context"
	"errors"
	"sync"

	"github.com/coder/acp-go-sdk"
)

func nativeInternalError(err error) error {
	requestErr := acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		containmentIncomplete(err) ||
		errors.Is(err, ErrHostAuthorityUnavailable) || errors.Is(err, ErrNativeTreeBusy) {
		return errors.Join(requestErr, publicContainmentError(err))
	}

	return requestErr
}

func cleanupFailureClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, errAgentGoroutinePanic):
		return "callback_panic"
	case containmentIncomplete(err):
		return "containment_incomplete"
	case errors.Is(err, context.Canceled):
		return authStateCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return "cleanup_failed"
	}
}

//nolint:gocritic // The deferred call must rewrite the named return error before it reaches the caller.
func finishPublicCall(err *error, finish func(error)) {
	*err = publicContainmentError(*err)
	finish(*err)
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

			if containmentIncomplete(err) {
				err = publicContainmentError(err)

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
