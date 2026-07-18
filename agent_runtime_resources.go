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
	if errors.Is(err, nativeamp.ErrProcessContainmentIncomplete) {
		return errors.Join(requestErr, err)
	}

	return requestErr
}

// beginLifecycleOperation registers a request that may own a native process
// before an active session exists. Agent.Close closes admission, cancels the
// returned context, and waits for every registered operation before it snapshots
// sessions. The finish callback records an incomplete-boundary sentinel before
// releasing the wait fence so Close and Serve cannot report completion falsely.
func (a *Agent) beginLifecycleOperation(ctx context.Context) (context.Context, func(error), error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return nil, nil, a.ensureOpen()
	}

	shutdown := a.lifecycleDone
	a.lifecycleWG.Add(1)
	a.mu.Unlock()

	operationCtx, cancel := context.WithCancel(ctx)
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

			a.lifecycleWG.Done()
		})
	}

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

	var once sync.Once

	return func() { once.Do(release) }, nil
}
