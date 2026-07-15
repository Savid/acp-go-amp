package ampacp

import (
	"context"
	"errors"
	"sync"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func acquireNativeRoot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeResourceKind) (func(), error) {
	return acquireRuntimeResource(ctx, hooks.AcquireNativeRoot, kind, "native root")
}

func reserveScratchRoot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeResourceKind) (func(), error) {
	return acquireRuntimeResource(ctx, hooks.ReserveScratchRoot, kind, "scratch root")
}

func releaseNativeRootWhenQuiescent(release func(), err error) {
	if nativeamp.ProcessTreeQuiescent(err) {
		release()
	}
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
