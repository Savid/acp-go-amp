package ampacp

import (
	"context"
	"sync"
	"time"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/observer"
)

// providerProcessSnapshotTracker owns the agent-wide absolute provider
// descendant count. A local process-tree count is not an absolute agent count:
// every active root must be known before the aggregate can be published.
type providerProcessSnapshotTracker struct {
	mu     sync.Mutex
	hooks  RuntimeResourceHooks
	nextID uint64
	roots  map[uint64]providerProcessRootSnapshot
	last   int
	set    bool
}

type providerProcessRootSnapshot struct {
	known bool
	count int
}

type providerProcessRootObservation struct {
	tracker *providerProcessSnapshotTracker
	id      uint64
}

func newProviderProcessSnapshotTracker(hooks RuntimeResourceHooks) *providerProcessSnapshotTracker {
	return &providerProcessSnapshotTracker{
		hooks: hooks,
		roots: make(map[uint64]providerProcessRootSnapshot),
	}
}

func (a *Agent) newProcessSnapshotObserver(ctx context.Context) nativeamp.ProcessSnapshotObserver {
	root := a.providerProcesses.start(ctx)

	return nativeamp.ProcessSnapshotObserver{
		Observe:   root.snapshot,
		Quiescent: root.quiescent,
		Unproven:  root.unproven,
	}
}

func (t *providerProcessSnapshotTracker) start(context.Context) *providerProcessRootObservation {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	t.nextID++
	id := t.nextID
	t.roots[id] = providerProcessRootSnapshot{}
	t.mu.Unlock()

	return &providerProcessRootObservation{tracker: t, id: id}
}

func (o *providerProcessRootObservation) snapshot(ctx context.Context, count int) {
	if o == nil || o.tracker == nil || count < 0 {
		return
	}

	t := o.tracker
	t.mu.Lock()

	root, ok := t.roots[o.id]
	if !ok {
		t.mu.Unlock()

		return
	}

	root.known = true
	root.count = count
	t.roots[o.id] = root
	t.publishKnownLocked(ctx)
	t.mu.Unlock()
}

func (o *providerProcessRootObservation) quiescent(ctx context.Context) {
	if o == nil || o.tracker == nil {
		return
	}

	t := o.tracker
	t.mu.Lock()
	if _, ok := t.roots[o.id]; !ok {
		t.mu.Unlock()

		return
	}

	delete(t.roots, o.id)

	if len(t.roots) == 0 {
		t.publishLocked(ctx, 0)
	} else {
		t.publishKnownLocked(ctx)
	}
	t.mu.Unlock()
}

func (o *providerProcessRootObservation) unproven() {
	if o == nil || o.tracker == nil {
		return
	}

	t := o.tracker
	t.mu.Lock()
	if root, ok := t.roots[o.id]; ok {
		// Once quiescence is unproven, even a previously observed count is no
		// longer an absolute inventory. Retain the root as unknown so later
		// roots cannot manufacture a lower aggregate or a false zero.
		root.known = false
		t.roots[o.id] = root
	}
	t.mu.Unlock()
}

func (t *providerProcessSnapshotTracker) publishKnownLocked(ctx context.Context) {
	total := 0

	for _, root := range t.roots {
		if !root.known {
			return
		}

		total += root.count
	}

	t.publishLocked(ctx, total)
}

func (t *providerProcessSnapshotTracker) publishLocked(ctx context.Context, count int) {
	if t.set && t.last == count {
		return
	}

	t.last = count
	t.set = true
	observeRuntimeProcessSnapshot(ctx, t.hooks, RuntimeProcessProviderDescendant, count)
}

func instrumentRuntimeResourceHooks(hooks RuntimeResourceHooks, observe *observer.Observer) RuntimeResourceHooks {
	wrapAcquire := func(resource string, acquire func(context.Context, RuntimeResourceKind) (func(), error)) func(context.Context, RuntimeResourceKind) (func(), error) {
		return func(ctx context.Context, lifecycle RuntimeResourceKind) (func(), error) {
			var (
				release func()
				err     error
			)

			if acquire == nil {
				release = func() {}
			} else {
				release, err = acquire(ctx, lifecycle)
			}

			if err != nil || release == nil {
				observe.RecordRuntimeResourceAdmission(ctx, resource, string(lifecycle), "rejected")

				return release, err
			}

			observe.RecordRuntimeResourceAdmission(ctx, resource, string(lifecycle), "admitted")
			observe.AddRuntimeResource(ctx, resource, 1)

			var once sync.Once

			return func() {
				once.Do(func() {
					release()
					observe.AddRuntimeResource(context.Background(), resource, -1)
				})
			}, nil
		}
	}
	hooks.AcquireNativeRoot = wrapAcquire("managed_native_root", hooks.AcquireNativeRoot)
	hooks.ReserveScratchRoot = wrapAcquire("adapter_scratch_root", hooks.ReserveScratchRoot)

	externalProcess := hooks.ObserveProcess
	hooks.ObserveProcess = func(ctx context.Context, kind RuntimeProcessKind, delta int64) {
		observe.AddRuntimeProcess(ctx, string(kind), delta)

		if externalProcess != nil {
			externalProcess(ctx, kind, delta)
		}
	}
	externalSnapshot := hooks.ObserveProcessSnapshot
	hooks.ObserveProcessSnapshot = func(ctx context.Context, kind RuntimeProcessKind, count int) {
		observe.SetRuntimeProcess(ctx, string(kind), count)

		if externalSnapshot != nil {
			externalSnapshot(ctx, kind, count)
		}
	}
	externalStage := hooks.ObserveStartupStage
	hooks.ObserveStartupStage = func(ctx context.Context, lifecycle RuntimeResourceKind, stage RuntimeStartupStage, elapsed time.Duration, err error) {
		observe.ObserveRuntimeStartupStage(ctx, string(lifecycle), string(stage), elapsed, err)

		if externalStage != nil {
			externalStage(ctx, lifecycle, stage, elapsed, err)
		}
	}

	return hooks
}

func observeRuntimeProcess(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeProcessKind, delta int64) {
	if hooks.ObserveProcess != nil && delta != 0 {
		hooks.ObserveProcess(ctx, kind, delta)
	}
}

func observeRuntimeProcessSnapshot(ctx context.Context, hooks RuntimeResourceHooks, kind RuntimeProcessKind, count int) {
	if hooks.ObserveProcessSnapshot != nil && count >= 0 {
		hooks.ObserveProcessSnapshot(ctx, kind, count)
	}
}

func observeRuntimeStartupStage(
	ctx context.Context,
	hooks RuntimeResourceHooks,
	lifecycle RuntimeResourceKind,
	stage RuntimeStartupStage,
	started time.Time,
	err error,
) {
	if hooks.ObserveStartupStage != nil {
		hooks.ObserveStartupStage(ctx, lifecycle, stage, time.Since(started), err)
	}
}
