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
	mu         sync.Mutex
	hooks      RuntimeResourceHooks
	enabled    bool
	nextID     uint64
	roots      map[uint64]providerProcessRootSnapshot
	last       int
	set        bool
	publishing bool
	dirty      bool
}

type providerProcessRootSnapshot struct {
	inventory nativeamp.ProcessInventory
}

type providerProcessRootObservation struct {
	tracker *providerProcessSnapshotTracker
	id      uint64
}

func newProviderProcessSnapshotTracker(hooks RuntimeResourceHooks, enabled bool) *providerProcessSnapshotTracker {
	return &providerProcessSnapshotTracker{
		hooks:   hooks,
		enabled: enabled,
		roots:   make(map[uint64]providerProcessRootSnapshot),
	}
}

func (a *Agent) newProcessSnapshotObserver(ctx context.Context, inventory nativeamp.ProcessInventory) nativeamp.ProcessSnapshotObserver {
	root := a.providerProcesses.start(ctx, inventory)

	return nativeamp.ProcessSnapshotObserver{
		Refresh:    root.refresh,
		Complete:   root.complete,
		Incomplete: root.incomplete,
	}
}

func (t *providerProcessSnapshotTracker) start(ctx context.Context, inventory nativeamp.ProcessInventory) *providerProcessRootObservation {
	if t == nil || !t.enabled {
		return nil
	}

	t.mu.Lock()
	t.nextID++
	id := t.nextID
	t.roots[id] = providerProcessRootSnapshot{inventory: inventory}
	publish := t.markDirtyLocked()
	t.mu.Unlock()

	if publish {
		t.publishLoop(ctx)
	}

	return &providerProcessRootObservation{tracker: t, id: id}
}

func (o *providerProcessRootObservation) refresh(ctx context.Context) {
	if o == nil || o.tracker == nil {
		return
	}

	t := o.tracker
	t.mu.Lock()

	if _, ok := t.roots[o.id]; !ok {
		t.mu.Unlock()

		return
	}

	publish := t.markDirtyLocked()
	t.mu.Unlock()

	if publish {
		t.publishLoop(ctx)
	}
}

func (o *providerProcessRootObservation) complete(ctx context.Context) {
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
	publish := t.markDirtyLocked()
	t.mu.Unlock()

	if publish {
		t.publishLoop(ctx)
	}
}

func (o *providerProcessRootObservation) incomplete() {
	if o == nil || o.tracker == nil {
		return
	}

	t := o.tracker
	t.mu.Lock()
	publish := false

	if root, ok := t.roots[o.id]; ok {
		// Once authoritative containment is incomplete, a previous count is no
		// longer an absolute inventory. Retain the root as unknown so later
		// roots cannot manufacture a lower aggregate or a false zero.
		root.inventory = nil
		t.roots[o.id] = root
		publish = t.markDirtyLocked()
	}
	t.mu.Unlock()

	if publish {
		t.publishLoop(context.Background())
	}
}

func (t *providerProcessSnapshotTracker) markDirtyLocked() bool {
	t.dirty = true

	if t.publishing {
		return false
	}

	t.publishing = true

	return true
}

func (t *providerProcessSnapshotTracker) publishLoop(ctx context.Context) {
	for {
		t.mu.Lock()
		if !t.dirty {
			t.publishing = false
			t.mu.Unlock()

			return
		}

		t.dirty = false
		count, available := t.snapshotLocked()
		observe := t.hooks.ObserveProcessSnapshot

		publish := available && observe != nil && (!t.set || t.last != count)
		if publish {
			t.last = count
			t.set = true
		}
		t.mu.Unlock()

		if publish {
			observe(ctx, RuntimeProcessProviderDescendant, count)
		}
	}
}

func (t *providerProcessSnapshotTracker) snapshotLocked() (int, bool) {
	if len(t.roots) == 0 {
		return 0, true
	}

	total := 0

	for _, root := range t.roots {
		if root.inventory == nil {
			return 0, false
		}

		count, available := root.inventory()
		if !available || count < 0 {
			return 0, false
		}

		total += count
	}

	// A zero while roots remain registered has not crossed their authoritative
	// completion boundary. Only the empty tracker may publish zero.
	if total == 0 {
		return 0, false
	}

	return total, true
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
	externalContainment := hooks.ObserveContainment
	hooks.ObserveContainment = func(ctx context.Context, mode RuntimeContainmentMode) {
		observe.ObserveRuntimeContainment(ctx, string(mode))

		if externalContainment != nil {
			externalContainment(ctx, mode)
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
