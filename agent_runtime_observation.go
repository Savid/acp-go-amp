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
	revision   uint64
	roots      map[uint64]providerProcessRootSnapshot
	last       int
	set        bool
	publishing bool
	dirty      bool
}

type providerProcessRootSnapshot struct {
	generation uint64
	context    func() context.Context
	inventory  nativeamp.ProcessInventory
}

type providerProcessRootObservation struct {
	tracker    *providerProcessSnapshotTracker
	id         uint64
	generation uint64
}

func newProviderProcessSnapshotTracker(hooks RuntimeResourceHooks, enabled bool) *providerProcessSnapshotTracker {
	return &providerProcessSnapshotTracker{
		hooks:   hooks,
		enabled: enabled,
		roots:   make(map[uint64]providerProcessRootSnapshot),
	}
}

func (a *Agent) newProcessSnapshotObserver(ctx context.Context, inventory nativeamp.ProcessInventory) nativeamp.ProcessSnapshotObserver {
	leave := enterExternalCallback(ctx)
	defer leave()

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
	t.revision++
	generation := t.revision
	observation := &providerProcessRootObservation{tracker: t, id: id, generation: generation}

	rootCtx := ctx
	if agent := callbackAgent(ctx); agent != nil {
		rootCtx = withCallbackProvenance(ctx, agent, observation)
	}

	t.roots[id] = providerProcessRootSnapshot{
		generation: generation,
		context:    func() context.Context { return rootCtx },
		inventory:  inventory,
	}
	publish := t.markDirtyLocked()
	t.mu.Unlock()

	if publish {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			t.mu.Lock()

			root, current := t.roots[id]
			if current && root.generation == generation {
				delete(t.roots, id)
				t.revision++
			}

			republish := t.markDirtyLocked()
			t.mu.Unlock()

			if republish {
				go t.publishSuccessor(rootCtx)
			}

			panic(recovered)
		}()

		t.publishLoop(rootCtx)
	}

	return observation
}

func (o *providerProcessRootObservation) refresh(ctx context.Context) {
	if o == nil || o.tracker == nil {
		return
	}

	t := o.tracker
	t.mu.Lock()

	root, ok := t.roots[o.id]
	if !ok || root.generation != o.generation {
		t.mu.Unlock()

		return
	}

	rootCtx := root.context()
	retainedAgent := callbackAgent(rootCtx)

	refreshAgent := callbackAgent(ctx)
	if retainedAgent == nil && refreshAgent != nil {
		rootCtx = withCallbackProvenance(ctx, refreshAgent, o)
	} else if retainedAgent != nil && retainedAgent == refreshAgent {
		rootCtx = withCallbackProvenance(ctx, retainedAgent, o)
	}

	root.context = func() context.Context { return rootCtx }
	t.roots[o.id] = root
	t.revision++

	publish := t.markDirtyLocked()
	t.mu.Unlock()

	if publish {
		t.publishLoop(rootCtx)
	}
}

func (o *providerProcessRootObservation) complete(ctx context.Context) {
	if o == nil || o.tracker == nil {
		return
	}

	t := o.tracker
	t.mu.Lock()

	root, ok := t.roots[o.id]
	if !ok || root.generation != o.generation {
		t.mu.Unlock()

		return
	}

	rootCtx := root.context()

	delete(t.roots, o.id)
	t.revision++
	publish := t.markDirtyLocked()
	t.mu.Unlock()

	if publish {
		t.publishLoop(rootCtx)
	}
}

func (o *providerProcessRootObservation) incomplete() {
	if o == nil || o.tracker == nil {
		return
	}

	t := o.tracker
	t.mu.Lock()
	publish := false
	ctx := context.Background()

	if root, ok := t.roots[o.id]; ok && root.generation == o.generation {
		// Once authoritative containment is incomplete, a previous count is no
		// longer an absolute inventory. Retain the root as unknown so later
		// roots cannot manufacture a lower aggregate or a false zero.
		root.inventory = nil
		t.roots[o.id] = root
		ctx = root.context()
		t.revision++
		publish = t.markDirtyLocked()
	}
	t.mu.Unlock()

	if publish {
		t.publishLoop(ctx)
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
	completed := false
	defer func() {
		if completed {
			return
		}

		recovered := recover()

		t.mu.Lock()
		pending := t.dirty
		t.dirty = true

		t.set = false
		if pending {
			t.publishing = true
		} else {
			t.publishing = false
		}
		t.mu.Unlock()

		if pending {
			go t.publishSuccessor(ctx)
		}

		if recovered != nil {
			panic(recovered)
		}
	}()

	for {
		t.mu.Lock()
		if !t.dirty {
			t.publishing = false
			completed = true
			t.mu.Unlock()

			return
		}

		t.dirty = false
		revision := t.revision

		roots := make([]providerProcessRootSnapshot, 0, len(t.roots))
		for _, root := range t.roots {
			roots = append(roots, root)
		}

		observe := t.hooks.ObserveProcessSnapshot
		t.mu.Unlock()

		count, available := snapshotProviderProcessRoots(roots)

		t.mu.Lock()
		if t.revision != revision {
			t.dirty = true
			t.mu.Unlock()

			continue
		}

		publish := available && observe != nil && (!t.set || t.last != count)
		if publish {
			t.last = count
			t.set = true
		}
		t.mu.Unlock()

		if publish {
			invokeExternal(ctx, func() {
				observe(ctx, RuntimeProcessProviderDescendant, count)
			})
		}
	}
}

func (t *providerProcessSnapshotTracker) publishSuccessor(ctx context.Context) {
	defer func() {
		if recover() == nil {
			return
		}

		t.mu.Lock()
		t.publishing = false
		t.dirty = true
		t.set = false
		t.mu.Unlock()
	}()

	t.publishLoop(ctx)
}

func snapshotProviderProcessRoots(roots []providerProcessRootSnapshot) (int, bool) {
	if len(roots) == 0 {
		return 0, true
	}

	total := 0

	for _, root := range roots {
		if root.inventory == nil {
			return 0, false
		}

		count, available := invokeExternalPair(root.context(), root.inventory)

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
			resourceCtx := withExactCallbackGeneration(ctx, "runtime_resource:"+resource)

			var (
				release func()
				err     error
			)

			if acquire == nil {
				release = func() {}
			} else {
				release, err = invokeExternalPair(resourceCtx, func() (func(), error) {
					return acquire(resourceCtx, lifecycle)
				})
			}

			if err != nil || release == nil {
				observe.RecordRuntimeResourceAdmission(ctx, resource, string(lifecycle), "rejected")

				return release, err
			}

			observe.RecordRuntimeResourceAdmission(ctx, resource, string(lifecycle), "admitted")
			observe.AddRuntimeResource(ctx, resource, 1)

			var (
				releaseMu sync.Mutex
				released  bool
			)

			return func() {
				releaseMu.Lock()
				if released {
					releaseMu.Unlock()

					return
				}

				released = true
				releaseMu.Unlock()

				defer observe.AddRuntimeResource(context.Background(), resource, -1)

				invokeExternal(resourceCtx, release)
			}, nil
		}
	}
	hooks.AcquireNativeRoot = wrapAcquire("managed_native_root", hooks.AcquireNativeRoot)
	hooks.ReserveScratchRoot = wrapAcquire("adapter_scratch_root", hooks.ReserveScratchRoot)

	externalProcess := hooks.ObserveProcess
	hooks.ObserveProcess = func(ctx context.Context, kind RuntimeProcessKind, delta int64) {
		observe.AddRuntimeProcess(ctx, string(kind), delta)

		if externalProcess != nil {
			callbackCtx := withExactCallbackGeneration(ctx, "runtime_observer:process")
			invokeExternal(callbackCtx, func() { externalProcess(callbackCtx, kind, delta) })
		}
	}
	externalSnapshot := hooks.ObserveProcessSnapshot
	hooks.ObserveProcessSnapshot = func(ctx context.Context, kind RuntimeProcessKind, count int) {
		observe.SetRuntimeProcess(ctx, string(kind), count)

		if externalSnapshot != nil {
			callbackCtx := withExactCallbackGeneration(ctx, "runtime_observer:snapshot")
			invokeExternal(callbackCtx, func() { externalSnapshot(callbackCtx, kind, count) })
		}
	}
	externalStage := hooks.ObserveStartupStage
	hooks.ObserveStartupStage = func(ctx context.Context, lifecycle RuntimeResourceKind, stage RuntimeStartupStage, elapsed time.Duration, err error) {
		observe.ObserveRuntimeStartupStage(ctx, string(lifecycle), string(stage), elapsed, err)

		if externalStage != nil {
			callbackCtx := withExactCallbackGeneration(ctx, "runtime_observer:startup_stage")
			invokeExternal(callbackCtx, func() { externalStage(callbackCtx, lifecycle, stage, elapsed, err) })
		}
	}
	externalContainment := hooks.ObserveContainment
	hooks.ObserveContainment = func(ctx context.Context, mode RuntimeContainmentMode) {
		observe.ObserveRuntimeContainment(ctx, string(mode))

		if externalContainment != nil {
			callbackCtx := withExactCallbackGeneration(ctx, "runtime_observer:containment")
			invokeExternal(callbackCtx, func() { externalContainment(callbackCtx, mode) })
		}
	}

	return hooks
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
