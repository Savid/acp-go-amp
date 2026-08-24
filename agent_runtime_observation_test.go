package ampacp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/savid/acp-go-amp/internal/observer"
	"github.com/stretchr/testify/require"
)

func TestProviderProcessSnapshotTrackerAggregatesProvenRoots(t *testing.T) {
	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, kind RuntimeProcessKind, count int) {
			require.Equal(t, RuntimeProcessProviderDescendant, kind)
			snapshots = append(snapshots, count)
		},
	}, true)

	firstCount := 2
	first := tracker.start(t.Context(), func() (int, bool) { return firstCount, true })
	first.refresh(t.Context())
	secondCount := 3
	second := tracker.start(t.Context(), func() (int, bool) { return secondCount, true })
	second.refresh(t.Context())

	// Every boundary re-queries every active root. A cached firstCount=2
	// would incorrectly publish 5 here after the live inventory became 4.
	firstCount = 4
	second.refresh(t.Context())
	first.complete(t.Context())
	second.complete(t.Context())

	require.Equal(t, []int{2, 5, 7, 3, 0}, snapshots)
}

func TestProviderProcessSnapshotTrackerIncompleteRootPreservesLastNonzero(t *testing.T) {
	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	}, true)

	incomplete := tracker.start(t.Context(), func() (int, bool) { return 4, true })
	incomplete.refresh(t.Context())
	incomplete.incomplete()

	other := tracker.start(t.Context(), func() (int, bool) { return 1, true })
	other.refresh(t.Context())
	other.complete(t.Context())

	require.Equal(t, []int{4}, snapshots, "incomplete containment must suppress lower totals and zero")
}

func TestProviderProcessSnapshotRetainsRootProvenanceAcrossBackgroundRefreshAndCompletion(t *testing.T) {
	agent := newTestAgent()
	var closeErr error
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			if count == 0 {
				closeErr = agent.Close()
			}
		},
	}, true)
	rootCtx := withCallbackProvenance(t.Context(), agent, &agentCallbackGeneration{generation: 1, kind: "root"})
	root := tracker.start(rootCtx, func() (int, bool) { return 1, true })

	root.refresh(context.Background())
	root.complete(context.Background())

	requireClosedCallbackRefusal(t, closeErr)
	require.NoError(t, agent.Close())
}

func TestProviderProcessSnapshotAdoptsAndRefreshesExactRootProvenance(t *testing.T) {
	agent := newTestAgent()
	var callbackContexts []context.Context
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(ctx context.Context, _ RuntimeProcessKind, _ int) {
			callbackContexts = append(callbackContexts, ctx)
		},
	}, true)
	root := tracker.start(context.Background(), func() (int, bool) { return 1, true })
	rootCtx := withCallbackProvenance(t.Context(), agent, &agentCallbackGeneration{generation: 1, kind: "root"})

	root.refresh(rootCtx)
	root.refresh(rootCtx)
	root.complete(rootCtx)

	require.Len(t, callbackContexts, 2)
	for _, ctx := range callbackContexts[1:] {
		require.True(t, contextOwnsAgentCallback(ctx, agent))
	}
	require.NoError(t, agent.Close())
}

func TestProviderProcessSnapshotTrackerConcurrentLifecycle(t *testing.T) {
	const roots = 32

	available := false
	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	}, true)
	observations := make([]*providerProcessRootObservation, roots)
	for i := range observations {
		observations[i] = tracker.start(context.Background(), func() (int, bool) { return 1, available })
	}
	available = true

	var group sync.WaitGroup
	for _, observation := range observations {
		group.Add(1)
		go func() {
			defer group.Done()
			observation.refresh(context.Background())
		}()
	}
	group.Wait()

	for _, observation := range observations {
		group.Add(1)
		go func() {
			defer group.Done()
			observation.complete(context.Background())
		}()
	}
	group.Wait()

	require.NotEmpty(t, snapshots)
	require.Equal(t, roots, snapshots[0])
	require.Equal(t, 0, snapshots[len(snapshots)-1])
}

func TestProviderProcessSnapshotTrackerHookReentryPublishesFreshAggregate(t *testing.T) {
	var snapshots []int
	var reentered bool
	var second *providerProcessRootObservation

	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{}, true)
	tracker.hooks.ObserveProcessSnapshot = func(ctx context.Context, _ RuntimeProcessKind, count int) {
		snapshots = append(snapshots, count)
		if !reentered {
			reentered = true
			second = tracker.start(ctx, func() (int, bool) { return 3, true })
		}
	}

	first := tracker.start(t.Context(), func() (int, bool) { return 2, true })
	require.NotNil(t, first)
	require.NotNil(t, second)
	require.Equal(t, []int{2, 5}, snapshots)
}

func TestProviderProcessSnapshotPanicSchedulesOneDirtySuccessor(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	final := make(chan struct{})
	var (
		mu        sync.Mutex
		attempted []int
		published []int
		finalOnce sync.Once
	)
	count := 1
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, got int) {
			mu.Lock()
			attempted = append(attempted, got)
			mu.Unlock()
			if got == 2 {
				close(entered)
				awaitCorrectionCallback(t, release, "snapshot panic release")
				panic("snapshot publication panic")
			}

			mu.Lock()
			published = append(published, got)
			mu.Unlock()
			if got == 0 {
				finalOnce.Do(func() { close(final) })
			}
		},
	}, true)
	root := tracker.start(t.Context(), func() (int, bool) { return count, true })
	count = 2
	recovered := make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		root.refresh(t.Context())
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot callback did not reach the panic barrier")
	}
	root.complete(t.Context())
	close(release)
	select {
	case got := <-recovered:
		require.Equal(t, "snapshot publication panic", got)
	case <-time.After(2 * time.Second):
		t.Fatal("panicking publisher did not unwind")
	}
	select {
	case <-final:
	case <-time.After(2 * time.Second):
		t.Fatal("dirty completion edge was not published by a successor")
	}

	require.Eventually(t, func() bool {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()

		return !tracker.publishing && !tracker.dirty
	}, 2*time.Second, time.Millisecond, "successor publisher did not retire")
	mu.Lock()
	require.Equal(t, []int{1, 2, 0}, attempted)
	require.Equal(t, []int{1, 0}, published)
	mu.Unlock()
}

func TestProviderProcessSnapshotInitialPanicReclaimsUnreturnedRoot(t *testing.T) {
	final := make(chan struct{})
	armed := true
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			if armed {
				armed = false
				panic("initial snapshot panic")
			}
			if count == 0 {
				close(final)
			}
		},
	}, true)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		tracker.start(t.Context(), func() (int, bool) { return 1, true })
	}()
	require.Equal(t, "initial snapshot panic", recovered)

	select {
	case <-final:
	case <-time.After(2 * time.Second):
		t.Fatal("initial panic did not publish the reclaimed root's final zero")
	}
	require.Eventually(t, func() bool {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()

		return len(tracker.roots) == 0 && !tracker.publishing && !tracker.dirty
	}, 2*time.Second, time.Millisecond)
}

func TestProviderProcessSnapshotSuccessorPanicRetiresWithoutSpinning(t *testing.T) {
	armed := false
	count := 1
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) {
			if armed {
				panic("successor publication panic")
			}
		},
	}, true)
	tracker.start(t.Context(), func() (int, bool) { return count, true })
	count = 2
	armed = true

	tracker.mu.Lock()
	tracker.dirty = true
	tracker.publishing = true
	tracker.mu.Unlock()
	tracker.publishSuccessor(t.Context())

	tracker.mu.Lock()
	require.False(t, tracker.publishing)
	require.True(t, tracker.dirty, "a later explicit refresh may retry the failed edge")
	require.False(t, tracker.set)
	tracker.mu.Unlock()
}

func TestProviderProcessSnapshotTrackerRunsSlowInventoryUnlockedAndDiscardsStaleResult(t *testing.T) {
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	}, true)
	root := tracker.start(t.Context(), func() (int, bool) {
		if calls.Add(1) == 1 {
			return 0, false
		}
		close(entered)
		awaitCorrectionCallback(t, release, "slow inventory release")

		return 7, true
	})

	refreshed := make(chan struct{})
	go func() {
		root.refresh(t.Context())
		close(refreshed)
	}()
	awaitCorrectionSignal(t, entered, "slow inventory entry")

	completed := make(chan struct{})
	go func() {
		root.complete(t.Context())
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("tracker mutex was held across process inventory")
	}
	close(release)
	awaitCorrectionSignal(t, refreshed, "stale refresh completion")

	require.Equal(t, []int{0}, snapshots, "inventory from the removed root must be discarded")
}

func TestProviderProcessSnapshotTrackerInventoryCanReenterItsRoot(t *testing.T) {
	var calls atomic.Int64
	var root *providerProcessRootObservation
	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	}, true)
	root = tracker.start(t.Context(), func() (int, bool) {
		if calls.Add(1) == 1 {
			return 0, false
		}
		root.complete(t.Context())

		return 9, true
	})

	done := make(chan struct{})
	go func() {
		root.refresh(t.Context())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reentrant inventory waited on the tracker mutex")
	}
	require.Equal(t, []int{0}, snapshots)
}

func TestProviderProcessSnapshotTrackerDefensiveAndDuplicateBoundaries(t *testing.T) {
	ctx := t.Context()
	var nilTracker *providerProcessSnapshotTracker
	require.Nil(t, nilTracker.start(ctx, nil))

	var nilObservation *providerProcessRootObservation
	nilObservation.refresh(ctx)
	nilObservation.complete(ctx)
	nilObservation.incomplete()
	(&providerProcessRootObservation{}).refresh(ctx)
	(&providerProcessRootObservation{}).complete(ctx)
	(&providerProcessRootObservation{}).incomplete()

	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	}, true)
	root := tracker.start(ctx, func() (int, bool) { return 2, true })
	root.refresh(ctx)
	root.refresh(ctx)
	root.complete(ctx)
	root.refresh(ctx)
	root.complete(ctx)
	root.incomplete()

	require.Equal(t, []int{2, 0}, snapshots)

	unavailable := tracker.start(ctx, nil)
	unavailable.refresh(ctx)
	unavailable.incomplete()
	negative := newProviderProcessSnapshotTracker(RuntimeResourceHooks{}, true).start(ctx, func() (int, bool) { return -1, true })
	negative.refresh(ctx)
	unknown := newProviderProcessSnapshotTracker(RuntimeResourceHooks{}, true).start(ctx, func() (int, bool) { return 0, false })
	unknown.refresh(ctx)
	zero := newProviderProcessSnapshotTracker(RuntimeResourceHooks{}, true).start(ctx, func() (int, bool) { return 0, true })
	zero.refresh(ctx)

	entries, err := (&agentSession{agent: &Agent{}}).loadTranscript(ctx)
	require.NoError(t, err)
	require.Nil(t, entries)
}

func TestRuntimeObservationHooksComposeExactLifetimes(t *testing.T) {
	var releases int
	var processDelta int64
	var snapshot int
	var stage RuntimeStartupStage
	var containment RuntimeContainmentMode
	hooks := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() { releases++ }, nil
		},
		ObserveProcess: func(_ context.Context, _ RuntimeProcessKind, delta int64) {
			processDelta += delta
		},
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshot = count
		},
		ObserveStartupStage: func(_ context.Context, _ RuntimeResourceKind, got RuntimeStartupStage, _ time.Duration, _ error) {
			stage = got
		},
		ObserveContainment: func(_ context.Context, got RuntimeContainmentMode) { containment = got },
	}, observer.New(observer.Config{}))

	release, err := hooks.AcquireNativeRoot(t.Context(), RuntimeResourceSession)
	require.NoError(t, err)
	release()
	release()
	require.Equal(t, 1, releases)

	panicReleases := 0
	panicking := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() {
				panicReleases++
				panic("release completed")
			}, nil
		},
	}, observer.New(observer.Config{}))
	panicRelease, err := panicking.AcquireNativeRoot(t.Context(), RuntimeResourceSession)
	require.NoError(t, err)
	func() {
		defer func() { require.Equal(t, "release completed", recover()) }()
		panicRelease()
	}()
	panicRelease()
	require.Equal(t, 1, panicReleases)

	hooks.ObserveProcess(t.Context(), RuntimeProcessHomeLockSupervisor, 2)
	hooks.ObserveProcessSnapshot(t.Context(), RuntimeProcessProviderDescendant, 3)
	observeRuntimeStartupStage(t.Context(), hooks, RuntimeResourceRuntime, RuntimeStartupReadiness, time.Now(), nil)
	hooks.ObserveContainment(t.Context(), RuntimeContainmentBestEffort)
	require.Equal(t, int64(2), processDelta)
	require.Equal(t, 3, snapshot)
	require.Equal(t, RuntimeStartupReadiness, stage)
	require.Equal(t, RuntimeContainmentBestEffort, containment)

	wantErr := errors.New("full")
	rejected := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return nil, wantErr
		},
	}, observer.New(observer.Config{}))
	_, err = rejected.ReserveScratchRoot(t.Context(), RuntimeResourcePrompt)
	require.ErrorIs(t, err, wantErr)
}
