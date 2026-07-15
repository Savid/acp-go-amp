package ampacp

import (
	"context"
	"errors"
	"sync"
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
	})

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
	first.quiescent(t.Context())
	second.quiescent(t.Context())

	require.Equal(t, []int{2, 5, 7, 3, 0}, snapshots)
}

func TestProviderProcessSnapshotTrackerUnprovenRootPreservesLastNonzero(t *testing.T) {
	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	})

	unproven := tracker.start(t.Context(), func() (int, bool) { return 4, true })
	unproven.refresh(t.Context())
	unproven.unproven()

	other := tracker.start(t.Context(), func() (int, bool) { return 1, true })
	other.refresh(t.Context())
	other.quiescent(t.Context())

	require.Equal(t, []int{4}, snapshots, "unproven containment must suppress lower totals and zero")
}

func TestProviderProcessSnapshotTrackerConcurrentLifecycle(t *testing.T) {
	const roots = 32

	available := false
	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	})
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
			observation.quiescent(context.Background())
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

	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{})
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

func TestProviderProcessSnapshotTrackerDefensiveAndDuplicateBoundaries(t *testing.T) {
	ctx := t.Context()
	var nilTracker *providerProcessSnapshotTracker
	require.Nil(t, nilTracker.start(ctx, nil))

	var nilObservation *providerProcessRootObservation
	nilObservation.refresh(ctx)
	nilObservation.quiescent(ctx)
	nilObservation.unproven()
	(&providerProcessRootObservation{}).refresh(ctx)
	(&providerProcessRootObservation{}).quiescent(ctx)
	(&providerProcessRootObservation{}).unproven()

	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	})
	root := tracker.start(ctx, func() (int, bool) { return 2, true })
	root.refresh(ctx)
	root.refresh(ctx)
	root.quiescent(ctx)
	root.refresh(ctx)
	root.quiescent(ctx)
	root.unproven()

	require.Equal(t, []int{2, 0}, snapshots)

	unavailable := tracker.start(ctx, nil)
	unavailable.refresh(ctx)
	unavailable.unproven()
	negative := newProviderProcessSnapshotTracker(RuntimeResourceHooks{}).start(ctx, func() (int, bool) { return -1, true })
	negative.refresh(ctx)
	unknown := newProviderProcessSnapshotTracker(RuntimeResourceHooks{}).start(ctx, func() (int, bool) { return 0, false })
	unknown.refresh(ctx)
	zero := newProviderProcessSnapshotTracker(RuntimeResourceHooks{}).start(ctx, func() (int, bool) { return 0, true })
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
	}, observer.New(observer.Config{}))

	release, err := hooks.AcquireNativeRoot(t.Context(), RuntimeResourceSession)
	require.NoError(t, err)
	release()
	release()
	require.Equal(t, 1, releases)

	observeRuntimeProcess(t.Context(), hooks, RuntimeProcessHomeLockSupervisor, 2)
	observeRuntimeProcessSnapshot(t.Context(), hooks, RuntimeProcessProviderDescendant, 3)
	observeRuntimeStartupStage(t.Context(), hooks, RuntimeResourceRuntime, RuntimeStartupReadiness, time.Now(), nil)
	require.Equal(t, int64(2), processDelta)
	require.Equal(t, 3, snapshot)
	require.Equal(t, RuntimeStartupReadiness, stage)

	wantErr := errors.New("full")
	rejected := instrumentRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return nil, wantErr
		},
	}, observer.New(observer.Config{}))
	_, err = rejected.ReserveScratchRoot(t.Context(), RuntimeResourcePrompt)
	require.ErrorIs(t, err, wantErr)
}
