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

	first := tracker.start(t.Context())
	first.snapshot(t.Context(), 2)
	second := tracker.start(t.Context())
	require.Equal(t, []int{2}, snapshots, "an unknown root must suppress a new absolute total")

	second.snapshot(t.Context(), 3)
	first.quiescent(t.Context())
	second.quiescent(t.Context())

	require.Equal(t, []int{2, 5, 3, 0}, snapshots)
}

func TestProviderProcessSnapshotTrackerUnprovenRootPreservesLastNonzero(t *testing.T) {
	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	})

	unproven := tracker.start(t.Context())
	unproven.snapshot(t.Context(), 4)
	unproven.unproven()

	other := tracker.start(t.Context())
	other.snapshot(t.Context(), 1)
	other.quiescent(t.Context())

	require.Equal(t, []int{4}, snapshots, "unproven containment must suppress lower totals and zero")
}

func TestProviderProcessSnapshotTrackerConcurrentLifecycle(t *testing.T) {
	const roots = 32

	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	})
	observations := make([]*providerProcessRootObservation, roots)
	for i := range observations {
		observations[i] = tracker.start(context.Background())
	}

	var group sync.WaitGroup
	for _, observation := range observations {
		group.Add(1)
		go func() {
			defer group.Done()
			observation.snapshot(context.Background(), 1)
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

func TestProviderProcessSnapshotTrackerDefensiveAndDuplicateBoundaries(t *testing.T) {
	ctx := t.Context()
	var nilTracker *providerProcessSnapshotTracker
	require.Nil(t, nilTracker.start(ctx))

	var nilObservation *providerProcessRootObservation
	nilObservation.snapshot(ctx, 1)
	nilObservation.quiescent(ctx)
	nilObservation.unproven()
	(&providerProcessRootObservation{}).snapshot(ctx, 1)
	(&providerProcessRootObservation{}).quiescent(ctx)
	(&providerProcessRootObservation{}).unproven()

	var snapshots []int
	tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
		ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
			snapshots = append(snapshots, count)
		},
	})
	root := tracker.start(ctx)
	root.snapshot(ctx, -1)
	root.snapshot(ctx, 2)
	root.snapshot(ctx, 2)
	root.quiescent(ctx)
	root.snapshot(ctx, 1)
	root.quiescent(ctx)
	root.unproven()

	require.Equal(t, []int{2, 0}, snapshots)

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
