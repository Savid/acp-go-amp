package ampacp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/savid/acp-go-amp/internal/observer"
	"github.com/stretchr/testify/require"
)

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
