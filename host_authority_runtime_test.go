package ampacp

import (
	"context"
	"errors"
	"fmt"
	"testing"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

type residualAuthority struct {
	environment  map[string]string
	envPanic     bool
	preparePanic bool
	reclaimPanic bool
	prepareErr   error
	reclaimErr   error
}

type contextReclaimAuthority struct {
	residualAuthority
	started chan struct{}
	exited  chan struct{}
}

func (a *contextReclaimAuthority) ReclaimNativeTree(ctx context.Context, _ string) error {
	close(a.started)
	<-ctx.Done()
	close(a.exited)

	return ctx.Err()
}

func (a residualAuthority) NativeEnvironment() map[string]string {
	if a.envPanic {
		panic("environment panic")
	}

	return a.environment
}

func (a residualAuthority) PrepareNativeTree(context.Context, string) error {
	if a.preparePanic {
		panic("prepare panic")
	}

	return a.prepareErr
}

func (a residualAuthority) ReadNativeAppendLog(context.Context, string, uint64) ([][]byte, error) {
	return nil, nil
}

func (a residualAuthority) StartNative(context.Context, NativeRequest) (NativeProcess, error) {
	return nil, ErrHostAuthorityUnavailable
}

func (a residualAuthority) ReclaimNativeTree(context.Context, string) error {
	if a.reclaimPanic {
		panic("reclaim panic")
	}

	return a.reclaimErr
}

type emptyMultiError struct{}

func (emptyMultiError) Error() string { return "empty multi error" }

func (emptyMultiError) Unwrap() []error { return nil }

func TestHostAuthorityUtilityResidualBranches(t *testing.T) {
	require.False(t, hostAuthorityNil(residualAuthority{}))
	require.True(t, interfaceValueNil(nil))
	require.True(t, interfaceValueNil([]string(nil)))
	require.False(t, interfaceValueNil(1))
	require.NoError(t, startNativeError(nil))

	environment, err := readHostEnvironment(residualAuthority{environment: map[string]string{"A": "B"}})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"A": "B"}, environment)
	_, err = readHostEnvironment(residualAuthority{})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	_, err = readHostEnvironment(residualAuthority{envPanic: true})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)

	require.False(t, detachedContextError(nil))
	require.False(t, detachedContextError(ErrContainmentIncomplete))
	require.False(t, detachedContextError(emptyMultiError{}))
	require.True(t, detachedContextError(errors.Join(context.Canceled, context.DeadlineExceeded)))
	require.False(t, detachedContextError(errors.Join(context.Canceled, errors.New("other"))))
	require.True(t, detachedContextError(fmt.Errorf("wrapped: %w", context.Canceled)))
	require.True(t, detachedContextError(context.Canceled))

	for _, boundary := range []error{ErrHostAuthorityUnavailable, ErrNativeTreeBusy, nativeamp.ErrContainmentIncomplete} {
		require.Error(t, nativeInternalError(classNativeStartupFailed, boundary))
	}
	require.NoError(t, publicContainmentError(nil))
}

func TestHostAuthorityPrepareAndReclaimResidualBranches(t *testing.T) {
	agent := NewAgent()
	require.NoError(t, agent.prepareNativeTree(t.Context(), t.TempDir()))
	require.NoError(t, agent.reclaimNativeTree(t.Context(), t.TempDir()))

	agent.options.hostAuthoritySupplied = true
	agent.options.HostAuthority = residualAuthority{preparePanic: true, environment: map[string]string{}}
	require.ErrorIs(t, agent.prepareNativeTree(t.Context(), t.TempDir()), ErrHostAuthorityUnavailable)

	agent = NewAgent()
	agent.options.hostAuthoritySupplied = true
	agent.options.HostAuthority = residualAuthority{reclaimPanic: true, environment: map[string]string{}}
	require.ErrorIs(t, agent.reclaimNativeTree(t.Context(), t.TempDir()), ErrHostAuthorityUnavailable)

	agent = NewAgent()
	agent.options.hostAuthoritySupplied = true
	agent.options.HostAuthority = residualAuthority{reclaimErr: ErrNativeTreeBusy, environment: map[string]string{}}
	require.ErrorIs(t, agent.reclaimNativeTree(t.Context(), t.TempDir()), ErrNativeTreeBusy)

	agent = NewAgent()
	agent.options.hostAuthoritySupplied = true
	agent.lifecycleContainmentErr = ErrContainmentIncomplete
	require.ErrorIs(t, agent.prepareNativeTree(t.Context(), t.TempDir()), ErrContainmentIncomplete)
	require.ErrorIs(t, agent.reclaimNativeTree(t.Context(), t.TempDir()), ErrContainmentIncomplete)
}
