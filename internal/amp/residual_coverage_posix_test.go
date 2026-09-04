//go:build !windows

package amp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientOutputResidualBranches(t *testing.T) {
	client := NewClient(nil, Options{OrdinaryEnvironment: map[string]string{"PATH": t.TempDir()}})
	_, err := client.outputWithArgs(t.Context(), "version")
	require.Error(t, err)

	original := getwd
	t.Cleanup(func() { getwd = original })
	getwd = func() (string, error) { return "/chosen", nil }
	require.Equal(t, "/chosen", NewClient(nil, Options{}).commandCwd())

	_, err = client.outputAtPath(cancelledContext(), "/unused", "version")
	require.ErrorIs(t, err, context.Canceled)

	wantProbe := errors.New("probe refused")
	client = NewClient(nil, Options{NewProbeClient: func(context.Context) (*Client, func() error, error) {
		return nil, nil, wantProbe
	}})
	_, err = client.outputAtPath(t.Context(), "/unused", "version")
	require.ErrorIs(t, err, wantProbe)

	wantCleanup := errors.New("cleanup refused")
	client = NewClient(nil, Options{NewProbeClient: func(context.Context) (*Client, func() error, error) {
		return NewClient(nil, Options{}), func() error { return wantCleanup }, nil
	}})
	_, err = client.outputAtPath(t.Context(), "/unused", "version")
	require.ErrorIs(t, err, wantCleanup)

	path, _ := fakeAmpPath(t, "")
	client = NewClient(nil, Options{NewProbeClient: func(context.Context) (*Client, func() error, error) {
		return newTestProbeClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()}), func() error { return wantCleanup }, nil
	}})
	_, err = client.outputAtPath(t.Context(), path, "version")
	require.ErrorIs(t, err, wantCleanup)

	client = NewClient(nil, Options{Cwd: t.TempDir(), Env: map[string]string{"bad=key": "x"}, StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return newCoverageNativeProcess(), nil
	}})
	_, err = client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorContains(t, err, "invalid environment key")

	wantStart := errors.New("start refused")
	client = NewClient(nil, Options{Cwd: t.TempDir(), StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, wantStart
	}})
	_, err = client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorIs(t, err, wantStart)

	var waitCalls int
	process := newCoverageNativeProcess()
	process.wait = func(context.Context) (NativeResult, error) {
		waitCalls++
		if waitCalls == 1 {
			return NativeResult{}, context.Canceled
		}

		return NativeResult{Revoked: true}, errors.New("terminal")
	}
	process.revokeErr = errors.New("revoke refused")
	client = NewClient(nil, Options{Cwd: t.TempDir(), StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return process, nil
	}})
	_, err = client.outputAtPath(t.Context(), "amp", "threads", "list")
	require.ErrorContains(t, err, "exit code")
	require.Equal(t, 2, waitCalls)

	process = newCoverageNativeProcess()
	process.wait = func(context.Context) (NativeResult, error) { return NativeResult{ExitCode: 7}, nil }
	client.options.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) { return process, nil }
	_, err = client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorContains(t, err, "exit code 7")
}
