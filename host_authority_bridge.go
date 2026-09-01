package ampacp

import (
	"context"
	"errors"
	"io"
	"reflect"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func (a *Agent) configureNativeClient(options *nativeamp.Options) {
	options.OrdinaryEnvironment = cloneStringMap(a.ordinaryEnvironment)
	options.NativeEnvironment = cloneStringMap(a.nativeEnvironment)
	options.TestOnlyAuthLoginPlatform = a.options.testOnlyAuthLoginPlatform

	if !a.options.hostAuthoritySupplied {
		return
	}

	options.ReadNativeAppendLog = func(ctx context.Context, path string, offset uint64) ([][]byte, error) {
		return readHostNativeAppendLog(a.options.HostAuthority, ctx, path, offset)
	}
	options.StartNative = func(ctx context.Context, request nativeamp.NativeRequest) (nativeamp.NativeProcess, error) {
		a.mu.Lock()
		boundaryErr := a.lifecycleContainmentErr
		a.mu.Unlock()

		if boundaryErr != nil {
			return nil, boundaryErr
		}

		process, err := startHostNative(a.options.HostAuthority, ctx, NativeRequest{
			Executable: request.Executable, Arguments: append([]string(nil), request.Arguments...),
			Environment: append([]string(nil), request.Environment...), WorkingDirectory: request.WorkingDirectory,
		})
		if err != nil {
			startErr := startNativeError(err)
			a.recordAuthorityFailure(startErr)

			return nil, startErr
		}

		if nativeProcessNil(process) {
			err = ambiguousNativeStartError()
			a.recordAuthorityFailure(err)

			return nil, err
		}

		return nativeProcessBridge{agent: a, process: process}, nil
	}
}

func readHostNativeAppendLog(
	authority HostAuthority,
	ctx context.Context,
	path string,
	offset uint64,
) (entries [][]byte, err error) {
	defer func() {
		if recover() != nil {
			entries = nil
			err = ErrHostAuthorityUnavailable
		}
	}()

	return authority.ReadNativeAppendLog(ctx, path, offset)
}

func startHostNative(authority HostAuthority, ctx context.Context, request NativeRequest) (process NativeProcess, err error) {
	defer func() {
		if recover() != nil {
			process = nil
			err = ambiguousNativeStartError()
		}
	}()

	return authority.StartNative(ctx, request)
}

// A returned StartNative error is a refusal: the authority contract proves no
// child or reusable-identity ambiguity remains. Preserve that error exactly
// unless it already carries an authority sentinel. Only contract violations on
// the successful-return side manufacture ambiguity.
func startNativeError(err error) error {
	if err == nil {
		return nil
	}

	if containmentIncomplete(err) {
		return authorityBoundaryError(err)
	}

	return err
}

func ambiguousNativeStartError() error {
	return errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete, nativeamp.ErrContainmentIncomplete)
}

func authorityBoundaryError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, ErrNativeTreeBusy) || detachedContextError(err) {
		return err
	}

	return errors.Join(err, ErrContainmentIncomplete, nativeamp.ErrContainmentIncomplete)
}

func nativeProcessNil(process NativeProcess) bool {
	return interfaceValueNil(process)
}

func interfaceValueNil(value any) bool {
	if value == nil {
		return true
	}

	reflected := reflect.ValueOf(value)

	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

type nativeProcessBridge struct {
	agent   *Agent
	process NativeProcess
}

func (p nativeProcessBridge) Stdin() io.WriteCloser { return p.process.Stdin() }
func (p nativeProcessBridge) Stdout() io.ReadCloser { return p.process.Stdout() }
func (p nativeProcessBridge) Stderr() io.ReadCloser { return p.process.Stderr() }

func (p nativeProcessBridge) Wait(ctx context.Context) (nativeamp.NativeResult, error) {
	result, err := p.process.Wait(ctx)
	boundaryErr := authorityBoundaryError(err)
	p.agent.recordAuthorityFailure(boundaryErr)

	return nativeamp.NativeResult{
		ExitCode: result.ExitCode, Signal: result.Signal, Revoked: result.Revoked,
	}, boundaryErr
}

func (p nativeProcessBridge) Revoke(ctx context.Context) error {
	err := p.process.Revoke(ctx)
	boundaryErr := authorityBoundaryError(err)
	p.agent.recordAuthorityFailure(boundaryErr)

	return boundaryErr
}
