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

	options.StartNative = func(ctx context.Context, request nativeamp.NativeRequest) (nativeamp.NativeProcess, error) {
		a.mu.Lock()
		boundaryErr := a.lifecycleContainmentErr
		a.mu.Unlock()

		if boundaryErr != nil {
			return nil, authorityBoundaryError(boundaryErr)
		}

		process, err := a.options.HostAuthority.StartNative(ctx, NativeRequest{
			Executable: request.Executable, Arguments: append([]string(nil), request.Arguments...),
			Environment: append([]string(nil), request.Environment...), WorkingDirectory: request.WorkingDirectory,
		})
		if err != nil {
			boundaryErr := authorityBoundaryError(err)
			a.recordAuthorityFailure(boundaryErr)

			return nil, boundaryErr
		}

		if nativeProcessNil(process) {
			err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
			a.recordAuthorityFailure(err)

			return nil, authorityBoundaryError(err)
		}

		return nativeProcessBridge{agent: a, process: process}, nil
	}
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
	if process == nil {
		return true
	}

	value := reflect.ValueOf(process)

	return value.Kind() == reflect.Pointer && value.IsNil()
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

	return authorityBoundaryError(err)
}
