package ampacp

import (
	"context"
	"errors"
	"reflect"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func hostAuthorityNil(authority HostAuthority) bool {
	if authority == nil {
		return true
	}

	value := reflect.ValueOf(authority)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func readHostEnvironment(authority HostAuthority) (environment map[string]string, err error) {
	if hostAuthorityNil(authority) {
		return nil, ErrHostAuthorityUnavailable
	}

	defer func() {
		if recover() != nil {
			environment = nil
			err = ErrHostAuthorityUnavailable
		}
	}()

	environment = authority.NativeEnvironment()
	if environment == nil {
		return nil, ErrHostAuthorityUnavailable
	}

	return cloneStringMap(environment), nil
}

func (a *Agent) prepareNativeTree(ctx context.Context, root string) (err error) {
	if !a.options.hostAuthoritySupplied {
		return nil
	}

	a.mu.Lock()
	boundaryErr := a.lifecycleContainmentErr
	a.mu.Unlock()

	if boundaryErr != nil {
		return authorityBoundaryError(boundaryErr)
	}

	defer func() {
		if recover() != nil {
			err = authorityBoundaryError(ErrHostAuthorityUnavailable)
		}

		if err != nil {
			err = authorityBoundaryError(err)
		}

		a.recordAuthorityFailure(err)
	}()

	return a.options.HostAuthority.PrepareNativeTree(ctx, root)
}

func (a *Agent) reclaimNativeTree(ctx context.Context, root string) (err error) {
	if !a.options.hostAuthoritySupplied {
		return nil
	}

	a.mu.Lock()
	boundaryErr := a.lifecycleContainmentErr
	a.mu.Unlock()

	if boundaryErr != nil {
		return authorityBoundaryError(boundaryErr)
	}

	defer func() {
		if recover() != nil {
			err = authorityBoundaryError(ErrHostAuthorityUnavailable)
		}

		a.recordAuthorityFailure(err)
	}()

	err = a.options.HostAuthority.ReclaimNativeTree(ctx, root)
	if err != nil && !errors.Is(err, ErrNativeTreeBusy) {
		err = authorityBoundaryError(err)
	}

	return err
}

func (a *Agent) recordAuthorityFailure(err error) {
	if a == nil || err == nil || errors.Is(err, ErrNativeTreeBusy) || detachedContextError(err) {
		return
	}

	if !errors.Is(err, ErrHostAuthorityUnavailable) && !containmentIncomplete(err) {
		return
	}

	err = publicContainmentError(err)

	a.mu.Lock()
	a.lifecycleContainmentErr = errors.Join(a.lifecycleContainmentErr, err)
	a.mu.Unlock()
}

func containmentIncomplete(err error) bool {
	return errors.Is(err, ErrContainmentIncomplete) || errors.Is(err, nativeamp.ErrContainmentIncomplete)
}

func publicContainmentError(err error) error {
	if err == nil || errors.Is(err, ErrContainmentIncomplete) || !errors.Is(err, nativeamp.ErrContainmentIncomplete) {
		return err
	}

	return errors.Join(err, ErrContainmentIncomplete)
}

func detachedContextError(err error) bool {
	if err == nil || containmentIncomplete(err) {
		return false
	}

	switch value := err.(type) {
	case interface{ Unwrap() []error }:
		children := value.Unwrap()
		if len(children) == 0 {
			return false
		}

		for _, child := range children {
			if !detachedContextError(child) {
				return false
			}
		}

		return true
	case interface{ Unwrap() error }:
		return detachedContextError(value.Unwrap())
	default:
		return err == context.Canceled || err == context.DeadlineExceeded
	}
}
