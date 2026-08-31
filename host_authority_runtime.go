package ampacp

import (
	"context"
	"errors"
	"reflect"
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
	if a.options.HostAuthority == nil {
		return nil
	}

	defer func() {
		if recover() != nil {
			err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
		}

		a.recordAuthorityFailure(err)
	}()

	err = a.options.HostAuthority.PrepareNativeTree(ctx, root)
	if err != nil && !errors.Is(err, ErrNativeTreeBusy) {
		err = errors.Join(err, ErrContainmentIncomplete)
	}

	return err
}

func (a *Agent) reclaimNativeTree(ctx context.Context, root string) (err error) {
	if a.options.HostAuthority == nil {
		return nil
	}

	defer func() {
		if recover() != nil {
			err = errors.Join(ErrHostAuthorityUnavailable, ErrContainmentIncomplete)
		}

		a.recordAuthorityFailure(err)
	}()

	err = a.options.HostAuthority.ReclaimNativeTree(ctx, root)
	if err != nil {
		err = errors.Join(err, ErrContainmentIncomplete)
	}

	return err
}

func (a *Agent) recordAuthorityFailure(err error) {
	if a == nil || err == nil {
		return
	}

	a.mu.Lock()
	a.lifecycleContainmentErr = errors.Join(a.lifecycleContainmentErr, err)
	a.mu.Unlock()
}
