package ampacp

import (
	"context"
	"errors"
	"fmt"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

var newDarwinGenerationRecord = nativeamp.NewDarwinGenerationRecord

func (mode RuntimeContainmentMode) provesWholeTreeLifecycle() bool {
	return mode == RuntimeContainmentAuthoritative
}

func (a *Agent) configureNativeClient(options *nativeamp.Options, kind RuntimeResourceKind) {
	options.Isolation = nativeProcessIsolation(a.options.ProcessIsolation, a.options.testOnlyNoCredential)
	options.OrdinaryEnvironment = cloneStringMap(a.ordinaryEnvironment)

	options.TestOnlyAuthLoginPlatform = a.options.testOnlyAuthLoginPlatform

	if options.Isolation != nil {
		options.Isolation.TestOnlyIdentityLockRoot = a.options.testOnlyIdentityLockRoot
	}

	options.DarwinBestEffort = a.containmentMode == RuntimeContainmentBestEffort
	options.AcquireNativeRoot = func(ctx context.Context) (func(), error) {
		return acquireNativeRoot(ctx, a.options.RuntimeResourceHooks, kind)
	}
	options.NewDarwinGeneration = func(ctx context.Context) (*nativeamp.DarwinGeneration, error) {
		releaseScratch, err := reserveScratchRoot(ctx, a.options.RuntimeResourceHooks, kind)
		if err != nil {
			return nil, err
		}

		parent, err := ensureScratchParent(a.options.ScratchDir)
		if err != nil {
			releaseScratch()

			return nil, err
		}

		root, err := mkdirTemp(parent, "acp-go-amp-command-*")
		if err != nil {
			releaseScratch()

			return nil, fmt.Errorf("create Amp containment generation root: %w", err)
		}

		generation, err := newDarwinGenerationRecord(parent, root, string(kind))
		if err != nil {
			removeErr := removeSessionDir(root)
			if removeErr == nil {
				releaseScratch()
			}

			return nil, errors.Join(err, removeErr)
		}

		generation.Release = func(complete bool) error {
			if !complete {
				return nil
			}

			removeErr := removeSessionDir(root)
			if removeErr == nil {
				releaseScratch()
			}

			return removeErr
		}

		return generation, nil
	}
}

func nativeProcessIsolation(isolation *ProcessIsolation, testOnlyNoCredential bool) *nativeamp.ProcessIsolation {
	if isolation == nil {
		return nil
	}

	base := cloneStringMap(isolation.BaseEnvironment)

	return &nativeamp.ProcessIsolation{
		UID: isolation.UID, GID: isolation.GID, BaseEnvironment: base,
		TestOnlyNoCredential: testOnlyNoCredential, IdentityLock: isolation.IdentityLock, AuthorityDomain: isolation.AuthorityDomain,
		StandaloneOwnerID: isolation.StandaloneOwnerID, StandaloneStateRoot: isolation.StandaloneStateRoot,
	}
}
