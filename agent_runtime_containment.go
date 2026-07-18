package ampacp

import (
	"context"
	"errors"
	"fmt"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func (a *Agent) configureNativeClient(options *nativeamp.Options, kind RuntimeResourceKind) {
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

		generation, err := nativeamp.NewDarwinGenerationRecord(parent, root, string(kind))
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
