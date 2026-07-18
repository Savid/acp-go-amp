package amp

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync/atomic"
	"testing"
)

var testDarwinRuntimeID atomic.Uint64

func newTestClient(t *testing.T, logger *slog.Logger, options Options) *Client {
	t.Helper()
	if runtime.GOOS == "darwin" {
		options.DarwinBestEffort = true
		options.NewDarwinGeneration = func(_ context.Context) (*DarwinGeneration, error) {
			return &DarwinGeneration{
				RuntimeID:   fmt.Sprintf("%032x", testDarwinRuntimeID.Add(1)),
				ScratchRoot: t.TempDir(),
			}, nil
		}
	}

	return NewClient(logger, options)
}
