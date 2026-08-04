package amp

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
)

var testDarwinRuntimeID atomic.Uint64

func newTestClient(t *testing.T, logger *slog.Logger, options Options) *Client {
	t.Helper()
	options.Isolation = testProcessIsolation()
	options.Isolation.TestOnlyIdentityLockRoot = t.TempDir()
	if options.TestOnlyAuthLoginPlatform == "" {
		options.TestOnlyAuthLoginPlatform = "linux"
	}
	if runtime.GOOS == "darwin" {
		options.DarwinBestEffort = true
		options.NewDarwinGeneration = func(_ context.Context) (*DarwinGeneration, error) {
			return &DarwinGeneration{
				RuntimeID:   fmt.Sprintf("%032x", testDarwinRuntimeID.Add(1)),
				ScratchRoot: t.TempDir(),
			}, nil
		}
	}

	client := NewClient(logger, options)
	client.checkAuthLoginCompatibility = func(string) error { return nil }

	return client
}

func testProcessIsolation() *ProcessIsolation {
	uid, gid := os.Geteuid(), os.Getegid()
	if uid == 0 || gid == 0 {
		uid, gid = 65534, 65534
	}

	return &ProcessIsolation{
		UID: uint32(uid), GID: uint32(gid),
		BaseEnvironment:      map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")},
		TestOnlyNoCredential: true,
	}
}
