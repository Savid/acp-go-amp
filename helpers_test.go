package ampacp

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

func testContainmentOptions(options []Option) []Option {
	baseEnvironment := map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")}
	for _, key := range []string{"AMP_API_KEY", "AMP_URL"} {
		if value, ok := os.LookupEnv(key); ok {
			baseEnvironment[key] = value
		}
	}
	options = append(options, WithProcessIsolation(ProcessIsolation{
		UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()),
		BaseEnvironment:   baseEnvironment,
		StandaloneOwnerID: "acp-go-amp-tests", StandaloneStateRoot: os.TempDir(),
	}))
	options = append(options, func(options *Options) {
		options.testOnlyNoCredential = true
		options.testOnlyIdentityLockRoot = testIdentityLockRoot()
		if options.testOnlyAuthLoginPlatform == "" {
			options.testOnlyAuthLoginPlatform = platformLinux
		}
	})
	if runtime.GOOS == "darwin" {
		return append(options, WithDarwinBestEffortContainment())
	}

	return options
}

func testIdentityLockRoot() string {
	root := filepath.Join(os.TempDir(), "acp-go-amp-agent-identities-"+strconv.Itoa(os.Getpid()))
	if err := os.Mkdir(root, 0o700); err != nil && !os.IsExist(err) {
		panic(err)
	}

	return root
}

func newTestAgent(options ...Option) *Agent {
	return NewAgent(testContainmentOptions(options)...)
}

func serveTest(ctx context.Context, input io.Reader, output io.Writer, options ...Option) error {
	return Serve(ctx, input, output, testContainmentOptions(options)...)
}
