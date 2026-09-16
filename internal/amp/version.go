package amp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/savid/acp-go-core/process"
)

// MinimumVersion is the lowest `amp --version` the adapter accepts: the
// version its behavior was verified against.
const MinimumVersion = "0.0.1789432613"

// versionProbeTimeout bounds the native version command.
const versionProbeTimeout = 10 * time.Second

// ProbeVersion reads only the native version command. The native output is
// the version followed by its release metadata, so the first field is it.
func ProbeVersion(ctx context.Context, executable string, env []string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, versionProbeTimeout)
	defer cancel()

	output, result, _, err := Run(ctx, process.Request{Executable: executable, Args: []string{"--version"}, Env: env}, nil, nil)
	if err != nil {
		return "", err
	}

	if result.ExitCode != 0 {
		return "", fmt.Errorf("native version exited with status %d", result.ExitCode)
	}

	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		return "", errors.New("native version is empty")
	}

	return fields[0], nil
}
