//go:build unix

package ampacp

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// TestHandoffFIFOInsideRootIsRejected pins that containment bounds where a name
// may lead and never what kind of object it names: a FIFO with no writer blocks
// an ordinary open until one appears, so the verdict has to arrive without the
// open ever waiting on it.
func TestHandoffFIFOInsideRootIsRejected(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "valid.png")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	block := handoffBlock(path, imageMIMEPNG, imageFixture(t, "valid.png"))

	done := make(chan error, 1)

	go func() {
		_, err := promptInputWithPolicy(
			context.Background(),
			[]acp.ContentBlock{block},
			handoffPolicy(root, applyOptions(nil).ImageLimits),
		)
		done <- err
	}()

	select {
	case err := <-done:
		requireHandoffError(t, err, 0, imageErrorPathNotAllowed, handoffNotRegularMessage)
	case <-time.After(10 * time.Second):
		t.Fatal("opening a FIFO inside the handoff root blocked the read")
	}
}
