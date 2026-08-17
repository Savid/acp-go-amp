package amp

import (
	"errors"
	"fmt"
	"strings"
)

// MaxThreadIDBytes bounds the native identifier adopted as the ACP session id.
// Real Amp thread ids are short T-prefixed tokens; this defensive ceiling keeps
// the preserved raw-event envelope bounded even when the native boundary is
// malformed or a host supplies corrupt stored state.
const MaxThreadIDBytes = 4 * 1024

// ValidateThreadID validates a native Amp thread identifier before the wrapper
// adopts it as an ACP session id or restores it from the session store.
func ValidateThreadID(threadID string) error {
	switch {
	case !strings.HasPrefix(threadID, "T-"):
		return errors.New("amp thread id must start with T-")
	case len(threadID) > MaxThreadIDBytes:
		return fmt.Errorf("amp thread id exceeds %d bytes", MaxThreadIDBytes)
	default:
		return nil
	}
}
