//go:build windows

package amp

import (
	"errors"
	"testing"
)

func TestPortableWindowsBrowserShimFailsClosed(t *testing.T) {
	shim, err := newBrowserShim(t.TempDir())
	if shim != nil || !errors.Is(err, ErrBrowserLaunchUnsupported) {
		t.Fatalf("newBrowserShim = %v, %v; want a refusal", shim, err)
	}
}
