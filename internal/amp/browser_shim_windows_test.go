//go:build windows

package amp

import (
	"testing"
)

func TestPortableWindowsBrowserShimMaterializationIsUnavailable(t *testing.T) {
	shim, err := MaterializeBrowserShim(t.TempDir())
	if shim != "" || err != nil {
		t.Fatalf("MaterializeBrowserShim = %q, %v; want an unavailable shim", shim, err)
	}
}
