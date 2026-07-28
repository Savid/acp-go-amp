//go:build windows

package amp

import "errors"

// errBrowserShimUnsupported reports that no browser launch can be neutralised
// on this platform.
var errBrowserShimUnsupported = errors.New("browser launch cannot be neutralised on this platform")

// newBrowserShim fails closed. CreateProcess resolves cmd.exe, explorer.exe,
// and rundll32.exe out of the system directory ahead of every PATH entry, and
// the `start` that opens a URL is a cmd.exe builtin with no image to shadow, so
// a shim directory on PATH neutralises nothing here. A leg that cannot prove
// the launch is contained refuses to run rather than opening the operator's
// browser.
func newBrowserShim(string) (*browserShim, error) {
	return nil, errBrowserShimUnsupported
}
