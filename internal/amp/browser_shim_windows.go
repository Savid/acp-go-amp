//go:build windows

package amp

// newBrowserShim fails closed. CreateProcess resolves cmd.exe, explorer.exe,
// and rundll32.exe out of the system directory ahead of every PATH entry, and
// the `start` that opens a URL is a cmd.exe builtin with no image to shadow, so
// a shim directory on PATH neutralises nothing here. A leg that cannot prove
// the launch is contained refuses to run rather than opening the operator's
// browser.
func newBrowserShim(string) (*browserShim, error) {
	return nil, ErrBrowserLaunchUnsupported
}

func MaterializeBrowserShim(string) (string, error) { return "", nil }
