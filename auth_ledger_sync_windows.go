//go:build windows

package ampacp

// syncAuthLedgerDirectory is a no-op on Windows. FlushFileBuffers does not
// accept a directory handle opened through os.Open, so the call it stands for
// can only fail there. What remains durable is the entry itself: the temporary
// file is written, chmod'd and fsynced before the rename, and NTFS journals
// the rename that publishes it, so a committed entry survives a crash under
// its final name. Only the ordering guarantee a directory flush adds on POSIX
// is unavailable.
func syncAuthLedgerDirectory(string) error { return nil }
