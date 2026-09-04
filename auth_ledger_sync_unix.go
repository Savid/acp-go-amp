//go:build !windows

package ampacp

import "errors"

// syncAuthLedgerDirectory flushes the ledger directory itself, so the rename
// that committed an entry is durable and not merely visible. The open goes
// through the ledgerOpen seam the write path's fault injection drives.
func syncAuthLedgerDirectory(path string) error {
	dir, err := ledgerOpen(path)
	if err != nil {
		return err
	}

	return errors.Join(dir.Sync(), dir.Close())
}
