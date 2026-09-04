//go:build windows

package ampacp

import (
	"errors"
	"os"
	"testing"
)

// TestAuthLedgerCommitNeverOpensTheLedgerRootOnWindows pins the documented
// Windows behaviour: FlushFileBuffers refuses a directory handle opened through
// os.Open, so the commit path never opens the ledger root at all and a fault
// planted on that open cannot reach a ledger write. The entry itself is still
// readable back, which is the durability the commit promises here.
func TestAuthLedgerCommitNeverOpensTheLedgerRootOnWindows(t *testing.T) {
	ledger := newTestLedger(t)

	original := ledgerOpen
	t.Cleanup(func() { ledgerOpen = original })

	ledgerOpen = func(string) (*os.File, error) { return nil, errors.New("open") }

	if err := syncAuthLedgerDirectory(ledger.dir); err != nil {
		t.Fatalf("directory sync: %v", err)
	}

	record := authLedgerRecord{ProviderID: "amp", ConnectionID: "conn-1"}
	if err := ledger.write(record); err != nil {
		t.Fatalf("ledger commit: %v", err)
	}

	stored, found, err := ledger.read("amp", "conn-1")
	if err != nil || !found {
		t.Fatalf("read back = (%v, %v)", found, err)
	}

	if stored != record {
		t.Fatalf("stored = %+v, want %+v", stored, record)
	}
}
