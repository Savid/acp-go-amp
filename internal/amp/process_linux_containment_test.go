//go:build linux

package amp

import (
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestLinuxProcessTreeContainmentHelpers(t *testing.T) {
	if err := (&processTree{}).runAuthoritativeCleanup(time.Millisecond); err != nil {
		t.Fatalf("empty cleanup = %v", err)
	}
	if err := validateBestEffortLaunch(nil, nil, nil); err != nil {
		t.Fatalf("Linux best-effort validation hook = %v", err)
	}

	if err := (*processTreeCommand)(nil).releaseStartGate(); err != nil {
		t.Fatalf("nil gate release = %v", err)
	}
	reader, writer, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	gate := &processTreeCommand{startGate: writer}
	if releaseErr := gate.releaseStartGate(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	var value [1]byte
	if _, readErr := reader.Read(value[:]); readErr != nil || value[0] != 1 {
		t.Fatalf("gate byte = %v, err=%v", value, readErr)
	}
	_ = reader.Close()

	reader, writer, pipeErr = os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	_ = reader.Close()
	_ = writer.Close()
	if releaseErr := (&processTreeCommand{startGate: writer}).releaseStartGate(); releaseErr == nil {
		t.Fatal("closed gate released")
	}
	(*processTreeCommand)(nil).abortStartGate()
	reader, writer, pipeErr = os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	_ = reader.Close()
	aborted := &processTreeCommand{startGate: writer}
	aborted.abortStartGate()
	if aborted.startGate != nil {
		t.Fatal("aborted gate retained")
	}

	if (*processTree)(nil).commandWait() != nil {
		t.Fatal("nil tree returned waiter")
	}
	want := errors.New("finish")
	if err := (*processTree)(nil).finish(want); !errors.Is(err, want) {
		t.Fatalf("nil finish = %v", err)
	}
	if err := (&processTree{generation: &DarwinGeneration{}}).interrupt(); err != nil {
		t.Fatalf("generation interrupt = %v", err)
	}
	if err := (&processTree{generation: &DarwinGeneration{}}).kill(); err != nil {
		t.Fatalf("generation kill = %v", err)
	}

	launch := &processTreeCommand{
		cmd: exec.Command("/bin/true"),
		acquireNative: func() (func(), error) {
			return nil, want
		},
	}
	if _, err := startProcessTree(launch); !errors.Is(err, want) {
		t.Fatalf("acquire failure = %v", err)
	}
}
