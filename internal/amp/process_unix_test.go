//go:build unix

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestSignalProcessGroupErrors(t *testing.T) {
	originalGetpgid := syscallGetpgid
	originalKill := syscallKill
	t.Cleanup(func() {
		syscallGetpgid = originalGetpgid
		syscallKill = originalKill
	})

	cmd := &exec.Cmd{Process: &os.Process{Pid: 12345}}

	syscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
	if err := killProcess(cmd); err != nil {
		t.Fatalf("getpgid ESRCH should map to nil, got %v", err)
	}

	syscallGetpgid = func(int) (int, error) { return 0, syscall.EPERM }
	if err := killProcess(cmd); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("getpgid EPERM should propagate, got %v", err)
	}

	syscallGetpgid = func(pid int) (int, error) { return pid, nil }
	syscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	if err := killProcess(cmd); err != nil {
		t.Fatalf("kill ESRCH should map to nil, got %v", err)
	}

	syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }
	if err := interruptProcess(cmd); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("kill EPERM should propagate, got %v", err)
	}
}

func TestInterruptReturnsSignalError(t *testing.T) {
	originalGetpgid := syscallGetpgid
	t.Cleanup(func() { syscallGetpgid = originalGetpgid })

	syscallGetpgid = func(int) (int, error) { return 0, syscall.EPERM }

	turn := &Turn{cmd: &exec.Cmd{Process: &os.Process{Pid: 12345}}}
	if err := turn.Interrupt(context.Background(), time.Second); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Interrupt should propagate signal error, got %v", err)
	}
}
