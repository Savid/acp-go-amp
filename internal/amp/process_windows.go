//go:build windows

package amp

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type processTree struct {
	mu  sync.Mutex
	job windows.Handle
}

type jobBasicAccountingInformation struct {
	TotalUserTime             int64
	TotalKernelTime           int64
	ThisPeriodTotalUserTime   int64
	ThisPeriodTotalKernelTime int64
	TotalPageFaultCount       uint32
	TotalProcesses            uint32
	ActiveProcesses           uint32
	TotalTerminatedProcesses  uint32
}

func (t *processTree) descendantCount() (int, bool) {
	if t == nil {
		return 0, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.job == 0 {
		return 0, false
	}

	var info jobBasicAccountingInformation
	if err := windows.QueryInformationJobObject(
		t.job,
		windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
		nil,
	); err != nil {
		return 0, false
	}

	return int(info.ActiveProcesses), true
}

func configureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_SUSPENDED}
}

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	cmd := launch.cmd
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		launch.close()

		return nil, fmt.Errorf("create amp containment job: %w", err)
	}

	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)),
		uint32(unsafe.Sizeof(limits)),
	); err != nil {
		_ = windows.CloseHandle(job)

		return nil, fmt.Errorf("configure amp containment job: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = windows.CloseHandle(job)
		launch.close()

		return nil, err
	}
	launch.releaseInherited()

	var assignErr error
	if err := cmd.Process.WithHandle(func(handle uintptr) {
		assignErr = windows.AssignProcessToJobObject(job, windows.Handle(handle))
	}); err != nil {
		assignErr = err
	}
	if assignErr != nil {
		return nil, abortSuspendedProcess(cmd, job, fmt.Errorf("assign amp process to containment job: %w", assignErr))
	}

	if err := resumeProcessThreads(uint32(cmd.Process.Pid)); err != nil {
		return nil, abortSuspendedProcess(cmd, job, fmt.Errorf("resume contained amp process: %w", err))
	}
	launch.control = nil

	return &processTree{job: job}, nil
}

func abortSuspendedProcess(cmd *exec.Cmd, job windows.Handle, cause error) error {
	tree := &processTree{job: job}
	terminateErr := tree.kill()
	waitErr := cmd.Wait()
	quiescenceErr := tree.terminateAndWait(defaultCloseWait)

	return errors.Join(cause, terminateErr, waitErr, quiescenceErr)
}

func resumeProcessThreads(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot) //nolint:errcheck // The enumeration result is authoritative.

	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	if err := windows.Thread32First(snapshot, &entry); err != nil {
		return err
	}

	resumed := false
	for {
		if entry.OwnerProcessID == pid {
			thread, openErr := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
			if openErr != nil {
				return openErr
			}

			_, resumeErr := windows.ResumeThread(thread)
			closeErr := windows.CloseHandle(thread)
			if resumeErr != nil || closeErr != nil {
				return errors.Join(resumeErr, closeErr)
			}

			resumed = true
		}

		if err := windows.Thread32Next(snapshot, &entry); err != nil {
			if errors.Is(err, syscall.ERROR_NO_MORE_FILES) {
				break
			}

			return err
		}
	}

	if !resumed {
		return errors.New("contained amp process has no resumable thread")
	}

	return nil
}

func (t *processTree) interrupt() error { return t.kill() }

func (t *processTree) kill() error {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.job == 0 {
		return nil
	}

	return windows.TerminateJobObject(t.job, 1)
}

func (t *processTree) terminateAndWait(timeout time.Duration) error {
	if t == nil {
		return nil
	}
	if err := t.kill(); err != nil {
		return fmt.Errorf("%w: terminate Windows job: %w", ErrProcessTreeNotQuiescent, err)
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		t.mu.Lock()
		job := t.job
		if job == 0 {
			t.mu.Unlock()

			return nil
		}

		var info jobBasicAccountingInformation
		err := windows.QueryInformationJobObject(
			job,
			windows.JobObjectBasicAccountingInformation,
			uintptr(unsafe.Pointer(&info)),
			uint32(unsafe.Sizeof(info)),
			nil,
		)
		if err == nil && info.ActiveProcesses == 0 {
			err = windows.CloseHandle(job)
			if err == nil {
				t.job = 0
			}
		}
		t.mu.Unlock()

		if err != nil {
			return fmt.Errorf("%w: inspect Windows job: %w", ErrProcessTreeNotQuiescent, err)
		}
		if info.ActiveProcesses == 0 {
			return nil
		}

		select {
		case <-deadline.C:
			return fmt.Errorf("%w: Windows job remained active", ErrProcessTreeNotQuiescent)
		case <-ticker.C:
		}
	}
}

func interruptProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	return cmd.Process.Kill()
}

func killProcess(cmd *exec.Cmd) error { return interruptProcess(cmd) }
