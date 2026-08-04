//go:build !linux

package ampacp

import (
	"fmt"
	"os"
	"runtime"
)

func handoffGeneratedNativeTreePlatform(_ string, uid uint32, gid uint32) error {
	if uid == uint32(os.Geteuid()) && gid == uint32(os.Getegid()) {
		return nil
	}

	return fmt.Errorf("native path ownership handoff is unsupported on %s", runtime.GOOS)
}

func validateNativeOwnedDirectoryPlatform(_ string, uid uint32, gid uint32) error {
	if uid == uint32(os.Geteuid()) && gid == uint32(os.Getegid()) {
		return nil
	}

	return fmt.Errorf("native path ownership validation is unsupported on %s", runtime.GOOS)
}

func writeNativeOwnedFilePlatform(path string, contents []byte, uid uint32, gid uint32) error {
	if uid == uint32(os.Geteuid()) && gid == uint32(os.Getegid()) {
		return os.WriteFile(path, contents, 0o600)
	}

	return fmt.Errorf("native path ownership handoff is unsupported on %s", runtime.GOOS)
}
