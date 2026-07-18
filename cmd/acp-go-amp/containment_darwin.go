//go:build darwin

package main

import (
	"io"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func diagnoseContainment(scratchDir string, output io.Writer) error {
	return nativeamp.DiagnoseDarwinContainment(scratchDir, output)
}

func cleanupContainment(scratchDir, runtimeID string, force bool, output io.Writer) error {
	return nativeamp.CleanupDarwinContainment(scratchDir, runtimeID, force, output)
}
