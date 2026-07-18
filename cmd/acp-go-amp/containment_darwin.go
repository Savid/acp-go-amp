//go:build darwin

package main

import (
	"io"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

var diagnoseContainment = func(scratchDir string, output io.Writer) error {
	return nativeamp.DiagnoseDarwinContainment(scratchDir, output)
}

var cleanupContainment = func(scratchDir, runtimeID string, force bool, output io.Writer) error {
	return nativeamp.CleanupDarwinContainment(scratchDir, runtimeID, force, output)
}
