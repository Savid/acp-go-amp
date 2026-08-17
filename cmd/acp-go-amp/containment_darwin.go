//go:build darwin

package main

import (
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

var diagnoseContainment = nativeamp.DiagnoseDarwinContainment

var cleanupContainment = nativeamp.CleanupDarwinContainment
