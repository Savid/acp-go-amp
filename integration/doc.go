//go:build integration

// Package integration contains live Amp CLI integration coverage.
//
// Run with both the integration build tag and ACP_GO_AMP_RUN_INTEGRATION=1.
// These tests launch the real local amp binary and require an authenticated
// Amp installation. Token-spending live tests additionally require
// ACP_GO_AMP_RUN_LIVE_TOKENS=1.
package integration
