// Package integration holds the tests that run against an installed Amp.
//
// The tests are behind the integration build tag and ACP_GO_AMP_RUN_INTEGRATION=1.
// The smoke tier spends no model tokens; ACP_GO_AMP_RUN_LIVE_TOKENS=1 enables
// prompts that do.
//
// ACP_GO_AMP_HARNESS_PATH selects the harness binary, which otherwise comes
// from PATH; an absent binary skips. ACP_GO_AMP_MODE selects the mode for live
// tests.
package integration
