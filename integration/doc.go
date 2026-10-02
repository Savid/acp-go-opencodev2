// Package integration holds the tests that run against an installed OpenCode.
//
// The tests are behind the integration build tag and ACP_GO_OPENCODEV2_RUN_INTEGRATION=1.
// The smoke tier spends no model tokens; ACP_GO_OPENCODEV2_RUN_LIVE_TOKENS=1 enables
// prompts that do.
//
// ACP_GO_OPENCODEV2_HARNESS_PATH selects the harness binary, which otherwise comes
// from PATH; an absent binary skips. Live tests use temporary native homes
// and authentication supplied through the native environment.
// ACP_GO_OPENCODEV2_MODEL selects the model for live tests.
package integration
