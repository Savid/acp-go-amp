.DEFAULT_GOAL := help

.PHONY: audit build clean coverage-check docs-audit fmt fmt-check help lint modernize-check test test/cover test-cross-compile test-integration-attended test-integration-cover test-integration-keystore test-integration-live test-integration-native-browser test-integration-smoke test-portable-runtime tidy vuln

GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

## build: build all packages
build:
	go build ./...

## fmt-check: require gofmt-clean Go files
fmt-check:
	@test -z "$$(gofmt -l .)"

GO_TEST_TIMEOUT ?= 40m

## test: run unit tests with race detector and shuffled order
test:
	go test -race -shuffle=on -timeout=$(GO_TEST_TIMEOUT) ./...

## coverage-check: run shuffled race tests and report statement coverage
coverage-check:
	go test -race -shuffle=on -coverprofile=coverage.out -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) ./...
	@awk 'NR > 1 && $$(NF - 1) > 0 { found = 1 } END { if (!found) { print "coverage profile has no statement blocks"; exit 1 } }' coverage.out
	@report=$$(go tool cover -func=coverage.out) || exit $$?; printf '%s\n' "$$report" | awk '/^total:/ { found = 1; if ($$3 !~ /^[0-9]+([.][0-9]+)?%$$/) { print "invalid total coverage line"; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-cross-compile: compile supported packages and fail-closed platform paths
test-cross-compile:
	rm -rf .tmp/cross
	mkdir -p .tmp/cross
	GOOS=linux GOARCH=amd64 go test -c -o .tmp/cross/amp-linux.test ./internal/amp
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/amp-darwin.test ./internal/amp
	GOOS=darwin GOARCH=arm64 go test -c -o .tmp/cross/amp-cmd-darwin.test ./cmd/acp-go-amp
	GOOS=darwin GOARCH=arm64 go build ./...
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/amp-windows.test ./internal/amp
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/amp-cmd-windows.test ./cmd/acp-go-amp
	GOOS=windows GOARCH=amd64 go test -c -o .tmp/cross/amp-root-windows.test .
	GOOS=freebsd GOARCH=amd64 go build ./...
	GOOS=openbsd GOARCH=amd64 go build ./...
	GOOS=windows GOARCH=amd64 go build ./...

## test-portable-runtime: execute the portable ordinary-lifecycle suite on a non-Unix host
# Cross-compilation is structural evidence only. This target refuses to run on a
# host whose build tags select a Unix process backend, so the portable tree can
# never be reported as covered by a machine that never executes it.
test-portable-runtime:
	@GO_TEST_TIMEOUT=$(GO_TEST_TIMEOUT) .github/scripts/run-portable-runtime.sh

## test-integration-smoke: run integration tests that do not spend model tokens
test-integration-smoke:
	ACP_GO_AMP_RUN_LIVE_TOKENS=0 ACP_GO_AMP_RUN_ATTENDED=0 ACP_GO_AMP_RUN_KEYSTORE=0 ACP_GO_AMP_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=120s -run Smoke ./integration/...

## test-integration-live: run live integration tests that spend model tokens
test-integration-live:
	ACP_GO_AMP_RUN_ATTENDED=0 ACP_GO_AMP_RUN_KEYSTORE=0 ACP_GO_AMP_RUN_INTEGRATION=1 ACP_GO_AMP_RUN_LIVE_TOKENS=1 go test -race -count=1 -tags=integration -timeout=180s -run Live -v ./integration/...

## test-integration-attended: run the provider-auth login a human must approve in real time
test-integration-attended:
	@set -eu; dir=$$(mktemp -d); trap 'rm -rf "$$dir"' EXIT HUP INT TERM; \
	dir=$$(cd "$$dir" && pwd); \
	export ACP_GO_AMP_RUN_INTEGRATION=1 ACP_GO_AMP_RUN_ATTENDED=1 ACP_GO_AMP_RUN_LIVE_TOKENS=0 ACP_GO_AMP_RUN_KEYSTORE=0; \
	go test -race -c -tags=integration -o "$$dir/integration.test" ./integration; \
	"$$dir/integration.test" -test.list '^TestAttendedProviderAuth' >"$$dir/selected"; \
	expected=$$(grep -Ec '^TestAttendedProviderAuth' "$$dir/selected" || true); \
	[ "$$expected" -gt 0 ] || { echo 'attended selector discovered no tests'; exit 1; }; \
	{ status=0; (cd integration && "$$dir/integration.test" -test.v -test.count=1 -test.timeout=1200s -test.run '^TestAttendedProviderAuth') 2>&1 || status=$$?; echo "$$status" >"$$dir/status"; } | tee "$$dir/output"; \
	status=$$(cat "$$dir/status"); passed=$$(grep -Ec '^--- PASS: TestAttendedProviderAuth' "$$dir/output" || true); \
	skipped=$$(grep -Ec '^[[:space:]]*--- SKIP:' "$$dir/output" || true); empty=$$(grep -c 'no tests to run' "$$dir/output" || true); \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq "$$expected" ] || { echo "attended tests passed $$passed of $$expected"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'attended test skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'attended selector ran no tests'; exit 1; }

## test-integration-keystore: run the three-configuration credential-residence matrix
test-integration-keystore:
	ACP_GO_AMP_RUN_LIVE_TOKENS=0 ACP_GO_AMP_RUN_ATTENDED=0 ACP_GO_AMP_RUN_INTEGRATION=1 ACP_GO_AMP_RUN_KEYSTORE=1 go test -race -count=1 -tags=integration -timeout=900s -v -run TestKeystore ./...

## test-integration-native-browser: require one pinned native Linux launcher-interception proof
test-integration-native-browser:
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ACP_GO_AMP_RUN_LIVE_TOKENS=0 ACP_GO_AMP_RUN_ATTENDED=0 ACP_GO_AMP_RUN_KEYSTORE=0 ACP_GO_AMP_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration,browsercanary -timeout=1200s -v -run '^TestNativeBrowserPinnedLinuxAmpLoginExecsOnlyShimLauncher$$' ./integration/... 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -c '^--- PASS: TestNativeBrowserPinnedLinuxAmpLoginExecsOnlyShimLauncher ' "$$log" || true); skipped=$$(grep -Ec '^[[:space:]]*--- SKIP: TestNativeBrowserPinnedLinuxAmpLoginExecsOnlyShimLauncher(/| )' "$$log" || true); empty=$$(grep -c 'no tests to run' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq 1 ] || { echo "native browser pass count $$passed, want exactly 1"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'required native browser canary skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'required native browser selector ran no tests'; exit 1; }

## test-integration-cover: run smoke integration tests with compiled binary coverage
test-integration-cover:
	@set -eu; mkdir -p .tmp; dir=$$(mktemp -d "$$(pwd)/.tmp/integration-cover.XXXXXX"); trap 'rm -rf "$$dir"' EXIT HUP INT TERM; \
	mkdir "$$dir/data"; \
	go build -cover -coverpkg=./... -o "$$dir/acp-go-amp" ./cmd/acp-go-amp; \
	{ status=0; ACP_GO_AMP_RUN_LIVE_TOKENS=0 ACP_GO_AMP_RUN_ATTENDED=0 ACP_GO_AMP_RUN_KEYSTORE=0 ACP_GO_AMP_RUN_INTEGRATION=1 ACP_GO_AMP_AGENT_BINARY="$$dir/acp-go-amp" GOCOVERDIR="$$dir/data" go test -race -count=1 -tags=integration -timeout=120s -run Smoke -v ./integration/... 2>&1 || status=$$?; echo "$$status" >"$$dir/status"; } | tee "$$dir/output"; \
	status=$$(cat "$$dir/status"); [ "$$status" -eq 0 ] || exit "$$status"; \
	[ -n "$$(find "$$dir/data" -name 'covcounters.*' -type f -size +0c -print -quit)" ] || { echo 'compiled adapter produced no coverage counters'; exit 1; }; \
	go tool covdata percent -i="$$dir/data"; \
	go tool covdata textfmt -i="$$dir/data" -o coverage-integration.out

## lint: run golangci-lint
lint:
	$(GOLANGCI_LINT) run --timeout=10m --allow-parallel-runners ./...

## fmt: format Go files
fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')
	$(GOLANGCI_LINT) fmt ./...

## tidy: verify module files are tidy
tidy:
	go mod tidy -diff

## vuln: run govulncheck from the go.mod tool directive
# golang.org/x/vuln v1.4.0 panics in x/tools SSA on Go 1.26 generics;
# keep the tool directive pinned at v1.5.0 or newer.
vuln:
	go tool govulncheck ./...

## modernize-check: check Go modernizations without changing files
modernize-check:
	go fix -diff ./...

## docs-audit: check required docs, examples, and documented flags
docs-audit:
	@test -f README.md
	@test -f doc.go
	@test -f AGENTS.md
	@test -f example_test.go
	@test -f docs.json
	@test -f docs/get-started/examples.mdx
	@test -f docs/features/authentication.mdx
	@test -f examples/minimal-client/main.go
	@test -f examples/interactive-chat/main.go
	@test -f examples/resume-from-file/main.go
	@test -f docs/overview.mdx
	@test -f docs/get-started/install.mdx
	@test -f docs/get-started/quickstart.mdx
	@test -f docs/get-started/run-modes.mdx
	@test -f docs/core/sessions.mdx
	@test -f docs/core/prompt-streaming.mdx
	@test -f docs/features/session-store.mdx
	@test -f docs/features/models-config.mdx
	@test -f docs/features/mcp.mdx
	@test -f docs/features/permissions.mdx
	@test -f docs/features/elicitation.mdx
	@test -f docs/features/raw-events.mdx
	@test -f docs/operations/security.mdx
	@test -f docs/operations/observability.mdx
	@test -f docs/operations/troubleshooting.mdx
	@test -f docs/reference/acp-methods.mdx
	@test -f docs/reference/cli.mdx
	@test -f docs/reference/go-api.mdx
	@test -f docs/reference/meta.mdx
	@test -f docs/reference/updates.mdx
	@rg -q 'flags.StringVar\(&path, "path"' cmd/acp-go-amp/main.go
	@rg -q 'flags.StringVar\(&home, "home"' cmd/acp-go-amp/main.go
	@rg -q 'flags.StringVar\(&model, "model"' cmd/acp-go-amp/main.go
	@rg -q 'flags.StringVar\(&providerAuthRoot, "provider-auth-root"' cmd/acp-go-amp/main.go
	@rg -q 'flags.StringVar\(&providerAuthDirectHome, "provider-auth-direct-home"' cmd/acp-go-amp/main.go
	@rg -q 'flags.BoolVar\(&debug, "debug"' cmd/acp-go-amp/main.go
	@rg -q 'flags.BoolVar\(&showVersion, "version"' cmd/acp-go-amp/main.go
	@rg -q 'local transcript restore is not native thread resurrection' README.md docs/features/session-store.mdx
	@rg -q 'continuation requires the live server-side Amp thread and AMP_API_KEY' README.md docs/features/session-store.mdx
	@rg -q 'session/load can replay the local transcript for display' docs/features/session-store.mdx
	@rg -q 'native_state_missing' docs/features/session-store.mdx docs/reference/updates.mdx
	@rg -q 'one `Replace` generation' docs/features/session-store.mdx
	@rg -q 'NativeEnvironment' docs/get-started/run-modes.mdx docs/reference/go-api.mdx
	@rg -q 'WithHostAuthority' README.md docs/reference/go-api.mdx docs/operations/security.mdx
	@rg -q 'No slash commands are advertised' docs/reference/acp-methods.mdx docs/core/prompt-streaming.mdx
	@rg -q '_amp/session/fork.*unsupported' README.md docs/reference/acp-methods.mdx
	@rg -q 'never sends `session/request_permission`' docs/features/permissions.mdx
	@rg -q 'does not set the native allow-all setting' docs/features/permissions.mdx
	@rg -q 'does not advertise Amp elicitation metadata' docs/features/elicitation.mdx
	@rg -q 'promptCapabilities.image: true' docs/reference/meta.mdx
	@rg -q '921,600 decoded bytes per image' docs/features/models-config.mdx
	@rg -q 'acp-go.dev/mediaEnvelope' docs/reference/meta.mdx
	@rg -q 'documentFormats' docs/reference/meta.mdx
	@rg -q 'acp-go.dev/handoff' docs/reference/meta.mdx docs/features/models-config.mdx
	@rg -q 'WithInputHandoffRoot' README.md docs/reference/go-api.mdx docs/operations/security.mdx
	@rg -q 'handoff_digest_mismatch' docs/features/models-config.mdx
	@rg -q 'invalid_handoff' docs/features/models-config.mdx
	@rg -q '_artifacts/images/<digest>.json' docs/features/session-store.mdx
	@rg -q 'MaxOutputBytesPerToolCall' docs/reference/go-api.mdx
	@rg -q 'WithProviderAuthRoot' README.md docs/reference/go-api.mdx docs/reference/cli.mdx docs/features/authentication.mdx
	@rg -q '_amp/auth/credential' docs/reference/acp-methods.mdx docs/features/authentication.mdx
	@rg -q 'amp_auth_failed' docs/reference/acp-methods.mdx
	@rg -q 'no Amp-side' docs/features/authentication.mdx
	@rg -q 'AMP_DISABLE_SECRET_REDACTION' docs/operations/security.mdx
	@rg -q 'acp-go.dev/lifecycle' docs/reference/meta.mdx docs/reference/updates.mdx
	@rg -q "response's own top-level" docs/reference/meta.mdx
	@rg -q 'never inside\n?.agentCapabilities._meta' -U docs/reference/meta.mdx
	@rg -q '\{"version": 1\}' docs/reference/meta.mdx
	@rg -q '"updatesOutsidePrompt": false' docs/reference/meta.mdx
	@rg -q '"authoritativeQuiescence": false' docs/reference/meta.mdx
	@rg -q '"quiescenceSource": "process-containment"' docs/reference/meta.mdx
	@rg -q '"activityKinds": \[\]' docs/reference/meta.mdx
	@rg -q 'one incarnation per prompt' -i docs/reference/meta.mdx
	@rg -q 'identity-only .session_info_update' docs/reference/meta.mdx docs/reference/updates.mdx
	@rg -q 'no envelope attached' docs/reference/meta.mdx docs/reference/updates.mdx
	@rg -q 'authenticate.*_amp/auth/\*.*\n.*_amp/session/fork.*rejects the key' -U docs/reference/meta.mdx
	@rg -q 'session/set_mode. does not exist on this adapter' docs/reference/meta.mdx
	@rg -q 'no action correlation value is' docs/reference/meta.mdx
	@rg -q 'claimed before delivery is attempted' docs/reference/meta.mdx

## audit: run local checks
audit: fmt-check lint build coverage-check test-cross-compile tidy vuln modernize-check docs-audit
	go mod verify

## clean: remove build artifacts
clean:
	rm -rf .tmp coverage.out coverage-integration.out coverage-summary.txt

## test/cover: open HTML coverage report
test/cover: coverage-check
	go tool cover -html=coverage.out

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
