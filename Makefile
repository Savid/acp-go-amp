.DEFAULT_GOAL := help

.PHONY: test-trusted-supervisor
.PHONY: audit build clean coverage-check docs-audit fmt fmt-check help lint modernize-check test test/cover test-cross-compile test-integration-attended test-integration-cover test-integration-keystore test-integration-live test-integration-native-browser test-integration-smoke test-portable-runtime tidy vuln vuln-sarif

GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

REMOVED_PUBLIC_TERMS = amp\x20acp|pro\x78y|compatibilit\x79|deprecat\x65d|legac\x79|migratio\x6e|session/imp\x6frt|sdkMessag\x65|emitRawSDKMessag\x65s|setGoa\x6c|goa\x6cs|\x4e\x45\x53|SSE\x20MCP|mcpCapabilities\x2eacp|ExportSessio\x6e|ImportSessio\x6e|DeleteSessio\x6e|ParseConfi\x67|AmpSessio\x6e|dangerouslyAllowAll|nativeExp\x6frt

## build: build all packages
build:
	go build ./...

## fmt-check: require gofmt-clean Go files
fmt-check:
	@test -z "$$(gofmt -l .)"

# Every isolated native launch claims the standalone agent identity, and that
# claim proves the identity vacant across every task in the PID namespace. In
# the initial namespace the suite requires, that is the whole host, so the
# adapter package runs far past the ten-minute default.
GO_TEST_TIMEOUT ?= 40m

## test: run unit tests with race detector and shuffled order
test:
	go test -race -shuffle=on -timeout=$(GO_TEST_TIMEOUT) ./...

## test-trusted-supervisor: run Linux root-only native authority tests
test-trusted-supervisor:
	@test "$$(uname -s)" = Linux
	@test "$$(id -u)" -eq 0
	@for directory in /var/lib/acp-go /var/lib/acp-go/agent-identities; do if [ ! -e "$$directory" ] && [ ! -L "$$directory" ]; then install -d -o root -g root -m 0700 "$$directory"; fi; [ "$$(stat -c '%F %u %g %a' -- "$$directory")" = 'directory 0 0 700' ] || { echo "unsafe trusted-supervisor authority directory $$directory" >&2; exit 1; }; done
	@selector='^(Test.*(ProcessIsolationActual|TrustedSupervisor|SupervisorGuardianSIGKILL|SupervisorLivenessSIGKILL|GeneratedNative|BorrowedIdentityAdoption|BorrowedDomainAdoption|BorrowedDisposition|AgentIdentityLock|AgentStandalone|AuthorityDomain|IdentityDisposition|PersistentProof|SupervisorConfigIsSealed|CommandCreatorThread|ProviderCreator|SecurityLimits).*)$$'; listing=$$(mktemp); log=$$(mktemp); rc=$$(mktemp); module=$$(go list -m); status=$$?; \
	[ "$$status" -eq 0 ] || { rm -f "$$listing" "$$log" "$$rc"; exit "$$status"; }; \
	go test -list "$$selector" ./... >"$$listing"; status=$$?; \
	[ "$$status" -eq 0 ] || { rm -f "$$listing" "$$log" "$$rc"; exit "$$status"; }; \
	required='TrustedSupervisor SupervisorGuardianSIGKILL SupervisorGuardianSIGKILLBeforeNativeLaunchRefusesStartAndCompletesAfterECHILD SupervisorLivenessSIGKILL GeneratedNative BorrowedIdentityAdoption BorrowedDomainAdoption BorrowedDisposition AgentIdentityLock AgentStandalone AuthorityDomain IdentityDisposition CommandCreatorThread SecurityLimits ProcessIsolationActual'; case "$$module" in github.com/savid/acp-go-amp|github.com/savid/acp-go-claude|github.com/savid/acp-go-hermes|github.com/savid/acp-go-pi) ;; github.com/savid/acp-go-codex|github.com/savid/acp-go-opencode) required="$$required PersistentProof SupervisorConfigIsSealed ProviderCreator" ;; *) rm -f "$$listing" "$$log" "$$rc"; echo "unrecognized trusted-supervisor module $$module"; exit 1 ;; esac; \
	for class in $$required; do grep -Eq "^Test.*$${class}" "$$listing" || { rm -f "$$listing" "$$log" "$$rc"; echo "trusted-supervisor selector discovered no $${class} tests"; exit 1; }; done; \
	expected=$$(grep -Ec '^Test' "$$listing" || true); rm -f "$$listing"; \
	[ "$$expected" -gt 0 ] || { rm -f "$$log" "$$rc"; echo 'trusted-supervisor selector discovered no tests'; exit 1; }; \
	{ go test -race -count=1 -json -run "$$selector" ./...; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -Ec '"Action":"pass","Package":"[^"]+","Test":"Test[^/"]+"' "$$log" || true); skipped=$$(grep -Ec '"Action":"skip","Package":"[^"]+","Test":"Test[^"]+"' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq "$$expected" ] || { echo "trusted-supervisor pass count $$passed, want $$expected"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo "trusted-supervisor skip count $$skipped, want 0"; exit 1; }

## coverage-check: require 100% statement coverage with race instrumentation
coverage-check:
	go test -race -coverprofile=coverage.out -covermode=atomic -timeout=$(GO_TEST_TIMEOUT) ./...
	@awk 'NR > 1 && $$(NF - 1) > 0 && $$NF == 0 { print "uncovered statement block: " $$0; missed = 1 } END { if (missed) exit 1 }' coverage.out
	@go tool cover -func=coverage.out | awk 'BEGIN { found = 0 } /^total:/ { found = 1; if ($$3 != "100.0%") { printf "total coverage %s, want 100.0%%\n", $$3; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

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
	ACP_GO_AMP_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=120s -run Smoke ./integration/...

## test-integration-live: run live integration tests that spend model tokens
test-integration-live:
	ACP_GO_AMP_RUN_INTEGRATION=1 ACP_GO_AMP_RUN_LIVE_TOKENS=1 go test -race -count=1 -tags=integration -timeout=180s -run Live -v ./integration/...

## test-integration-attended: run the provider-auth login a human must approve in real time
test-integration-attended:
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ACP_GO_AMP_RUN_INTEGRATION=1 ACP_GO_AMP_RUN_ATTENDED=1 go test -race -count=1 -tags=integration -timeout=1200s -v -run TestAttended ./integration/... 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); ran=$$(grep -c '^--- PASS: TestAttended' "$$log"); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$ran" -gt 0 ] || { echo 'no attended provider-auth login ran: -run TestAttended selected nothing'; exit 1; }

## test-integration-keystore: run the three-configuration credential-residence matrix
test-integration-keystore:
	ACP_GO_AMP_RUN_INTEGRATION=1 ACP_GO_AMP_RUN_KEYSTORE=1 go test -race -count=1 -tags=integration -timeout=900s -v -run TestKeystore ./...

## test-integration-native-browser: require one pinned native Linux launcher-interception proof
test-integration-native-browser:
	@log=$$(mktemp); rc=$$(mktemp); \
	{ ACP_GO_AMP_RUN_INTEGRATION=1 go test -race -count=1 -tags=integration -timeout=1200s -v -run '^TestNativeBrowserPinnedLinuxAmpLoginExecsOnlyShimLauncher$$' ./integration/... 2>&1; echo $$? >"$$rc"; } | tee "$$log"; \
	status=$$(cat "$$rc"); passed=$$(grep -c '^--- PASS: TestNativeBrowserPinnedLinuxAmpLoginExecsOnlyShimLauncher ' "$$log" || true); skipped=$$(grep -Ec '^[[:space:]]*--- SKIP: TestNativeBrowserPinnedLinuxAmpLoginExecsOnlyShimLauncher(/| )' "$$log" || true); empty=$$(grep -c 'no tests to run' "$$log" || true); \
	rm -f "$$log" "$$rc"; \
	[ "$$status" -eq 0 ] || exit "$$status"; \
	[ "$$passed" -eq 1 ] || { echo "native browser pass count $$passed, want exactly 1"; exit 1; }; \
	[ "$$skipped" -eq 0 ] || { echo 'required native browser canary skipped'; exit 1; }; \
	[ "$$empty" -eq 0 ] || { echo 'required native browser selector ran no tests'; exit 1; }

## test-integration-cover: run smoke integration tests with compiled binary coverage
test-integration-cover:
	rm -rf .tmp/integration-cover coverage-integration.out
	mkdir -p .tmp/integration-cover/data
	go build -cover -coverpkg=./... -o .tmp/integration-cover/acp-go-amp ./cmd/acp-go-amp
	ACP_GO_AMP_RUN_INTEGRATION=1 ACP_GO_AMP_AGENT_BINARY=$$(pwd)/.tmp/integration-cover/acp-go-amp GOCOVERDIR=$$(pwd)/.tmp/integration-cover/data go test -race -count=1 -tags=integration -timeout=120s -run Smoke -v ./integration/...
	go tool covdata percent -i=.tmp/integration-cover/data
	go tool covdata textfmt -i=.tmp/integration-cover/data -o coverage-integration.out

## lint: run golangci-lint
lint:
	$(GOLANGCI_LINT) run ./...

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

## vuln-sarif: run the same pinned govulncheck, emitting SARIF for code scanning
# The CI job uploads this file. It runs the go.mod tool directive rather than a
# third-party action that go-installs an unpinned govulncheck: the family pins
# every tool, and x/vuln before v1.5.0 panics in x/tools SSA on Go 1.26 generics.
vuln-sarif:
	go tool govulncheck -format sarif ./... > govulncheck.sarif

## modernize-check: check Go modernizations without changing files
modernize-check:
	go fix -n ./...

## docs-audit: check public docs, examples, flags, and removed terms
docs-audit:
	@pattern=$$(printf '%b' '$(REMOVED_PUBLIC_TERMS)'); ! rg -n -- "$$pattern" README.md doc.go docs.json docs examples cmd/acp-go-amp/*.go AGENTS.md
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
	@rg -q 'native `HOME` plus `XDG_CONFIG_HOME`' docs/get-started/run-modes.mdx
	@rg -q 'isolated native HOME/XDG state' README.md docs/reference/cli.mdx
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
	@rg -q 'SupervisorGuardianSIGKILL' Makefile
	@rg -q 'SupervisorLivenessSIGKILL' Makefile
	@rg -q 'BorrowedIdentityAdoption' Makefile
	@rg -q 'BorrowedDomainAdoption' Makefile
	@rg -q 'validateInheritedAgentIdentityFlock' internal/amp/agent_identity_lock_linux.go
	@rg -q '/proc/self/fdinfo/' docs/operations/security.mdx

## audit: run local checks
audit: fmt-check lint build test coverage-check test-cross-compile tidy vuln modernize-check docs-audit
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
