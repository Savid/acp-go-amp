.DEFAULT_GOAL := help
.PHONY: audit build clean coverage-check docs-audit fmt fmt-check help lint modernize-check test test/cover test-integration test-integration-cover test-integration-live test-integration-smoke tidy vuln

GOLANGCI_LINT_VERSION ?= v2.12.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

REMOVED_PUBLIC_TERMS = amp\x20acp|pro\x78y|compatibilit\x79|deprecat\x65d|legac\x79|migratio\x6e|session/imp\x6frt|sdkMessag\x65|emitRawSDKMessag\x65s|setGoa\x6c|goa\x6cs|\x4e\x45\x53|SSE\x20MCP|mcpCapabilities\x2eacp|ExportSessio\x6e|ImportSessio\x6e|DeleteSessio\x6e|ParseConfi\x67|AmpSessio\x6e|dangerouslyAllowAll|nativeExp\x6frt

## build: build all packages
build:
	go build ./...

## fmt-check: require gofmt-clean Go files
fmt-check:
	@test -z "$$(gofmt -l .)"

## test: run tests with race detector and coverage
test:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...

## coverage-check: require 100% statement coverage
coverage-check: test
	@go tool cover -func=coverage.out | awk 'BEGIN { found = 0 } /^total:/ { found = 1; if ($$3 != "100.0%") { printf "total coverage %s, want 100.0%%\n", $$3; exit 1 } printf "total coverage %s\n", $$3 } END { if (!found) { print "missing total coverage line"; exit 1 } }'

## test-integration-smoke: run integration tests that do not spend model tokens
test-integration-smoke:
	go test -race -count=1 -timeout=120s -run Smoke ./integration/...

## test-integration-live: run live integration tests that spend model tokens
test-integration-live:
	ACP_GO_AMP_LIVE=1 go test -race -count=1 -timeout=180s -run Live -v ./integration/...

## test-integration: alias for live integration tests
test-integration: test-integration-live

## test-integration-cover: run smoke integration tests with compiled binary coverage
test-integration-cover:
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; mkdir -p "$$tmp/data"; go build -cover -coverpkg=./... -o "$$tmp/acp-go-amp" ./cmd/acp-go-amp; ACP_GO_AMP_AGENT_BINARY="$$tmp/acp-go-amp" GOCOVERDIR="$$tmp/data" go test -race -count=1 -timeout=120s -run Smoke -v ./integration/...; go tool covdata percent -i="$$tmp/data"; go tool covdata textfmt -i="$$tmp/data" -o coverage-integration.out

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

## vuln: run govulncheck
vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

## modernize-check: check Go modernizations without changing files
modernize-check:
	@tmp=$$(mktemp); if ! go fix -n ./... >"$$tmp" 2>&1; then cat "$$tmp"; rm -f "$$tmp"; exit 1; fi; rm -f "$$tmp"

## docs-audit: check public docs, examples, flags, and removed terms
docs-audit:
	@pattern=$$(printf '%b' '$(REMOVED_PUBLIC_TERMS)'); ! rg -n -- "$$pattern" README.md doc.go docs.json docs examples cmd/acp-go-amp/*.go AGENTS.md
	@test -f README.md
	@test -f doc.go
	@test -f example_test.go
	@test -f docs.json
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
	@test -f docs/reference/acp-methods.mdx
	@test -f docs/reference/cli.mdx
	@test -f docs/reference/go-api.mdx
	@test -f docs/reference/meta.mdx
	@test -f docs/reference/updates.mdx
	@rg -q 'flags.StringVar\(&path, "path"' cmd/acp-go-amp/main.go
	@rg -q 'flags.StringVar\(&home, "home"' cmd/acp-go-amp/main.go
	@rg -q 'flags.StringVar\(&model, "model"' cmd/acp-go-amp/main.go
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

## audit: run local checks
audit: fmt-check lint build test coverage-check tidy vuln modernize-check docs-audit
	go mod verify

## clean: remove build artifacts
clean:
	rm -f coverage.out coverage-integration.out coverage-summary.txt acp-go-amp

## test/cover: open HTML coverage report
test/cover: test
	go tool cover -html=coverage.out

## help: show this help
help:
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
