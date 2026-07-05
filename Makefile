GO ?= go
GOLANGCI_LINT_VERSION ?= v2.12.2
GOVULNCHECK_VERSION ?= latest

.PHONY: help build test coverage-check test-integration-smoke test-integration-live test-integration-cover lint fmt tidy vuln modernize-check audit docs-audit clean

help:
	@printf '%s\n' 'targets: build test coverage-check test-integration-smoke test-integration-live test-integration-cover lint fmt tidy vuln modernize-check audit docs-audit clean'

build:
	$(GO) build ./...
	$(GO) build ./cmd/acp-go-amp

test:
	$(GO) test ./...

coverage-check:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | awk '/^total:/ { if ($$3 != "100.0%") { print "coverage " $$3 " below 100.0%"; exit 1 } }'

test-integration-smoke:
	$(GO) test ./integration -run Smoke

test-integration-live:
	ACP_GO_AMP_LIVE=1 $(GO) test ./integration -run Live

test-integration-cover:
	$(GO) test -coverprofile=coverage-integration.out ./integration

lint:
	$(GO) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

fmt:
	$(GO) fmt ./...
	@test -z "$$(gofmt -l .)"

tidy:
	$(GO) mod tidy
	$(GO) mod verify

vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

modernize-check:
	$(GO) fix -n ./...

audit: fmt lint test coverage-check vuln modernize-check docs-audit

docs-audit:
	@missing=0; for f in README.md doc.go AGENTS.md docs.json docs/overview.mdx docs/get-started/quickstart.mdx docs/get-started/install.mdx docs/get-started/run-modes.mdx docs/get-started/examples.mdx docs/core/sessions.mdx docs/core/prompt-streaming.mdx docs/features/authentication.mdx docs/features/mcp.mdx docs/features/models-config.mdx docs/features/permissions.mdx docs/features/elicitation.mdx docs/features/session-store.mdx docs/features/raw-events.mdx docs/reference/go-api.mdx docs/reference/cli.mdx docs/reference/acp-methods.mdx docs/reference/meta.mdx docs/reference/updates.mdx docs/operations/security.mdx docs/operations/observability.mdx; do test -f "$$f" || { echo "missing $$f"; missing=1; }; done; exit $$missing

clean:
	rm -f coverage.out coverage-integration.out acp-go-amp
