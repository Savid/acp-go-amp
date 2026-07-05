# acp-go-amp

## Purpose

This module exposes the Amp CLI as a Go ACP agent. Keep it aligned with
`/home/savid/ai/siblings/CONTRACT.md` and the HA Amp rulings.

## Project Map

- Root package `ampacp`: ACP agent methods, request builders, metadata parsing,
  raw events, config options, and session-store API.
- `internal/amp`: Amp process boundary, stream-json parsing, environment
  construction, and interrupt handling.
- `cmd/acp-go-amp`: stdio ACP command with only `-path`, `-home`, `-model`,
  `-debug`, and `-version` flags.
- `examples`: embeddable host examples that must stay covered by tests.
- `integration`: smoke and live tests for installed Amp binaries.
- `docs`: public documentation mirrored by `docs.json` navigation.

## Commands

- `make test`: unit tests.
- `make coverage-check`: 100.0% total statement coverage gate.
- `make lint`: pinned golangci-lint.
- `make docs-audit`: public-doc forbidden-term and Amp semantics audit.
- `make audit`: local release gate.
- `make test-integration-live`: live Amp prompt tests gated by
  `ACP_GO_AMP_LIVE=1`.

## Coding Rules

- Keep the public package surface identical to the sibling contract with Amp
  names substituted.
- Do not add permission bridging, command catalogs, model config options, or
  fork behavior without a director ruling.
- Keep process handling simple: one short-lived `amp threads continue` process
  per prompt, isolated settings file, dedicated stdout/stderr pipes.
- Preserve raw Amp stream JSON bytes in the `transcript` store subpath.
- Do not persist auth, settings, API keys, or other secrets.

## Testing Rules

- New code must keep `make coverage-check` at 100.0%.
- Fake Amp binaries and generated test files must live under `t.TempDir()` or
  another ignored path outside the repository tree.
- Conformance tests must pin strict `_meta.amp` handling, no fork capability,
  no elicitation metadata, command silence, MCP accept/reject behavior, and
  backpressure errors.
- Live tests may spend tokens only when explicitly env-gated.

## Security And Boundaries

- `AMP_API_KEY` and `AMP_URL` are injected from live process environment or
  options; they are never written to `SessionStore`.
- `session/load` replays local transcript frames for display; it does not create
  a new server-side thread.
- Native continuation requires the original server-side Amp thread to still
  exist.
