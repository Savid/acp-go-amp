# AGENTS.md

## Purpose

This module exposes the local Amp CLI through ACP. Native `threads new` creates
the durable thread ID. Each prompt owns one `threads continue` stream-json
process with a temporary native lifecycle plugin; restore also attaches without
input to read remote state. The remote thread remains available to the native
CLI after close.

## Project Map

- `agent*.go`, `options.go`, `request_builders.go`: agent construction,
  environment options, session admission, store coordination, and the vendor
  session-request options that ride core's request builders.
- `session*.go`: prompt process ownership, event projection, lifecycle,
  mode configuration, images, native export reconciliation, and replay.
- `internal/amp`: native commands, process and pipe ownership, and frames.
- `cmd/acp-go-amp`: stdio entrypoint, flags, signals, and telemetry setup.
- `integration`: installed CLI tests behind explicit gates.

## Commands

```sh
make build
make test
make lint
make audit
make test-integration-smoke
make test-integration-live
```

`make test` runs race detection and shuffled order. `make audit` is the complete
local gate. Native smoke requires an authenticated Amp installation; live tests
spend model tokens and require explicit operator intent.

## Coding Rules

- Shared behavior comes from `github.com/savid/acp-go-core`.
- Keep native protocol details under `internal/amp` and ACP projection beside
  its handler. Every child and pipe reader has a joined lifetime.
- Merge the inherited environment, agent overlay, and session overlay. Drop
  only `ACP_GO_AMP_INTERNAL_*` markers. Resolve the executable before applying
  session PATH directories. `WithHome` is unsupported.
- Wait for the native end receipt and quiet thread state before verifying the
  export. Cancellation calls the native thread API before stopping the observer.
  Reattach without input after disconnection; active remote work refuses another
  prompt. Repeated stale exports never prove completion.
- Mirror verified raw native thread exports atomically with accepted configuration.
  Remote state must agree at every shared message. Recover a confirmed missing
  thread through the native internal importer; verify its exported history before
  atomically publishing the new native binding under the stable ACP ID.
- Native state is never deleted by the adapter, except the private destination
  thread a failed recovery created and never bound. Delete tombstones the store
  before closing active work.
- Native tool permissions remain Amp's responsibility. There is no ACP
  permission, elicitation, model, or slash-command discovery surface.
- Unit tests use the test binary as a scripted Amp process.
- Comments describe current behavior or constraints, never history or plans.

## Verification

Run `make audit` after Go changes settle. Run native smoke after changes to
the native boundary, and the live continuation tests when authorized.

## Boundaries

- Never log credentials, prompts, tool bodies, or raw events by default.
- Native permissions remain native; never synthesize approval on the host's behalf.
