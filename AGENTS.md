# acp-go-amp

## Purpose

This module exposes the Amp CLI as a Go ACP agent. It wraps a local `amp`
harness behind the Agent Client Protocol so hosts can drive Amp threads over
stdio or embed the agent directly in Go.

## Project Map

- Root package `ampacp`: ACP agent methods, request builders, metadata parsing,
  raw events, config options, and session-store API.
- `internal/amp`: Amp process boundary, stream-json parsing, environment
  construction, interrupt handling, and the contained-descendant inventory.
- `internal/lifecycle`: the self-contained `acp-go.dev/lifecycle` reducer,
  decoder, negotiation and correlation readers, and ordered emitter.
- `cmd/acp-go-amp`: stdio ACP command with `-path`, `-home`, `-model`,
  `-scratch-dir`, `-debug`, `-version`, and repeatable `-seed-file` flags, plus
  Darwin containment operations.
- `examples`: embeddable host examples that must stay covered by tests.
- `integration`: smoke and live tests for installed Amp binaries.
- `testdata/lifecycle`: the canonical family reducer battery, verbatim.
- `docs`: public documentation mirrored by `docs.json` navigation.

## Commands

- `make test`: unit tests.
- `make coverage-check`: 100.0% total statement coverage gate.
- `make lint`: pinned golangci-lint.
- `make docs-audit`: public-doc forbidden-term and Amp semantics audit.
- `make audit`: local release gate.
- `make test-integration-live`: live Amp prompt tests gated by
  `ACP_GO_AMP_RUN_INTEGRATION=1` and `ACP_GO_AMP_RUN_LIVE_TOKENS=1`.
- `make test-portable-runtime`: portable ordinary-lifecycle suite, runnable only
  on a `!unix` host that selects `internal/amp/process_unsupported.go`.

## Coding Rules

- Keep the public package surface identical to the adapter contract with Amp
  names substituted.
- Do not add permission bridging, command catalogs, model config options, or
  fork behavior without a director ruling.
- Keep process handling simple: one short-lived amp process per prompt — a
  thread-less `amp -x` execute on the first prompt (which creates the
  server-side thread), `amp threads continue` afterwards — with an isolated
  settings file and dedicated stdout/stderr pipes.
- Preserve ordinary Amp stream JSON bytes in the `transcript` store subpath.
  Image-bearing tool results use canonical artifact references so base64 and
  signed URLs do not leak into transcript or diagnostic surfaces.
- Do not persist auth, settings, API keys, or other secrets.
- Answer the lifecycle negotiation only with facts the active configuration
  proves, resolved from the same code path that enforces containment. A
  degenerate answer is correct; an unprovable one is not.
- Settle a prompt in one order: native terminal, containment and vacancy proof,
  durable commit, terminal idle, quiescence fact, response. One commit point
  covers every exit path. Settlement runs on a context detached from the
  request's, and the completion latch close and delete wait on is published only
  once the whole order has run. The latch carries the boundary's own failure
  only — an incomplete containment, a failed commit, or a failed terminal
  lifecycle or quiescence delivery — never the native turn's outcome: a natively
  failed prompt over a boundary that settled is a successful close and delete. A
  failed commit or an incomplete boundary fails the prompt and emits no terminal
  idle.
- `session/delete` fences the session's writes and waits out the commit in
  flight before its tombstone lands: a late `Replace` never clears a tombstone
  it did not create. A commit the fence stops retains its frames as
  mirror-unsynced: a delete whose tombstone never lands hands the host back a
  live session, and that session may not report itself clean over frames it was
  never allowed to write.
- Admitting a prompt and closing a session are one linearization. A prompt
  publishes itself as the session's active turn under the same lock a close or
  delete fences one with, and a session already closed or already fenced for
  delete admits none, so a teardown that observed an empty prompt slot is one no
  later prompt slips past.
- The outcome a settled turn recorded is the one its v1 response states: a
  cancel landing while a turn is already failing never rewrites that failure
  into a cancelled success.

## Testing Rules

- New code must keep `make coverage-check` at 100.0%.
- Fake Amp binaries and generated test files must live under `t.TempDir()` or
  another ignored path outside the repository tree.
- Conformance tests must pin strict `_meta.amp` handling, the mode-only config
  surface, no fork capability, no elicitation metadata, command silence, MCP
  accept/reject behavior, and backpressure errors.
- Run every vector in `testdata/lifecycle` with exact-equality projection
  matching, including each vector's `postRefusal` inputs. Those files are the
  contract: never edit, reorder, or delete one.
- Reduce this adapter's own emitted lifecycle stream through the same reducer
  the vectors drive.
- Live tests may spend tokens only when explicitly env-gated.

## Security And Boundaries

- `AMP_API_KEY` and `AMP_URL` are injected from live process environment or
  options; they are never written to `SessionStore`.
- `session/load` replays local transcript frames for display; it does not create
  a new server-side thread.
- Native continuation requires the original server-side Amp thread to still
  exist.
