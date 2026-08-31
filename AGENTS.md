# acp-go-amp

## Purpose

This module exposes the Amp CLI as a Go ACP agent. It wraps a local `amp`
harness behind the Agent Client Protocol so hosts can drive Amp threads over
stdio or embed the agent directly in Go.

## Project Map

- Root package `ampacp`: ACP agent methods, request builders, metadata parsing,
  raw events, config options, and session-store API.
- `internal/amp`: Amp process boundary, stream-json parsing, environment
  construction, interrupt handling, and browser mediation.
- `internal/lifecycle`: the self-contained `acp-go.dev/lifecycle` reducer,
  decoder, negotiation and correlation readers, and ordered emitter.
- `cmd/acp-go-amp`: stdio ACP command with `-path`, `-home`, `-model`,
  `-scratch-dir`, `-debug`, `-version`, and repeatable `-seed-file` flags.
- `examples`: embeddable host examples that must stay covered by tests.
- `integration`: smoke and live tests for installed Amp binaries.
- `testdata/lifecycle`: the canonical family reducer battery, verbatim.
- `docs`: public documentation mirrored by `docs.json` navigation.

## Commands

- `make test`: unit tests.
- `make coverage-check`: 100.0% total statement coverage gate.
- `make lint`: pinned golangci-lint.
- `make docs-audit`: required public docs, examples, flags, and Amp semantics audit.
- `make audit`: local release gate.
- `make test-integration-live`: live Amp prompt tests gated by
  `ACP_GO_AMP_RUN_INTEGRATION=1` and `ACP_GO_AMP_RUN_LIVE_TOKENS=1`.
- `make test-portable-runtime`: portable ordinary environment and executable
  behavior suite, exercised by the strict Windows CI proof.

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
- Every request builder that accepts a caller `_meta` map merges rather than
  assigns, so option order carries no meaning, and refuses any `acp-go.dev/*`
  family literal in that map rather than merging, overwriting, or dropping it.
- The reserved lifecycle key is read before an extension method name is
  resolved, so every dispatched extension method refuses it by name — a name
  this adapter does not serve answers the key, not method-not-found.
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
- `session/close` carries the same durable commit as its own rung. A settlement
  whose commit failed retains its frames, and the close retries them after the
  completion wait, on a detached context, while the session is still
  addressable: a commit the close cannot land fails the close and reclaims
  nothing, so only a settled close evicts the session. A session already fenced
  for delete commits nothing there.
- `Agent.Close` owes the same rung: the shutdown ladder applies identically to
  an embedded close, so a commit a wire close would have made is made there or
  reported as the store's refusal, never dropped with the wrapper. Shutdown is
  the last word on its sessions, so the failure is fail-closed on the commit
  alone — every session still releases its settings and scratch state, because
  no later close exists to release it.
- One `Replace` states each key exactly once. A key stated twice is refused by
  that key's own name, because slice order is not a caller's decision about
  which state the row holds.
- The session store enforces tombstone finality itself. Over a key `Delete`
  tombstoned, `Append` and `Replace` both write nothing, clear nothing, and
  return success; an adapter-level deletion marker is not a substitute for a
  write already in flight.
- `session/load` and `session/resume` re-check the deletion marker under the
  same lock that installs the session, tear down a fully prepared replacement
  that lost the race, and never clear the marker as a side effect of
  installing: a delete that completes while a load is preparing wins, however
  far the preparation got.
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
  the vectors drive. The emitter validates the rendered notification, not the
  struct behind it: render, marshal, decode, then reduce.
- Live tests may spend tokens only when explicitly env-gated.

## Security And Boundaries

- `AMP_API_KEY` and `AMP_URL` are injected from live process environment or
  options; they are never written to `SessionStore`.
- `session/load` replays local transcript frames for display; it does not create
  a new server-side thread.
- Native continuation requires the original server-side Amp thread to still
  exist.
