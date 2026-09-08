# AGENTS.md

## Purpose

Expose the local Amp CLI as a Go ACP agent over stdio or embedded Go. Each
prompt owns one short-lived process: thread-less `amp -x` creates the remote
thread; later prompts use `amp threads continue` for that same thread.

## Project Map

- Root `ampacp`: agent/session ownership, validation, auth, media, configuration,
  and session-store API.
- `internal/amp`: processes, stream-JSON, environment, interruption, browser safety.
- `internal/lifecycle`: negotiation, decoding, reducer, and ordered emission.
- `cmd/acp-go-amp`: stdio CLI; flags belong in `docs/reference/cli.mdx`.
- `examples`: embedded hosts with ordinary unit coverage.
- `integration`: explicitly gated installed-Amp tests.
- `testdata/lifecycle`: canonical reducer vectors, preserved verbatim.
- `docs`, `docs.json`: self-contained public documentation and navigation.

## Commands

- `make test`: race-enabled, shuffled ordinary unit tests.
- `make coverage-check`: race-enabled, shuffled suite and coverage report.
- `make lint`: pinned golangci-lint; do not substitute a global binary.
- `make modernize-check`: inspect actual `go fix -diff` proposals.
- `make docs-audit`: public docs, examples, flags, and documented Amp behavior.
- `make audit`: combined release gate; inspect its targets before running.
- `make test-portable-runtime`: execute portable ordinary behavior on a supported
  non-Unix host. Cross-compilation alone is not runtime evidence.
- Native targets: `make test-integration-smoke`, `test-integration-live`,
  `test-integration-attended`, `test-integration-keystore`, and
  `test-integration-native-browser`. Run only when already authorized by the
  task; environment gates and installed credentials do not grant authorization.

## Coding Rules

- Preserve the supported public API and existing error identities. Keep Amp
  mode-only, with no permission bridge, command catalog, model option, fork, or
  elicitation unless the task authorizes a public-surface change.
- Keep stdout for ACP; preserve Go context and wrapped-error conventions.
- Metadata builders merge caller maps and refuse every reserved `acp-go.dev/*`
  literal. Preserve owned raw lifecycle metadata through typed wire decoding;
  validate it at the existing semantic stage, including unknown extensions.
- Prompt admission and close/delete fencing share one lock. One settlement owner
  retains native cleanup, owed commit, terminal delivery, and exact retries.
  Commit plus idle releases the foreground result; later quiescence belongs to
  full completion, which close/delete join. Native failure is not boundary failure.
- Delete fences writes before tombstoning; a main-key store delete is final even
  against writes in flight. Load/resume never clear deletion markers and tear
  down a prepared replacement that loses installation. Preserve exact commit
  generations and retained unsynced frames. See `docs/features/session-store.mdx`.

## Testing Rules

- Add focused regressions at observable boundaries; synchronize concurrent tests
  with explicit barriers. Run relevant failure/race cases during edits and the
  combined gate once changes settle. Review coverage without padding tests or
  production seams for a percentage.
- Keep strict `_meta.amp`, mode-only configuration, absent fork/elicitation,
  command silence, MCP acceptance/refusal, and backpressure regressions.
- Run every lifecycle vector, including `postRefusal`, with exact-equality
  projections. Never edit, reorder, or delete canonical fixture bytes. Render,
  marshal, decode, then reduce emitted notifications through the same reducer.
- Fake binaries and generated files belong in `t.TempDir()` or ignored scratch.
  Deterministic fakes prove wrapper behavior; native claims need real-native
  evidence. Live prompts require explicit authorization and both integration
  and live-token environment gates.

## Security And Boundaries

- Managed launches use host authority exclusively, dedicated stdout/stderr pipes,
  and isolated settings. Materialize settings, MCP, seeds, and launchers before
  preparation. Every failed prepare remains opaque and fences admission; only
  successful reclaim restores adapter access. Never fall back to direct execution.
- Advertise vacancy/quiescence only when the configured authority proves them.
  Keep a managed handoff descriptor disjoint from the complete scratch domain;
  the host preserves directory identities and disjointness across its lifetime.
- Hosted login is ordinary-only: managed selectors cannot be locally audited.
  Managed provider auth retains the manual API-key flow. Preserve the ordinary
  browser audit and explicit provider-auth boundaries.
- Never persist native settings or provider-auth ledger/residence contents.
  Persist the complete session environment including raw `PATH`; credentials
  such as `AMP_API_KEY` make the store secret-bearing. Credential harvest must
  settle before reading and recheck flow/session ownership before one-shot delivery.
- Preserve ordinary transcript bytes. Image tool results use canonical artifact
  references; omit base64/signed URLs from transcripts and diagnostics. Artifact
  storage failure is fatal; replay failure uses `amp_restore_failed` and keeps
  the session. Load replays local display history; resume omits replay. Neither
  creates a remote thread, and remote-thread loss never deletes the local mirror.
