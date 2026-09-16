# acp-go-amp

`acp-go-amp` exposes the [Amp CLI](https://ampcode.com/docs/cli) through the
[Agent Client Protocol](https://agentclientprotocol.com). It creates a native
thread with `amp threads new`, then runs one `threads continue` process per
prompt using stream-json input and output.

Continue the conversation after closing the adapter:

```sh
amp threads continue NATIVE_SESSION_ID
```

New, load, and resume responses and session-list entries expose the current
native ID as `_meta.amp.nativeSessionId`. Use it for native CLI continuation.
ACP requests continue to use the stable ACP `sessionId`. The store's configuration
record saves both IDs with the matching native history.

## Run and embed

```sh
go install github.com/savid/acp-go-amp/cmd/acp-go-amp@latest
acp-go-amp [-path amp] [-seed-file rel=host]... [-debug]
```

Requires Amp 0.0.1789432613 or newer and native Amp authentication. Run
`amp login` separately. Native configuration and auth resolve from the inherited
environment. `-version` prints the adapter version; `OTEL_*` variables configure
telemetry exporters. `-home` and `-model` refuse nonempty values. `-scratch-dir` accepts a scratch parent. `-path` selects the executable, `-seed-file` seeds a relative native configuration file, and `-debug` enables stderr diagnostics.

```go
err := ampacp.Serve(ctx, os.Stdin, os.Stdout,
    ampacp.WithSessionStore(store),
    ampacp.WithTurnTimeout(5*time.Minute),
)
```

| Process option | Meaning |
|---|---|
| `WithExecutablePath` | Select the native executable. |
| `WithHome` | Refuse nonempty values; native home selection uses the inherited environment. |
| `WithEnv` | Overlay the inherited environment. |
| `WithScratchDir` | Parent for temporary lifecycle bridge files. |
| `WithSeedFiles` | Seed native configuration files without overwriting unmanaged files. |
| `WithDefaultModel`, `WithConfiguredModels` | Refuse nonempty values; native Amp selects models through modes. |
| `WithSessionStore`, `WithSessionStoreLoadTimeout` | Select the durability store and bound restore reads. |
| `WithTurnTimeout`, `WithConcurrencyLimits` | Bound prompt duration and configurable concurrency. |
| `WithImageLimits`, `WithInputHandoffRoot` | Set image byte limits and the root for image handoffs. |
| `WithLogger` | Supply the structured logger. |
| `WithTracerProvider`, `WithMeterProvider`, `WithTextMapPropagator` | Configure OpenTelemetry providers and context propagation. |
| `WithAgentName`, `WithAgentTitle`, `WithAgentVersion` | Set the identity advertised at initialize. |

## Sessions

Pass `_meta.amp.options` on new, load, or resume, or use `WithSessionAmpOptions`.

| Field | Meaning |
|---|---|
| `mode` | Native built-in or plugin mode, forwarded unchanged |
| `env` | Environment overlay for every command belonging to this session |
| `extraPathDirs` | Absolute directories prepended to the session PATH in order |

The only session config option is `mode`, a select over `low`, `medium`,
`high`, and `ultra` plus the accepted value when it is outside that menu. No
model selector is advertised, and `configId: "model"` is refused.

The environment merges the adapter process, `WithEnv`, then session `env`.
Only `ACP_GO_AMP_INTERNAL_*` markers are dropped. Executable resolution uses
the base PATH before applying the session paths. Seed files are relative to
the native settings directory and never overwrite an unmanaged file.

The adapter installs a unique temporary plugin in Amp's system plugin directory
under `XDG_CONFIG_HOME`, or `~/.config` when unset. It is inert in other launches
and removed after its process is reaped. Other plugins and the inherited native
configuration remain active. The plugin directory must be writable.

`model`, `outputSchema`, nonempty `mcpServers`, and unknown owned fields are
refused. Mode configuration applies to the next prompt; native `agent_mode`
updates the accepted value. Amp keeps its native tool permission behavior.
There is no ACP permission or elicitation bridge, model catalog, or slash
command catalog. Slash-prefixed text is ordinary prompt input.

Images use bounded inline base64 or validated file handoffs. Native inline
tool images are validated; remote image URLs become resource links without
fetching. The current native input ceiling is 5,138,022 bytes per image and
8,000 pixels per dimension. Native Amp enforces the dimension limit.

`_meta.amp.rawEvent.enabled` enables `_amp/rawEvent`. Image payloads are removed
from this diagnostic channel. Optional lifecycle negotiation opens a fresh
stream for each prompt process and reports acceptance and terminal state.

## Persistence

`session/new` runs `amp threads new`, creating an empty remote thread before
the first prompt. `--stream-json-input` delivers prompts to that thread.

`SessionStoreFormat` is `amp-thread-json-v1`. Each generation contains the raw
native thread export and a `config` sidecar with cwd, additional directories,
ACP and native session IDs, service origin, accepted mode, environment, ordered
paths, update time, and historic message usage. The process exits
and is reaped before export and atomic store publication. The mirror is durable
before terminal lifecycle state and the prompt response; a turn the store never
received ends its lifecycle incarnation without a terminal idle. A generation
captured by a failed commit is published by the next successful commit,
including the one at close. A failed close commit fails the close and still
releases the session.

A temporary native plugin observes `agent.end`, the native thread state, and
message IDs and contents. Each prompt first attaches to the existing thread
without input and refuses to submit while remote work is active. Cancellation
calls the native thread's cancel API and waits for acknowledgement and a settled
thread before stopping the local process. A disconnected process is reattached
without input to discover whether the remote work actually stopped.

Amp can finish a streamed turn before its export includes all completed messages.
The adapter compares the raw export with the observed quiet thread, retrying
with increasing delays under a 30-second deadline. Identical stale exports do
not count as completion. Terminal lifecycle state and the prompt response wait
for the verified export and atomic store commit. If reconciliation fails, the
previous mirror survives and no terminal idle is emitted.

Load attaches to the remote thread without submitting a prompt, verifies a
current export against its native state and every shared mirror message, adopts
newer messages, and replays history. Resume performs the same validation without
replay. A confirmed missing remote thread is recovered into a new private thread
through Amp's authenticated internal import API. Recovery attaches without input,
verifies exported message identities, order, roles, content, and completion state,
then commits the replacement native ID and history under the same ACP session ID.
Historic usage remains in the configuration record when native import omits it.

Shorter, conflicting, or inaccessible remote history fails restore. Authentication
and network failures never authorize replacement. Failed verification or store
publication preserves the previous committed generation and deletes the private
destination thread recovery created and never bound; a process killed mid-recovery
can still leave one behind. No other remote state is ever deleted.

The importer is an internal Amp API and may change independently of CLI commands.
Unsupported informational messages or a conversion that changes history fail
recovery. Native timestamps and some metadata may change. Cross-account recovery
and remote attachment access have not been verified.

Amp compacts a thread on its own when the context window fills, inserting an
informational summary message before the prompt that triggered it. A turn that
compacts settles normally: the mirror keeps the summary, verification compares
the conversation around it, and later prompts, loads, and resumes read past it.
A compacted thread cannot be recovered after native deletion: the importer
rejects summary messages, so restore fails and the backup stays intact.

Close joins the current prompt process. Delete tombstones the adapter store
first, then closes the session. Both leave the native remote thread intact.
The default store is in memory; supply a durable store for adapter restarts.

## Development

```sh
make test
make audit
make test-integration-smoke
ACP_GO_AMP_MODE=medium make test-integration-live
```

Unit tests use a native subprocess fixture and need no credentials. Smoke tests
exercise native creation/export/restore without model calls and skip when the
native binary is missing; `ACP_GO_AMP_HARNESS_PATH` overrides which binary they
resolve. Live tests spend
model tokens and test ACP → native CLI → ACP continuation, fresh local state,
PATH changes, cancellation, and deletion. Integration tests link the native
secret store into temporary XDG directories so credential refresh updates the
original store.
