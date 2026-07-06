# acp-go-amp

Go ACP agent for the local Amp CLI. It wraps `amp threads continue`, speaks
[Agent Client Protocol](https://agentclientprotocol.com/) over JSON-RPC
streams, and is built on
[`github.com/coder/acp-go-sdk`](https://github.com/coder/acp-go-sdk).

Use it as either:

- a standalone ACP subprocess: `acp-go-amp`
- an embedded Go adapter through `ampacp.Serve`

## Install

```sh
go install github.com/savid/acp-go-amp/cmd/acp-go-amp@latest
```

For local development:

```sh
go run ./cmd/acp-go-amp
```

The process speaks ACP over stdin/stdout. In normal use, an editor or ACP host
launches it as a subprocess rather than a human-facing chat UI.

## Quickstart

Serve the smallest embedded agent over stdio and connect any ACP client to it:

```sh
go run ./examples/minimal-client
```

Or serve the same stdio surface for an interactive ACP host:

```sh
go run ./examples/interactive-chat
```

Load and resume a stored Amp thread transcript:

```sh
go run ./examples/resume-from-file -file ./examples/resume-from-file/session.jsonl
```

## Embedded Go

```go
package main

import (
	"context"
	"log"
	"os"

	ampacp "github.com/savid/acp-go-amp"
)

func main() {
	err := ampacp.Serve(context.Background(), os.Stdin, os.Stdout,
		ampacp.WithExecutablePath("amp"),
		ampacp.WithSessionStore(ampacp.NewInMemorySessionStore()),
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

See [Go API docs](docs/reference/go-api.mdx) for options such as the Amp
executable path, isolated home directory, session storage, and OpenTelemetry
providers.

## What It Provides

- ACP session lifecycle: create, prompt, cancel, close, list, load, and resume.
  Amp thread IDs are used directly as ACP session IDs.
- Amp process management: `session/new` creates an empty server-side thread with
  `amp threads new`, and each prompt starts one short-lived
  `amp threads continue <thread> --stream-json --stream-json-input -x` process
  with isolated native HOME/XDG state, an isolated settings file, and dedicated
  stdout/stderr pipes.
- Prompt streaming for assistant messages, tool calls, and thread results.
- No ACP slash-command advertisement. `/review`, `/plan`, and other
  slash-prefixed text is sent to Amp as ordinary prompt input.
- Amp never asks the client for permission and does not advertise elicitation
  metadata; the adapter never sends `session/request_permission`.
- MCP stdio and streamable HTTP configuration. Other MCP transports are rejected
  because Amp does not expose supported paths for them.
- Durable mirroring through a host-provided `SessionStore`; stored rows are raw
  Amp stream JSON frames kept under a `transcript` subpath. local transcript
  restore is not native thread resurrection: continuation requires the live
  server-side Amp thread and AMP_API_KEY. When that server-side thread is gone,
  `session/load` still replays local display history and a later prompt returns
  the `native_state_missing` terminal error.
- No fork surface: Amp exposes no documented fork or thread-seeding CLI, so
  `_amp/session/fork` is unsupported and stable `session/fork` returns
  method-not-found.
- Optional raw Amp stream extension notifications through `_amp/rawEvent`.
- OpenTelemetry adapter telemetry without recording prompt or tool secrets by
  default.

## Docs

- [Overview](docs/overview.mdx)
- [Run modes](docs/get-started/run-modes.mdx)
- [Go API](docs/reference/go-api.mdx)
- [ACP methods](docs/reference/acp-methods.mdx)
- [Observability](docs/operations/observability.mdx)

## Development

```sh
make audit
make test-integration-smoke
make test-integration-live
make test-integration-cover
```

Live integration tests require a local authenticated `amp` CLI. The live target
sets `ACP_GO_AMP_RUN_INTEGRATION=1` and `ACP_GO_AMP_RUN_LIVE_TOKENS=1` and may
spend model tokens. Live tests always launch Amp with isolated native HOME/XDG
state; `AMP_API_KEY` and `AMP_URL` are injected from the process environment or
options and are never written to the session store. If the required
credentials are absent, live tests fail instead of launching against the
developer's real Amp home.
