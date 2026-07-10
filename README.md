# acp-go-amp

Go ACP agent that exposes the Amp CLI as an [Agent Client Protocol](https://agentclientprotocol.com/) agent.

[![Go Reference](https://pkg.go.dev/badge/github.com/savid/acp-go-amp.svg)](https://pkg.go.dev/github.com/savid/acp-go-amp)
[![CI](https://github.com/savid/acp-go-amp/actions/workflows/go-test.yml/badge.svg)](https://github.com/savid/acp-go-amp/actions/workflows/go-test.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)

Use it as either:

- a standalone ACP subprocess: `acp-go-amp`
- an embedded Go adapter through `ampacp.Serve`

## Install

Library:

```sh
go get github.com/savid/acp-go-amp
```

CLI:

```sh
go install github.com/savid/acp-go-amp/cmd/acp-go-amp@latest
```

For local development, run the command straight from a checkout:

```sh
go run ./cmd/acp-go-amp
```

The process speaks ACP over stdin/stdout and reserves stdout for ACP JSON-RPC;
diagnostics go to stderr. In normal use an editor or ACP host launches it as a
subprocess rather than a human-facing chat UI.

## Quickstart

The example programs run from a checkout of this repo, so clone it first:

```sh
git clone https://github.com/savid/acp-go-amp && cd acp-go-amp
```

Run a tiny local client that launches the agent, sends one prompt, and prints
the reply (the prompt argument is optional):

```sh
go run ./examples/minimal-client "Reply with hello from ACP"
```

Or drive the agent from an interactive client session:

```sh
go run ./examples/interactive-chat
```

Load a stored Amp thread transcript and send one follow-up prompt against it:

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

- ACP session lifecycle (create, prompt, cancel, close, list, load, resume),
  using Amp thread IDs directly as ACP session IDs.
- One short-lived `amp threads continue` process per prompt, run with
  isolated native HOME/XDG state, an isolated settings file, and dedicated
  stdout/stderr pipes.
- Prompt streaming for assistant messages, tool calls, and thread results.
- MCP stdio and streamable HTTP configuration; other MCP transports are
  rejected because Amp exposes no supported path for them.
- No ACP slash-command advertisement; `/review`, `/plan`, and similar text is
  sent to Amp as ordinary prompt input.
- No permission or elicitation bridging; Amp never asks the client for
  permission, so the adapter never sends `session/request_permission`.
- No fork surface; `_amp/session/fork` is unsupported and `session/fork`
  returns method-not-found.
- Durable mirroring through a host-provided `SessionStore`; stored rows are raw
  Amp stream JSON frames kept under a `transcript` subpath.
- Native continuation requires the live server-side Amp thread and
  `AMP_API_KEY`; when it is gone, `session/load` still replays local display
  history and a later prompt returns the `native_state_missing` terminal error.
- Optional raw Amp stream notifications through `_amp/rawEvent`, plus
  OpenTelemetry telemetry that records no prompt or tool secrets by default.

## Docs

- [Overview](docs/overview.mdx)
- [Run modes](docs/get-started/run-modes.mdx)
- [Go API](docs/reference/go-api.mdx)
- [ACP methods](docs/reference/acp-methods.mdx)
- [Observability](docs/operations/observability.mdx)
- [Go package reference](https://pkg.go.dev/github.com/savid/acp-go-amp)

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
options and are never written to the session store. If the required credentials
are absent, live tests fail instead of launching against the developer's real
Amp home.

## License

[GNU General Public License v3.0](LICENSE).
