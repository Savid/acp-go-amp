# Minimal Client

Launches `acp-go-amp` as a subprocess, creates one session in the current
directory, sends one prompt, and prints the streamed answer.

```sh
go run ./examples/minimal-client "Reply with a short hello from ACP"
```

A local `amp` must be installed and authenticated; the session inherits your
environment and Amp's native files exactly as running `amp` in this directory would.
