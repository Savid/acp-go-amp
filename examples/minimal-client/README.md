# Minimal Client

This example is the smallest embedding of the `ampacp` agent: it calls
`ampacp.Serve` with stdin and stdout, so the process speaks ACP JSON-RPC on
stdio exactly like the `acp-go-amp` binary, with no extra configuration. Use
it as the starting point for wiring the agent into your own Go host.

```sh
go run ./examples/minimal-client
```

The program takes no flags or arguments. Connect an ACP client to its stdio
to initialize the connection, create a session, and prompt Amp; the process
exits when the client closes the connection. A local `amp` CLI must be
installed and authenticated.
