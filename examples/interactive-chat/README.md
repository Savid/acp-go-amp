# Interactive Chat

This example serves the `ampacp` agent over stdin and stdout so an
interactive ACP client — an editor or any host that speaks the Agent Client
Protocol — can hold a chat with Amp through it. It is the same stdio surface
the `acp-go-amp` binary exposes, embedded directly in Go.

```sh
go run ./examples/interactive-chat
```

The program takes no flags or arguments. Point your ACP client at the
process's stdio, create a session, and each prompt you send streams Amp's
assistant messages, tool calls, and usage back as ACP session updates. The
process exits when the client closes the connection. A local `amp` CLI must
be installed and authenticated.
