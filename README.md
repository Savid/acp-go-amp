# acp-go-amp

`acp-go-amp` exposes the Amp CLI as an Agent Client Protocol agent for Go hosts.

The package uses Amp thread IDs as ACP session IDs. Each prompt starts one short-lived `amp threads continue <thread> --stream-json --stream-json-input -x` process with an isolated settings file.

```go
err := ampacp.Serve(ctx, input, output,
	ampacp.WithExecutablePath("amp"),
	ampacp.WithSessionStore(ampacp.NewInMemorySessionStore()),
)
```

Amp does not expose a documented import or fork CLI surface, so `_amp/session/fork` returns the package's unsupported error in this release.
