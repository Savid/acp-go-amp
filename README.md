# acp-go-amp

`acp-go-amp` exposes the Amp CLI as an Agent Client Protocol agent for Go hosts.

The package uses Amp thread IDs as ACP session IDs. `session/new` creates an
empty server-side Amp thread with `amp threads new`; each prompt starts one
short-lived `amp threads continue <thread> --stream-json --stream-json-input -x`
process with an isolated settings file and dedicated stdout/stderr pipes.

```go
err := ampacp.Serve(ctx, input, output,
	ampacp.WithExecutablePath("amp"),
	ampacp.WithSessionStore(ampacp.NewInMemorySessionStore()),
)
```

The session store mirrors Amp stream JSON frames for load replay. local transcript restore is not native thread resurrection: continuation requires the live server-side Amp thread and AMP_API_KEY. If that server-side thread is gone,
`session/load` can still replay local display history and a later prompt returns
the `native_state_missing` terminal error.

Amp does not expose a documented fork or thread-seeding CLI surface, so
`_amp/session/fork` returns the package's unsupported error. Stable
`session/fork` returns method-not-found.
