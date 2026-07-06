# Resume From File

This example reads an Amp stream-JSON transcript from `session.jsonl` in this
directory into a `SessionStore` (manifest on the main key, raw frames under
the `transcript` subpath), loads the session through ACP `session/load` so the
stored frames are replayed, sends one no-tools smoke-test prompt, prints the
stop reason, then closes the session.
It denies tool permissions by default so a copied thread cannot silently run
commands while you are checking resume behavior.

Use it with a transcript captured from a real Amp thread — for example the
frames a host persisted from this agent's `SessionStore` `transcript` subpath,
one stream-JSON frame per line:

```sh
cd examples/resume-from-file
go run . -session <thread-id> -cwd /absolute/path/to/project
```

If the JSONL frames include `session_id`, `-session` can be omitted; a system
`init` frame's `cwd` fills `-cwd`, falling back to the current directory.
Pass `-prompt "..."` to change the smoke-test turn, `-file` to point at a
different transcript, `-path` to select the `amp` CLI, and `-home` to set the
parent directory for the agent's isolated Amp home state.

Loading replays the local transcript for display only; the prompt continues
the original server-side Amp thread, so that thread must still exist and the
`amp` CLI must be installed and authenticated.
