# Resume From File

This example reads an Amp stream-JSON transcript from `session.jsonl` in this
directory into a `SessionStore`, loads the session through ACP so previous
interactions are replayed, then sends one no-tools smoke-test prompt in-process.
It denies tool permissions by default so a copied thread cannot silently run
commands while you are checking resume behavior.

Use it with a real Amp thread transcript:

```sh
cd examples/resume-from-file
go run . -session <thread-id> -cwd /absolute/path/to/project
```

If the JSONL frames include `session_id`, `-session` can be omitted and the id
is inferred; `-cwd` likewise defaults to the transcript cwd or the current
directory. Loading uses normal ACP `session/load`, and the prompt uses normal
ACP `session/prompt`.

Pass `-prompt "..."` to change the smoke-test turn, `-file` to point at a
different transcript, `-path` to point at a specific `amp` CLI, and `-home` to
set the parent root for isolated Amp session state.
