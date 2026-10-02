# AGENTS.md

## Purpose

This Go module exposes the local OpenCode v2 `opencode` CLI as an Agent Client Protocol agent.
One shared `opencode serve` process handles the Agent's sessions through native
HTTP and SSE. Native storage stays in its XDG home. After closing the adapter,
continue with `opencode run --session NATIVE_SESSION_ID` in the same directory and home.

## Project Map

- `cmd/acp-go-opencodev2`: stdio entrypoint, OpenTelemetry setup, signals, flags.
- Root `agent*.go`, `options.go`, `request_builders.go`: the public ACP
  surface and option validation; the shared ACP transport orders publication.
- Root `session*.go`: one session's binding and event pump,
  prompt turns, permissions and elicitation, lifecycle stream, store mirror,
  replay, config options, and image input/output.
- `internal/opencode`: native HTTP/SSE client, session import/export,
  and launch arguments.
- `integration`: gated tests against the installed opencode.

## Commands

```sh
make build
make test
make lint
make audit
make test-integration-smoke
make test-integration-live
```

`make test` runs with race detection and shuffled order. `make audit` is the
full local gate. Integration targets need an installed `opencode`; the live target
spends model tokens and requires explicit operator intent.

## Coding Rules

- Follow Go idioms: `ctx` first, `%w` for wrapped errors, small interfaces at
  the consumer. Keep native protocol details in `internal/opencode` and ACP glue
  beside its handler.
- Shared family behavior comes from `github.com/savid/acp-go-core`; never copy
  it here.
- The adapter does no isolation: opencode inherits the process environment, the
  agent overlay, then adapter-owned XDG roots and loopback credentials. A native
  plugin applies the addressed session environment and ordered PATH directories
  to its tools. Only `ACP_GO_OPENCODEV2_INTERNAL_*` markers are dropped.
- Native state is never deleted. The session store is the durability
  boundary; OpenCode's own database is the native copy.
- Unit tests never require an installed opencode: the test binary doubles as a
  scripted fake opencode. Keep the fake's protocol in step with `internal/opencode`.
- A comment states what the code does or why a constraint exists.

## Verification

Run `go test ./...` for ordinary changes and `make lint` for Go edits. Run
`make audit` once changes settle. Run the integration smoke target after
changing anything opencode-facing.

## Boundaries

- The permission bridge is the session permission system. Never bypass its
  dialog or fail open on a denied or cancelled answer.
- Do not log prompts, tool input or output, or raw native event bodies by
  default.
- Account usage resolves credentials and routes through the addressed native
  runtime, then delegates provider HTTP reads to `acp-go-core/usage`. Reject
  extension methods other than the advertised account-usage read.
