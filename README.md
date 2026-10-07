# acp-go-opencodev2

`acp-go-opencodev2` exposes [OpenCode v2](https://opencode.ai/v2/) as an
[Agent Client Protocol](https://agentclientprotocol.com) agent. One
`opencode serve` process handles the Agent's sessions through authenticated
loopback HTTP and a shared event stream.

Sessions retain OpenCode's native storage. After closing the adapter,
continue a session in the same directory and native home:

```sh
opencode run --session NATIVE_SESSION_ID "Continue the task"
```

New, load, and resume responses and session-list entries expose the current
native ID as `_meta.opencode.nativeSessionId`. Use it for native CLI continuation.
ACP requests continue to use the stable ACP `sessionId`. The store's configuration
record saves both IDs with the matching native history.

## Install and run

```sh
go install github.com/savid/acp-go-opencodev2/cmd/acp-go-opencodev2@latest
acp-go-opencodev2 [-path opencode] [-home DIR] [-scratch-dir DIR] [-model provider/id] [-seed-file rel=host]... [-debug]
```

The default executable is `opencode`; select an alternate local installation
with `-path opencode2`. A bare `-path` is resolved on the inherited PATH.
`-home` maps `DIR/data`, `DIR/config`, `DIR/cache`, and `DIR/state` to the
four XDG home variables; omit it to use native home resolution. Native CLI
continuation uses those same variables when a home was supplied.
`-seed-file` writes a relative file inside OpenCode's configuration directory.
`-scratch-dir` holds the temporary environment plugin. `-version` prints the
adapter version. Standard `OTEL_*` variables configure telemetry exporters.

## Embed

```go
err := opencodeacp.Serve(ctx, os.Stdin, os.Stdout,
    opencodeacp.WithHome("/srv/opencode"),
    opencodeacp.WithSessionStore(store),
)
```

Options: `WithExecutablePath`, `WithHome`, `WithScratchDir`,
`WithInputHandoffRoot`, `WithDefaultModel`, `WithConfiguredModels`, `WithEnv`,
`WithSeedFiles`, `WithSessionStore`, `WithConcurrencyLimits`, `WithImageLimits`,
`WithLogger`, `WithTracerProvider`, `WithMeterProvider`, `WithTextMapPropagator`,
`WithAgentName`, `WithAgentTitle`, `WithAgentVersion`.

### Session options

Pass `_meta.opencode.options` on new, load, or resume, or use
`WithSessionOpenCodeOptions` from Go.

| Field | Meaning |
|---|---|
| `model` | Provider-qualified model ID |
| `mode` | Native agent ID, such as `build` or `plan` |
| `effort` | Native model variant, forwarded unchanged |
| `permission` | Native tool policy: `ask`, `allow`, or `deny`; default `ask` |
| `env` | Environment overlay for tools in this session |
| `extraPathDirs` | Absolute directories prepended to the session's PATH in order |

The environment plugin reads the addressed session's metadata through OpenCode's
API. Child sessions inherit that carrier through their native parent. OpenCode
keeps its native authentication and configuration. Nonempty `mcpServers` and
unknown owned options are invalid parameters.

`session/set_config_option` accepts nonempty `model`, `mode`, and `effort`
values. The native provider catalog supplies model names, image capabilities,
context windows, and available variants. Configured and selected models are
included even when absent from the catalog. OpenCode v2 has no schema-enforced
prompt API: `outputSchema` is rejected and no structured-output helper or
capability is exposed.

Native permission requests use ACP permissions; native questions use ACP form
elicitation. Missing or cancelled answers reject the native request. Forms requiring hidden,
conditional, or external fields are cancelled. Unbound child sessions are
mirrored, but their permission and form requests are refused. Commands
come from OpenCode's command catalog. An exact `/name` match against an
advertised command uses the native command endpoint; other text uses the
prompt endpoint.

Images enter as inline base64 or validated file handoffs. Text must precede
images because the native API separates the text from its attachments; other
orders are rejected before dispatch. Output supports native file content and
tool attachments, with bounded local reads and image limits.
Remote URLs become resource links. `_meta.opencode.rawEvent.enabled` enables
`_opencode/rawEvent`; image bytes are omitted from that diagnostic channel.
Optional lifecycle negotiation supplies ordered session and turn updates.

Each model call reports a `usage_update` with its token breakdown when its
`session.step.ended` event arrives, including the native session’s cumulative
cost in USD. Agent message and thought chunks carry no
`messageId`, and the breakdown carries no `responseId`: OpenCode keeps none of
the gateway's response ids, and its own message and part ids are not response ids.

### Persistence and runtime

`SessionStoreFormat` is `opencode-session-export-v1`. The main subpath holds native
session exports for one conversation and its descendants. The `config` sidecar
holds accepted options, captured local image bytes, and deferred synthetic inbox
entries. A verified native snapshot commits atomically before terminal idle and
the prompt response, including on cancellation. The execution’s terminal event
identifies its durable idle marker in the native export. The root snapshot ends
at that marker; each descendant ends at its latest idle marker, or has empty
history if it has not completed an execution. A second graph read verifies the
retained messages, configuration, and deferred inbox while later execution
continues. Native cumulative usage and session timestamps retain their values
at capture time and may include later execution. Commands without execution,
configuration changes, restore, and close require matching idle graph snapshots.
Synthetic reminders are restored with their native IDs without starting
execution. A directory change returns backpressure while native inbox entries
remain pending.

Load imports missing sessions through the native import API and replays ACP
history. Resume imports without replay. Existing native messages must preserve
every saved message’s identity, content, and order. Newly completed assistant,
shell, and compaction records omitted by earlier native exports may appear
between saved messages; later turns are adopted. Conflicting or shorter native
histories are refused because the import API cannot replace a session. Local
image replay remains available after its original file is removed. The default
store is in memory; supply a durable store to restore across adapter restarts.

Close releases one session while peers retain the shared server. A server crash
fails affected work, and the next operation starts a replacement and rebinds the
addressed session. Delete tombstones the store entry. Native state remains
available to OpenCode's CLI. A native-home file lock prevents two adapter servers
from owning the same home concurrently.

## Development

```sh
make test
make lint
make audit
make test-integration-smoke
ACP_GO_OPENCODEV2_MODEL=provider/model make test-integration-live
```

Unit tests use a scripted native HTTP server inside the test binary and require
no installed OpenCode or credentials. Smoke tests use the installed CLI and a
local stub provider without spending model tokens. Live tests use temporary
homes and credentials supplied through the native environment (for example
`OPENCODE_API_KEY`); they spend tokens.
Set `ACP_GO_OPENCODEV2_HARNESS_PATH=opencode2` to test an alternate installation.

## Account usage

`AccountUsageMethod` (`_opencode/accountUsage`) accepts `sessionId` and
`providerId` (`opencode-go`, `openrouter`, `anthropic`, or `openai-codex`).
Initialization advertises the method, session scope, and supported providers.
A provider routed through an explicitly configured native endpoint that
publishes a gateway usage report is read with its effective catalog credential. Reads hold the session's
foreground gate and spend no model tokens.

The adapter resolves the directory's effective API key and route through the
native server. Only official endpoints and verified API-key routes are read;
OAuth connections and unverified authentication overrides yield `not_reported`.
OpenCode Console organization-scoped inference routes currently return
`not_reported`; their account scope does not match the OpenCode Go key reader.
Credentials stay local. Provider HTTP reads come from `github.com/savid/acp-go-core/usage`.

OpenCode Go reports rolling, weekly, and monthly percentage windows. OpenRouter
reports key spending caps, lifetime spend, free-model request counts, and any
account credit balance accessible with the same key. Dollar amounts are USD;
a missing cap is explicitly uncapped, zero is a real value, and a negative
remaining balance is preserved. Account credits and key caps remain separate.
Each measurement retains its own observation and expiry times. Unavailable
optional account credits do not discard key data.

Claude and ChatGPT subscription usage require effective OAuth credentials from
the native runtime. Direct OAuth usage reads are not implemented; subscription measurements are
available only through a verified configured gateway’s usage report.

## Context compaction

Reports native compaction starts, completions, failures, and cancellations,
with the native trigger when present. Automatic and manual attempts retain one
ID through their terminal outcome. Context counts are unavailable.

Notifications carry `acp-go.dev/compaction` on the notification’s `_meta`,
with an otherwise empty `session_info_update`. The value is `acp-go-core`
`wire.Compaction`: a required `compactionId` and `status`, and optional
`trigger`, `contextBefore`, and `contextAfter`. A start and its outcome share
an ID. Unknown facts are omitted. These are live notifications; historical
replay emits none. Usage accounting is independent.
