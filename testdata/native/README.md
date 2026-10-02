# Native evidence

`agent-origin.json` was captured from stock OpenCode 2.0.21 on 2026-10-02,
release commit `8a8bd622a3d7dc29ccf30ec17f84e363ed95ed72`.

A direct native HTTP client created a session, subscribed to `/api/event`,
and submitted a prompt through `/api/session/{sessionID}/prompt`. No ACP
prompt was active. The model was `opencode-go/qwen3.8-flash`, variant `low`.
The capture retains the native execution, streaming, and usage events through
`session.execution.succeeded`. The session ID and workspace path are replaced
with fixture values; event ordering and payloads are otherwise unchanged.

The fixture proves that the permanent channel reports work started outside
ACP and supplies the boundaries used for an agent-origin lifecycle cycle.
