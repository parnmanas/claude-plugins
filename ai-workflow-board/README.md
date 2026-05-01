# AI Workflow Board (Claude plugin)

Stdio MCP bridge that lets Claude CLI talk to an AWB server. Install via the
`parnmanas` marketplace, then run `/awb:setup <server-url> <api-key>`.

```
Claude CLI <--stdio--> proxy.mjs <--HTTP--> AWB Server (/mcp)
```

The plugin is intentionally minimal — it only forwards MCP JSON-RPC traffic.
Background work that used to run inside the proxy (SSE channel events,
subagent delegation, persistent ticket/chat sessions, multi-CLI adapters)
moved to a separate package, **`@awb/agent-manager`**, in the
`ai-workflow-board` repo under `apps/agent-manager/`. Install and pair that
process to get autonomous trigger handling.

## Files

| Path | Purpose |
|------|---------|
| `proxy.mjs` | stdio entrypoint launched by Claude CLI via `.mcp.json`. |
| `lib/mcp-forward-session.mjs` | Owns the AWB MCP session — stale-session recovery, network retries. |
| `lib/config.mjs` | Loads `~/.claude/channels/awb/config.json`. |
| `lib/logging.mjs` | Stderr + `~/.claude/channels/awb/proxy.log` log writer. |
| `lib/constants.mjs` | Config path + request timeout. |
| `skills/setup/SKILL.md` | `/awb:setup` slash-command guide. |
| `test/mcp-forward-session.test.mjs` | Forward-session lifecycle tests (`node --test`). |

## Migration from v0.39.x

`v0.40.0` removed the daemon and proxy-side delegation pipeline. To restore
SSE-driven trigger handling on a host that previously ran the proxy daemon:

1. `npm i -g @awb/agent-manager` (or run from a checkout of
   `submodules/ai-workflow-board/apps/agent-manager`).
2. In the AWB admin UI, open **Agent Manager → Pair manager…**, mint a
   token, and redeem it from the agent-manager process on first start.
3. Create managed agents from the AWB UI; the manager spawns the configured
   CLI per agent.

`~/.claude/channels/awb/config.json` and `agent.json` are preserved and
auto-imported by agent-manager on first run.
