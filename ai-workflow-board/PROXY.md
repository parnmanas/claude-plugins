# AWB MCP Proxy — Delegation Guide

The AWB plugin proxy (`proxy.mjs`) bridges Claude CLI to an AWB server via MCP over HTTP, and also receives SSE events from the server to deliver into Claude's channel notification stream.

As of plugin version 0.5.0 (Phase 4), the proxy can optionally **delegate** trigger processing to background Claude CLI subagents instead of forwarding every trigger to the main session. This lets the main session stay unblocked while subagents work on tickets in parallel.

> **Daemon mode (v0.36.0+):** the same SSE + subagent pipeline can run as a standalone process via `daemon.mjs` — no Claude CLI parent required. See "Daemon Mode" near the bottom of this doc. The proxy stays the right choice for users running an interactive Claude session who want delegation triggered by their own MCP traffic; the daemon is the right choice for headless agent boxes or for users who want subagent processing to keep working when no interactive Claude session is open.

## Architecture

```
Claude CLI (main session)
  │  stdio MCP
  ▼
proxy.mjs ─── HTTP MCP ───▶ AWB Server
  │                            │
  │   SSE events                │
  ◀────────────────────────────┘
  │
  ├─ EventStream.#handleTrigger(event)
  │    ├─ delegation.enabled? → SubagentManager.spawn() ──▶ claude --print child
  │    │                                                    (MCP tools back to AWB)
  │    └─ else → sendChannelEvent(main session)
  │
  └─ EventStream.#handleChatRequest(event)
       └─ SubagentManager.spawn() ──▶ claude --print child
                                       (send_chat_message MCP tool back to AWB)
```

The main session only sees lightweight "dispatched" and "completed" notifications. All real work (ticket claims, comments, status changes, chat replies) happens via the subagent's own MCP tool calls against the AWB server — those changes propagate back to the main session's web UI via the existing SSE stream, same as any other agent activity.

## Configuration

The proxy reads `~/.claude/channels/awb/config.json`:

```json
{
  "url": "https://awb.example.com:7700",
  "apiKey": "awb_...",
  "delegation": {
    "enabled": true,
    "maxConcurrent": 5,
    "ttlMinutes": 15,
    "claudeBin": "claude",
    "appendSystemPromptMode": "role_only"
  }
}
```

| Field | Default | Purpose |
|-------|---------|---------|
| `delegation.enabled` | `true` | Set to `false` to restore legacy Phase 1 behavior (forward all triggers to main session). Instant rollback hatch — no plugin reinstall needed. |
| `delegation.maxConcurrent` | `5` | Max subagents running at once. When reached, new triggers fall back to main-session forwarding with a warning log until slots free up. |
| `delegation.ttlMinutes` | `15` | Max subagent runtime. Exceeding triggers `SIGTERM` then `SIGKILL` after a 5s grace period. |
| `delegation.claudeBin` | `"claude"` | Path to Claude CLI binary. Override for test stubs (e.g. `./test/fake-claude.sh`). |
| `delegation.appendSystemPromptMode` | `"role_only"` | Reserved for future prompt composition strategies — currently only `role_only` is implemented. |

The `delegation` section is normalized via `loadConfig()` — missing fields are merged with defaults. Users upgrading from plugin 0.3.x who do not touch `delegation` will get the Phase 4 defaults (delegation on, cap 5, TTL 15min).

## Subagent Lifecycle

When a trigger arrives and delegation is on:

1. Proxy fetches fresh ticket context via `GET /api/tickets/:id?include=comments` (best-effort — a transient REST outage does not block dispatch; the subagent just gets less context).
2. Proxy composes a task prompt: ticket title + description + recent 5 comments + ticket.prompt_text + instructions to use AWB MCP tools to claim the ticket, leave a comment, and move the column.
3. Proxy writes a per-subagent MCP config file at `~/.claude/channels/awb/subagents/<pid>/mcp-config.json` (mode `0600`, dir mode `0700`) containing:
   ```json
   {
     "mcpServers": {
       "awb": {
         "type": "http",
         "url": "...",
         "headers": { "Authorization": "Bearer ..." }
       }
     }
   }
   ```
   The `{"mcpServers": {...}}` wrapper shape is MANDATORY — the Claude CLI silently rejects a bare `{name: {...}}` file.
4. Proxy spawns: `claude --print --output-format json --mcp-config <file> --strict-mcp-config --allowedTools "mcp__awb__*" --append-system-prompt "<role_prompt>" --dangerously-skip-permissions "<task>"`. The `AWB_API_KEY` env var is passed via `env:` on the spawn (never on argv — would leak in `ps`).
5. Proxy sends a lightweight `[AWB Subagent] Dispatched ticket=... pid=...` notification to the main session via `sendChannelEvent` with meta `type: "subagent_dispatched"`.
6. Child runs autonomously, calling `mcp__awb__*` tools to claim the ticket, read it, work on it, comment, and move it.
7. On child exit, proxy sends a completion notification via `sendChannelEvent` with meta `type: "subagent_complete"` in one of three variants:
   - `[AWB Subagent] ticket=... completed (duration=Ns)` — exit code 0
   - `[AWB Subagent] ticket=... FAILED (exit=N, duration=Ns, see proxy logs)` — non-zero exit
   - `[AWB Subagent] ticket=... TIMED OUT after Ns` — killed by SIGTERM/SIGKILL
8. Per-subagent config file + directory are deleted.

Parallel subagents are tracked in memory (`Map<pid, record>`) and persisted to `~/.claude/channels/awb/subagents.json` so proxy restarts can reconcile in-flight subagents.

The chat subagent path (`EventStream.#handleChatRequest`) uses the same spawn pipeline with `kind: 'chat'` — the task text is composed from the chat conversation history + new message and the subagent is instructed to reply via the `mcp__awb__send_chat_message` tool (not stdout, which would only reach the proxy's log and never the user's web UI).

## TTL Sweep & Orphan Cleanup

Every 60 seconds, the proxy sweeps its subagent map:
- **Existence check** via `process.kill(pid, 0)` — dead PIDs get dropped from the map.
- **TTL enforcement** — subagents older than `delegation.ttlMinutes` get `SIGTERM`ed, then `SIGKILL`ed after a 5-second grace period.

On proxy shutdown (`SIGTERM`, `SIGINT`, or stdio `close`), the proxy `SIGTERM`s every tracked child and waits 2 seconds before `SIGKILL`ing survivors. This mitigates bug #33947 (orphan accumulation on session end) with **3 layers of cleanup**:

1. Clean proxy shutdown handler (`runProxy`) sends `SIGTERM` to all tracked children on any exit signal.
2. Startup reconciliation (`SubagentManager.init`) reads the persisted `subagents.json`, runs `process.kill(pid, 0)` existence checks, drops dead PIDs, and re-adds still-live ones to the in-memory map so the TTL continues counting.
3. Periodic `setInterval(60_000).unref()` TTL sweep kills anything still alive past `delegation.ttlMinutes`.

Worst-case orphan lifetime is therefore bounded to `ttlMinutes` (default 15). Without any of these layers, long-running `claude --print` subprocesses can accumulate if the parent session dies abruptly.

## Rollback

To disable delegation and restore pure Phase 1 behavior without reinstalling the plugin:

```bash
# Edit config
vim ~/.claude/channels/awb/config.json
# Set "delegation": {"enabled": false}
# Restart the proxy (Claude CLI will auto-restart it on the next session)
```

In-flight subagents continue running until they complete naturally or hit their TTL — the config change only affects NEW triggers. New triggers after the restart go through the legacy `sendChannelEvent` forward-to-main path (`type: "agent_trigger"` meta), identical to Phase 1 behavior. `chat_request` SSE events are silently ignored when delegation is off (Phase 2 emitted them with no consumer; "do nothing" is the correct non-delegation behavior).

## Logs

All proxy output goes to **stderr**, tagged with `[AWB]`. Subagent stdout/stderr lines are re-tagged with `[subagent:<pid>]` / `[subagent:<pid>:err]` and written to the same stream. Run `claude -d` to see them in the main session's log tail.

Typical log sequence for a successful trigger:

```
[AWB] SubagentManager initialized (pidDir=/.../subagents, cap=5, ttl=15min)
[AWB] Subagent spawned: pid=12345 kind=trigger ticket=abc-123
[AWB] Trigger dispatched to subagent: ticket=abc-123 pid=12345
[AWB] [subagent:12345] {"type":"result","subtype":"success","is_error":false, ...}
[AWB] Subagent exit: pid=12345 kind=trigger code=0 signal=- duration=42s
```

For a delegation-disabled legacy path:

```
[AWB] Trigger forwarded (legacy path): ticket=abc-123 role=coder
```

For a TTL kill:

```
[AWB] Subagent TTL exceeded: pid=12345 — SIGTERM
[AWB] Subagent exit: pid=12345 kind=trigger code=null signal=SIGTERM duration=900s
```

## Troubleshooting

**Subagent spawns but immediately exits with a Claude CLI MCP error.** Most likely cause is the `--mcp-config` wrapper shape. The file MUST be `{"mcpServers": {...}}` not bare `{serverName: {...}}`. The proxy writes the correct shape — if you see this error with a fresh subagent config, check whether something else modified the file (`ls -la ~/.claude/channels/awb/subagents/<pid>/`).

**Subagents hang indefinitely with no output.** The child's stdin is being left open somewhere. The proxy uses `stdio: ['ignore', 'pipe', 'pipe']` to close stdin on spawn. If this happens, check for stdio inheritance bugs in your Claude CLI installation or a custom `claudeBin` wrapper script.

**Orphan `claude` processes accumulating on the host.** Run `ps aux | grep "claude --print"` to count them. If more than `maxConcurrent`, the TTL sweep is not running — check proxy stderr for `[AWB] SubagentManager initialized`. If missing, the init failed (usually permissions on `~/.claude/channels/awb/subagents/`). Manual cleanup: `pkill -TERM -f "claude --print"` then restart the main session.

**Cap reached immediately after proxy restart.** Startup reconciliation kept records from the previous session. Either wait up to `ttlMinutes` for them to be swept, or manually remove `~/.claude/channels/awb/subagents.json` and restart the proxy. Check with `cat ~/.claude/channels/awb/subagents.json` — if PIDs listed no longer exist (`ps -p <pid>`), the reconciliation logic has a bug and the file needs to be removed manually.

**Subagent exits with code 0 but never comments on the ticket.** The subagent ran but didn't call any MCP tools. Check the `[subagent:<pid>]` log lines for the task prompt — a truncated or malformed `ticket_prompt` can produce this. Verify the agent's `role_prompt` is set in AWB (agents without a role_prompt fall back to an empty system prompt).

**Completion notification never arrives.** The `SubagentManager.onExit` hook is unassigned. This is normal in test harnesses but should never happen in `runProxy()` — check that plugin version is 0.5.0+ (`cat ~/.claude/channels/awb/../.claude-plugin/plugin.json`).

## Daemon Mode (v0.36.0+)

`daemon.mjs` is a sibling entrypoint to `proxy.mjs` that runs the same SSE + subagent pipeline without an MCP-stdio bridge. It exists to support the multi-CLI Agent Manager work (ticket f338f6a5) — Phase 1 reuses the existing Claude CLI subagent path without changes.

```bash
node submodules/claude-plugins/ai-workflow-board/daemon.mjs
```

What's the same vs the proxy:
- Reads the same `~/.claude/channels/awb/{config,agent}.json`.
- Reuses `SubagentManager`, `ChatSessionManager`, `TicketSessionManager`, `EventStream`, `SubagentMonitor`, `FsBrowser`, `PresenceHeartbeat`, `cleanupOrphanSubagents`, `uploadIfNewErrors`, `onFlushThreshold`.
- Same `~/.claude/channels/awb/proxy.log` log file (lines are pid-tagged so daemon and proxy entries are distinguishable).
- Same on-disk subagent state (`subagents.json` + per-subagent cfg files in `~/.claude/channels/awb/subagents/`); orphan cleanup is sibling-aware so a daemon and a proxy running side-by-side won't kill each other's children.

What's missing vs the proxy:
- No MCP stdio forward — the daemon does not bridge a Claude CLI's mcp tool calls.
- No `notifications/claude/channel` notifications back to a parent — there is no parent. Subagent completion is logged only.
- Fallback paths in `EventDispatcher` that call `sendChannelEvent` (cap reached, delegation disabled, …) still write a JSON-RPC notification line to the daemon's stdout. Nothing consumes it; the same path is logged. For the daemon, delegation should always be enabled (the daemon is delegation).

Concurrent run with `proxy.mjs`: discouraged but not forbidden. The AWB server pins agent-targeted events (`agent_trigger`, `chat_request`, `chat_room_message`, `comment_mention`, `fs_request`) to one main SSE session per agent, so most events go to one path. Broadcast events still fan out to both. A future phase will add a lockfile to make the two mutually exclusive.

Phase scope (ticket f338f6a5):
- Phase 1 (this version): daemon entrypoint with claude-CLI subagent parity.
- Phase 2: per-CLI adapter abstraction (gemini, codex, …).
- Phase 3: AWB server / web UI control surface for daemons.
- Phase 4: lockfile, daemon self-update, per-agent config UI.

## Related

- Phase 4 research: `.planning/phases/04-subagent-delegation/04-RESEARCH.md`
- Phase 4 context: `.planning/phases/04-subagent-delegation/04-CONTEXT.md`
- Bug references:
  - [anthropics/claude-code#13605](https://github.com/anthropics/claude-code/issues/13605) — does NOT apply to this spawn path. That bug affects plugin-defined `.claude/agents/*.md` subagents invoked via the Task tool. Phase 4 spawns a fresh `claude --print` CLI process with `--mcp-config` + `--allowedTools mcp__awb__*`, a completely different mechanism that grants full MCP tool access.
  - [anthropics/claude-code#33947](https://github.com/anthropics/claude-code/issues/33947) — mitigated by the 3-layer cleanup described in the TTL Sweep section above.
- Test harness: `test/subagent-manager.test.mjs` (9 lifecycle scenarios) and `test/subagent-delegation.test.mjs` (6 end-to-end scenarios). Both use `test/fake-claude.sh` as the `claudeBin` stub — zero dependency on a real Claude CLI install or login to run the test suite.
