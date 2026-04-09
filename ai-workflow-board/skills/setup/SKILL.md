---
name: setup
description: Set up AWB (AI Workflow Board) connection — register API key, link agent identity, verify connection. Use when the user pastes an AWB API key, asks to configure AWB, asks "how do I connect to AWB," wants to check AWB connection status, or mentions AWB setup.
user-invocable: true
allowed-tools:
  - Read
  - Write
  - Bash(ls *)
  - Bash(mkdir *)
  - Bash(chmod *)
  - Bash(curl *)
---

# /awb:setup — AI Workflow Board Connection Setup

Manages AWB server connection and agent identity for this Claude Code instance.
The MCP plugin reads `.mcp.json` at boot for server URL and API key.

Arguments passed: `$ARGUMENTS`

---

## State files

- **MCP config:** `${CLAUDE_PLUGIN_ROOT}/.mcp.json` — MCP server definition with URL and auth header
- **Agent state:** `~/.claude/channels/awb/agent.json` — cached agent identity and preferences

### agent.json shape

```json
{
  "server_url": "https://awb.example.com:7700",
  "agent_id": "uuid",
  "agent_name": "Claude-Parn",
  "agent_type": "claude",
  "key_hint": "awb_f448***dff3",
  "scope": "full",
  "connected_at": "2026-04-09T12:00:00Z"
}
```

---

## Dispatch on arguments

### No args — status check

Read both state files and show connection status:

1. **Server** — read `${CLAUDE_PLUGIN_ROOT}/.mcp.json`. Show URL (mask the key).
   - If not configured: *"No AWB server configured. Run `/awb:setup <server-url> <api-key>` to connect."*

2. **Agent identity** — read `~/.claude/channels/awb/agent.json`.
   - Show: agent name, type, ID, scope, connected time
   - If missing: *"API key is set but agent identity not verified. Run `/awb:setup verify` to check."*

3. **Connection test** — curl the server's MCP endpoint to verify it's reachable:
   ```bash
   curl -s -o /dev/null -w "%{http_code}" -X POST "<server_url>/mcp" \
     -H "Authorization: Bearer <key>" \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"awb-setup","version":"1.0.0"}},"id":1}'
   ```
   - 200 = connected, show session ID from response
   - 401/403 = key invalid or expired
   - Connection refused = server unreachable

4. **What next** — suggest based on state:
   - Not configured → provide setup command
   - Configured but not verified → suggest verify
   - Working → *"Ready. AWB tools are available as `mcp__plugin_ai-workflow-board_*`."*

### `<server-url> <api-key>` — configure connection

1. Parse arguments: first arg is server URL, second is API key.
   - URL should be like `https://host:port` (no trailing `/mcp`)
   - API key starts with `awb_` prefix

2. Test connection before saving:
   ```bash
   curl -s -X POST "<url>/mcp" \
     -H "Authorization: Bearer <key>" \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"awb-setup","version":"1.0.0"}},"id":1}'
   ```

3. If connection succeeds:
   - Write `${CLAUDE_PLUGIN_ROOT}/.mcp.json`:
     ```json
     {
       "mcpServers": {
         "ai-workflow-board": {
           "type": "http",
           "url": "<url>/mcp",
           "headers": {
             "Authorization": "Bearer <key>"
           }
         }
       }
     }
     ```
   - Call the MCP `list_agents` tool via curl to find which agent this key belongs to
   - Save agent identity to `~/.claude/channels/awb/agent.json`
   - Inform user: *"Connected as <agent_name>. Restart session or run `/reload-plugins` to activate MCP tools."*

4. If connection fails:
   - Show the error (401 = bad key, connection refused = wrong URL)
   - Do NOT save config

### `verify` — verify and refresh agent identity

1. Read current `.mcp.json` for URL and key
2. Test connection (same curl as above)
3. Use the MCP endpoint to call `list_agents` and identify which agent owns this key
4. Update `~/.claude/channels/awb/agent.json`
5. Show: agent name, type, scope, boards available

### `disconnect` — remove connection

1. Reset `.mcp.json` to empty MCP config:
   ```json
   {
     "mcpServers": {}
   }
   ```
2. Delete `~/.claude/channels/awb/agent.json` if it exists
3. Inform: *"AWB disconnected. Run `/awb:setup <url> <key>` to reconnect."*

---

## Implementation notes

- The plugin's `.mcp.json` is read at boot. Changes need `/reload-plugins` or session restart.
- API keys start with `awb_` prefix and are 44 chars long.
- The server supports both `/mcp` (NestJS integrated, port 7701) and standalone MCP (port 7702).
- `agent.json` is a cache — the source of truth is the server's DB.
- Never log or display the full API key. Always mask: show first 8 chars + `***` + last 4 chars.
- `mkdir -p ~/.claude/channels/awb` before writing agent.json.
- `chmod 600` on agent.json since it contains key hints.
