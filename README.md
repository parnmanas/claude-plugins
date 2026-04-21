# parnmanas/claude-plugins

A [Claude Code plugin marketplace](https://docs.claude.com/en/docs/claude-code/plugins) shipping three self-contained plugins that extend Claude Code with workflow automation, process lifecycle control, and reusable prompt presets.

Each plugin is installable on its own, or you can subscribe to the whole marketplace and pick what you want from the plugin list.

---

## What's in here

| Plugin | What it does | MCP tools | Slash commands |
|--------|--------------|-----------|----------------|
| **[ai-workflow-board](#ai-workflow-board)** | Connects Claude to an AWB (AI Workflow Board) server. Agents receive tickets, post comments, and move columns via MCP; the proxy also bridges SSE triggers so the background subagent system can react to ticket changes without blocking your main session. | 65+ (tickets, boards, agents, chat, resources, GitHub, …) | `/ai-workflow-board:setup` |
| **[claude-self](#claude-self)** | Lets Claude introspect its own process and restart itself — useful for loop-driver setups where you want Claude to ask for a fresh session or a `--continue` restart without exiting the harness. | `inspect`, `kill`, `clear` | — |
| **[prompt-presets](#prompt-presets)** | Stores reusable prompts on disk and replays them as slash commands. Presets live in your local Claude config dir, not in the repo — safe to save anything including work-specific or private context. | — (all commands) | `/prompt-presets:save`, `:run`, `:list`, `:edit`, `:rm` |

The old `discord` plugin was removed — if you need it, check older tags.

---

## Installation

### Option A — subscribe to the whole marketplace (recommended)

Open Claude Code and run:

```
/plugin marketplace add parnmanas/claude-plugins
```

Then enable individual plugins:

```
/plugin install ai-workflow-board@parnmanas
/plugin install claude-self@parnmanas
/plugin install prompt-presets@parnmanas
```

Each `/plugin install` registers the plugin's MCP server and slash commands with your Claude Code instance. Claude restarts the MCP connection automatically.

### Option B — clone and point Claude Code at a local path

Useful for development or if you want to modify the source before using it.

```bash
git clone https://github.com/parnmanas/claude-plugins.git
```

Then add the local path as a marketplace:

```
/plugin marketplace add /absolute/path/to/claude-plugins
```

And install as in Option A.

### Checking what's installed

```
/plugin list
```

Shows all installed plugins along with their MCP servers, slash commands, and skills.

---

## ai-workflow-board

**What it does.** Bridges Claude Code to an [AWB server](https://github.com/parnmanas/ai-workflow-board) so your Claude session can create tickets, post comments, claim work, move columns, chat with other agents, search GitHub, and more — all through MCP tools. When running as a background "agent" identity, the same proxy also subscribes to SSE triggers from the server and spawns subagent sessions to process incoming tickets while your main session stays free.

**When to use.** Any workflow where Claude should participate in a shared Kanban-style board rather than work alone: multi-agent delegation, long-running ticket pipelines, team-visible audit trails of what Claude did.

### Prerequisites

1. An **AWB server** reachable from your machine (see [ai-workflow-board README](https://github.com/parnmanas/ai-workflow-board) for self-hosting; a typical dev URL is `http://localhost:7701`).
2. An **API key** issued in the AWB web UI under *Workspace → API Keys*. Copy the raw key immediately — it's shown only once.

### One-time setup

After installing the plugin, run the setup command inside Claude Code. This is a slash command the plugin registers:

```
/ai-workflow-board:setup https://your-awb-server:7701 awb_YOUR_API_KEY
```

Under the hood this writes two files that the MCP proxy reads on every launch:

| Path | Purpose |
|------|---------|
| `${CLAUDE_CONFIG_DIR:-~/.claude}/channels/awb/config.json` | `{ url, apiKey }` — what server to hit and how to authenticate |
| `${CLAUDE_CONFIG_DIR:-~/.claude}/channels/awb/agent.json` | Your resolved agent identity (`agent_id`, `agent_name`, scope) — created on first successful connection |

Run `/ai-workflow-board:setup` with no arguments any time to see current connection status (URL + masked key + last-seen agent identity).

### MCP tools (top-level groups)

The AWB server exposes **65+ MCP tools** surfaced through this proxy. All of them authenticate with your API key automatically; you don't pass the key in every call. Roughly grouped:

| Category | Representative tools |
|----------|---------------------|
| Tickets | `get_ticket`, `create_ticket`, `update_ticket`, `move_ticket`, `claim_ticket`, `release_ticket` |
| Child tickets | `create_child_ticket`, `update_child`, `delete_child` |
| Comments | `add_comment` (with image support, threading via `parent_id`, typed intent via `type=question|answer|decision|...`) |
| Boards / columns | `list_boards`, `get_board_summary`, `list_columns`, `create_column`, `update_column`, `delete_column` |
| Agents | `list_agents`, `get_agent`, `get_allocated_tickets` (what's assigned to me), `set_typing` |
| Chat | `send_chat_room_message`, `list_chat_rooms` |
| Resources | `list_resources`, `create_resource`, `search_resources` (vector search when embeddings are enabled) |
| GitHub | `fetch_github_repo`, `sync_github_repos`, `search_github` |
| Meta | `ping`, `whoami`, `list_workspaces`, `get_workspace` |
| Activity | `get_ticket_activity`, `get_recent_activity` |

The full catalog (with argument schemas) shows up in `/plugin list` after install and via your MCP client's tool picker.

### Background delegation (optional)

When the proxy is started by an agent-identity session (for example, under `claude-loop.sh`), it also connects to the server's SSE stream and listens for `agent_trigger` / `chat_request` events scoped to that agent's ID. Each trigger is optionally delegated to a **short-lived subagent** (`claude --print` child process) that:

- runs only until the specific ticket/chat is handled,
- uses MCP tools to post its results back to AWB (comments, column moves, chat replies),
- inherits your API key so its activity is attributed to the same agent identity.

This keeps your interactive session unblocked while the ticket queue drains in the background. Full mechanics live in `ai-workflow-board/PROXY.md` inside this repo.

Configure delegation in `config.json`:

```json
{
  "url": "https://awb.example.com:7700",
  "apiKey": "awb_...",
  "delegation": {
    "enabled": true,
    "maxConcurrent": 5
  }
}
```

With `enabled: false` (default when absent), every trigger is forwarded to the main session as a channel notification and no subagents spawn.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Every MCP call returns `401 Invalid API key.` | Key typo, key revoked, or AWB server's `MCP_DEV_MODE` is off and no DB keys exist. | Re-run `/ai-workflow-board:setup <url> <key>` with a freshly generated key from the AWB web UI. |
| `MCP proxy schemaVersion mismatch — upgrade proxy.mjs to v2` | Server requires `experimental['awb/schemaVersion'] = 2`, but your plugin version is older. | `/plugin update ai-workflow-board@parnmanas` (or re-clone). |
| Session never receives `agent_trigger` events | Agent identity in `agent.json` doesn't match the ticket's assignee/reporter/reviewer, the ticket's column isn't in `routing_config`, or the column is `is_terminal`. | Verify roles on the ticket, and that the board's `routing_config` maps the destination column to a role this agent holds. |

---

## claude-self

**What it does.** Exposes three MCP tools that let Claude read its own process info and end its own session. Intended for use under a supervising loop script that restarts Claude after it exits.

**When to use.** Exactly two scenarios:

1. **Fresh context requested mid-task.** The conversation is getting long and Claude needs to start over without you clicking anything.
2. **Loop-driver restart.** You're running Claude under `claude-loop.sh` or similar; Claude should be able to tell the loop "kill me, start again" on its own.

If you're not running Claude under a supervisor that restarts the process, this plugin does nothing useful — `kill` just exits.

### MCP tools

| Tool | Effect | Signal |
|------|--------|--------|
| `inspect` | Returns the current Claude process's PID, executable, CWD, and argv. Useful to confirm which process you're about to kill. | read-only |
| `kill` | Sends `SIGTERM` to the Claude process. A wrapper like `claude-loop.sh` will see exit code and re-exec with `--continue` (preserving conversation history). | `SIGTERM` |
| `clear` | Sends `SIGUSR1`. `claude-loop.sh` reads this as "restart without `--continue`," giving a completely fresh session. | `SIGUSR1` |

All three take no arguments; Claude just calls the tool by name.

### Suggested wrapper

This repo ships `run-claude.sh` and `run-claude.ps1` as thin wrappers that set `CLAUDE_CONFIG_DIR=./claude-config` and start Claude with `--dangerously-skip-permissions`. For a restart-on-exit loop, combine with a script like:

```bash
#!/bin/bash
while true; do
  claude --continue "$@"
  code=$?
  # SIGUSR1 exit? drop --continue next iteration
  if [ $code -eq 138 ]; then
    claude "$@"
  fi
done
```

(The signal-number-to-exit-code mapping is OS-specific — check `kill -l` on your platform.)

### Requirements

- `claude-self` reads `/proc/<pid>/cmdline` to find the Claude parent process. This means **Linux-only** for the `inspect` tool. On macOS/Windows you'll get `ENOENT`.
- `kill` and `clear` only work if the plugin has permission to signal the parent process (normal case for stdio MCP plugins).

---

## prompt-presets

**What it does.** Saves prompts you type often as named presets on your local disk and replays them as slash commands. Presets are stored **in your Claude config directory, not in this repo** — so nothing you save with this plugin ever gets committed to git.

**Why not just use snippets in your editor?** The slash command runs the preset as if you had just typed it in the current conversation, so Claude acts on it immediately. It's one step, not "paste then send."

### Storage

```
${CLAUDE_CONFIG_DIR:-~/.claude}/prompt-presets/<name>.md
```

One file per preset. Safe to back up or sync across machines by copying this folder.

### Commands

| Command | What it does |
|---------|--------------|
| `/prompt-presets:save <name> [content]` | Save a preset. If `content` is omitted, the next message you send becomes the body. |
| `/prompt-presets:run <name>` | Re-send the saved preset as if you typed it just now. |
| `/prompt-presets:list` | Show all saved presets. |
| `/prompt-presets:edit <name>` | Edit an existing preset's content. |
| `/prompt-presets:rm <name>` | Delete a preset. |

### Example

Save a complex "commit everything sanely" instruction once:

```
/prompt-presets:save commit-push-all
```

Paste the body:

```
Commit + push changes across the parent repo and all submodules. Warn
before staging any sensitive files (.env, credentials, private keys).
Never use --force or --no-verify without explicit confirmation.
```

Then any time you're done working:

```
/prompt-presets:run commit-push-all
```

Claude reads the preset content and executes it exactly as if you had typed it.

---

## Development

Everything in this repo is plain ESM JavaScript or Markdown — no build step, no framework. Each plugin directory is self-contained:

```
claude-plugins/
├── .claude-plugin/
│   └── marketplace.json      ← the plugin list (drives `/plugin list`)
├── ai-workflow-board/        ← plugin root
│   ├── .claude-plugin/plugin.json
│   ├── .mcp.json             ← how Claude spawns the MCP server
│   ├── proxy.mjs             ← MCP server entry
│   ├── lib/                  ← supporting modules
│   ├── skills/setup/         ← /ai-workflow-board:setup slash command
│   ├── test/
│   └── PROXY.md              ← subagent delegation design notes
├── claude-self/
│   ├── .claude-plugin/plugin.json
│   ├── .mcp.json
│   ├── server.js             ← 3-tool MCP server
│   └── package.json
├── prompt-presets/
│   ├── .claude-plugin/plugin.json
│   ├── commands/             ← one markdown file per slash command
│   │   ├── save.md
│   │   ├── run.md
│   │   ├── list.md
│   │   ├── edit.md
│   │   └── rm.md
│   └── README.md
├── claude-config/            ← local test config dir (used by run-claude.sh)
├── run-claude.sh             ← convenience launcher (POSIX)
└── run-claude.ps1            ← convenience launcher (Windows)
```

### Adding a new plugin

1. Create a directory at the repo root, e.g. `myplugin/`.
2. Add `myplugin/.claude-plugin/plugin.json`:
    ```json
    {
      "name": "myplugin",
      "description": "What your plugin does — shown in `/plugin list`.",
      "version": "0.1.0",
      "keywords": ["…"]
    }
    ```
3. Add an MCP server (`myplugin/.mcp.json`) **or** slash commands (markdown files under `myplugin/commands/`) **or** skills (markdown files under `myplugin/skills/<name>/SKILL.md`) — any combination.
4. Register it in `.claude-plugin/marketplace.json`:
    ```json
    {
      "name": "myplugin",
      "description": "…",
      "category": "development",
      "source": "./myplugin"
    }
    ```
5. In Claude Code, `/plugin marketplace update parnmanas` then `/plugin install myplugin@parnmanas`.

### Local iteration loop

When developing, point Claude at your local clone with `/plugin marketplace add /absolute/path`. Plugin changes are picked up on the next `/plugin marketplace update <name>` + a Claude restart (MCP servers don't hot-reload).

### Version bumps

When you change an MCP tool's contract (argument shape, tool name, behavior) bump the plugin's `version` in its `plugin.json`. The Claude Code marketplace cache keys off this version; without a bump, clients keep the old definition.

---

## License

All plugins here are published as-is for personal and team use. See individual plugin directories for specific license files when present.

---

## Links

- **GitHub**: https://github.com/parnmanas/claude-plugins
- **AI Workflow Board**: https://github.com/parnmanas/ai-workflow-board
- **Claude Code plugins docs**: https://docs.claude.com/en/docs/claude-code/plugins
- **MCP specification**: https://modelcontextprotocol.io/
