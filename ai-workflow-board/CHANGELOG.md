# Changelog

## v0.59.0 — 2026-06-25

Marketplace-cache bump for the AWB MCP tool surface added by the security
scheduler / manual full-inspection feature (ai-workflow-board ticket 7c07c19d):

- `start_security_batch` / `get_security_batch` — run several security profiles
  SEQUENTIALLY ("수동 전체 점검"): profile N+1 only dispatches after profile N
  reaches a terminal status, never all at once. Pass an ordered `profile_ids`
  list or `all: true` to expand to every enabled profile in scope at dispatch
  time. `stop_on_fail` halts on the first non-passed run.
- `list_security_schedules` / `get_security_schedule` /
  `create_security_schedule` / `update_security_schedule` /
  `delete_security_schedule` / `run_security_schedule_now` — an automatic
  trigger layer that kicks the sequential batch when due. `scope` is `all`
  (resolve enabled profiles at dispatch time) or `selected` (explicit
  `profile_ids`); cadence is exactly one of `cron` (5 UTC fields) or
  `interval_ms`. `run_security_schedule_now` fires immediately without
  disturbing the automatic cadence.

The proxy itself is unchanged — it's a pure pass-through forwarder, so the new
tools land automatically once Claude Code re-fetches the AWB server's
`tools/list`. Version bump exists only to invalidate the marketplace cache per
CLAUDE.md's "Plugin version sync" rule.

## v0.51.0 — 2026-06-18

Marketplace-cache bump for the AWB MCP tool surface change added by the board
output-language (i18n) feature (ai-workflow-board ticket ae28dcaf):

- `update_board` — gains an optional `language` input (a human-readable
  language name, e.g. "Korean"). Agents dispatched on the board write their
  comments / chat / commit messages / code comments in that language; the
  server folds it into the dispatch system prompt. Empty string / null clears
  the override (agents fall back to their default, English).

The proxy itself is unchanged — it's a pure pass-through forwarder, so the
extended `update_board` schema lands automatically once Claude Code re-fetches
the AWB server's `tools/list`. Version bump exists only to invalidate the
marketplace cache per CLAUDE.md's "Plugin version sync" rule.

## v0.44.0 — 2026-05-28

Marketplace-cache bump for the AWB MCP tool surface added by the ticket
prerequisites / dependency feature (ai-workflow-board ticket 48d14fff):

- `add_ticket_prerequisites` — block a ticket until the listed prerequisite
  ticket(s) reach a terminal column. Auto-resumes (no human action) once every
  prerequisite lands terminal. Prefer this over `pend_ticket` whenever the
  blocker is another ticket rather than human input.
- `remove_ticket_prerequisite` — drop a single prerequisite link; removing the
  last remaining link auto-unblocks the ticket.
- `list_ticket_prerequisites` — list a ticket's prerequisite links. The same
  data is also folded into the `get_ticket` response under `prerequisites`, so
  no extra call is needed in the common case.
- `pend_ticket` description tightened to "human input only" to steer agents
  toward `add_ticket_prerequisites` for ticket-on-ticket waits.

The proxy itself is unchanged — it's a pure pass-through forwarder, so the
new tools and the extended `get_ticket` schema land automatically once Claude
Code re-fetches the AWB server's `tools/list`. Version bump exists only to
invalidate the marketplace cache per CLAUDE.md's "Plugin version sync" rule.

## v0.43.0 — 2026-05-26

Marketplace-cache bump for the AWB MCP tool surface added by the chat
attachment feature (ai-workflow-board ticket 92082b55):

- `add_chat_message_attachment` — upload a file into a chat room (base64).
  Returns an `attachment_id` the caller passes to `send_chat_room_message`.
- `delete_chat_message_attachment` — discard a pending (pre-send) upload.
  Bound (sent) attachments live and die with their message; use room
  deletion for those.
- `send_chat_room_message` schema gained an optional `attachment_ids: string[]`
  field so agent-authored chat messages can carry file attachments.
- `chat_room_message` SSE payload and the `GET /api/chat-rooms/:room_id/messages`
  history fetch now include an `attachments[]` projection (`id`, `filename`,
  `mime_type`, `size_bytes`, `download_url`, `thumbnail_url?`). The history
  endpoint stays REST-only — no `list_chat_messages` MCP tool is added.

The proxy itself is unchanged — it's a pure pass-through forwarder, so the
new tools and the extended `send_chat_room_message` schema land
automatically once Claude Code re-fetches the AWB server's `tools/list`.
Version bump exists only to invalidate the marketplace cache per
CLAUDE.md's "Plugin version sync" rule.

## v0.42.0 — 2026-05-25

Marketplace-cache bump for the AWB MCP tool surface added by the ticket
auto-archive feature (ai-workflow-board ticket 9b44526b):

- `list_archived_tickets` — paginated archived ticket lookup.
- `archive_ticket` — manual archive (with activity_log audit).
- `unarchive_ticket` — restore (resets `terminal_entered_at`).
- `get_board` / `get_board_summary` grew an `include_archived` boolean.
- `update_board` accepts `auto_archive_days` (null or 1..365).

The proxy itself is unchanged — it's a pure pass-through forwarder, so
the new tools land automatically once Claude Code re-fetches the AWB
server's `tools/list`. Version bump exists only to invalidate the
marketplace cache per CLAUDE.md's "Plugin version sync" rule.

## v0.40.0 — 2026-05-02

**Breaking:** the daemon and proxy-side delegation pipeline have been removed.
Background work (SSE events, subagent spawning, persistent ticket/chat
sessions, CLI adapters) now lives in the standalone `@awb/agent-manager`
package shipped from the `ai-workflow-board` repo (`apps/agent-manager/`).

What's left in this plugin is a pure stdio MCP bridge: `proxy.mjs` plus the
files it needs to forward Claude CLI's MCP JSON-RPC traffic to the AWB
server with stale-session recovery and retry. See README.md for the
migration path.

Removed:
- `daemon.mjs`
- `lib/{agent-lockfile,base-session-manager,chat-session-manager,claude-bin-resolver,cli-resolver,error-log-uploader,event-dispatcher,event-log-recorder,event-stream,fs-browser,instance-heartbeat,mcp-client,orphan-cleanup,presence-heartbeat,prompts,rest,self-update,subagent-manager,subagent-monitor,ticket-session-manager}.mjs`
- `lib/cli-adapters/`
- `test/{agent-lockfile,chat-session-manager,self-update,subagent-delegation,subagent-manager}.test.mjs`, `test/fake-claude.sh`
- `claude/channel` capability injection (no SSE events delivered through this proxy anymore)
- `PROXY.md` (delegation guide is no longer relevant)

## v0.39.0 — 2026-04-30

Per-process instance heartbeat for the Phase 3 Agent Manager dashboard.

(...older history elided — see git log on this submodule for prior versions.)
