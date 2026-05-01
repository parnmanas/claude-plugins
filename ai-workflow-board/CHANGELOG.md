# Changelog

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
