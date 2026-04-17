// ─── AWB Proxy Constants ──────────────────────────────────
// Shared values used across proxy.mjs and lib/ modules. Keep this file free
// of runtime side effects (no fs writes, no timers, no logger init) so it
// can be imported from anywhere without circular-import risk.

import { join } from 'path';
import { homedir } from 'os';

export const CONFIG_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'config.json',
);
export const AGENT_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'agent.json',
);
export const RECONNECT_INITIAL_MS = 2000;
export const RECONNECT_MAX_MS = 30000;
export const REQUEST_TIMEOUT_MS = 30000;
export const HEARTBEAT_INTERVAL_MS = 30_000;  // Phase 3 D-52 presence — must be < sweep's 90s threshold

export const CHANNEL_INSTRUCTIONS = [
  'This server uses push-based event delivery via SSE.',
  'You will receive <channel> events for all ticket activity:',
  '  - type="agent_trigger": A trigger assigned to you — claim the ticket, read it, and process it.',
  '  - type="board_update": A ticket was updated (comment added, status changed, field edited, etc.).',
  'Do NOT poll or create cron jobs for get_pending_triggers or subscribe_events — events arrive automatically via push.',
].join('\n');

// ─── Delegation Constants (Phase 4 D-55..D-75) ────────────
export const SUBAGENTS_BASE_DIR = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'subagents',
);
export const SUBAGENTS_PERSIST_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'subagents.json',
);
export const DELEGATION_DEFAULTS = Object.freeze({
  enabled: true,              // D-59: default-on; consumers still gated by Plan 04-03
  maxConcurrent: 5,           // D-63
  ttlMinutes: 15,             // D-67 (trigger subagents only)
  claudeBin: 'claude',        // D-75 — overridable for test stubs
  appendSystemPromptMode: 'role_only', // D-75 — reserved for Plan 04-03 prompt composition
  // v0.7.0: persistent per-room chat subagents. When false, chat events spawn a
  // fresh Claude CLI per message (legacy v0.6.x behavior) — rollback hatch.
  persistentChatSessions: true,
  persistentTicketSessions: true, // v0.8.0: persistent per-ticket subagents
  idleMinutes: 10,            // session idle TTL before stdin is closed
  maxTurnsPerSession: 30,     // soft respawn after N user turns to bound context growth
});
export const TTL_SWEEP_INTERVAL_MS = 60_000;
export const SIGTERM_GRACE_MS = 5_000;
export const STOP_GRACE_MS = 2_000;
