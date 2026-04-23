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
// v0.26.0: supervisor logic moved server-side (TicketSupervisorService). The
// plugin no longer polls get_allocated_tickets or tracks session silence /
// progress cadence. Server re-pushes agent_trigger (with force_respawn for
// wedged sessions) when my_last_update_at stalls past 30 min.

export const CHANNEL_INSTRUCTIONS = [
  'This server uses push-based event delivery via SSE.',
  'You will receive <channel> events for all ticket activity:',
  '  - type="agent_trigger": A trigger assigned to you — claim the ticket, read it, and process it.',
  '  - type="board_update": A ticket was updated (comment added, status changed, field edited, etc.).',
  'Events arrive automatically via push — you do not need to manually poll for them.',
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
  // Raised from 5 → 15 in v0.29.x because per-role session split (a single
  // ticket may now occupy up to 3 slots — assignee/reporter/reviewer) made
  // 5 too tight: three parallel active tickets already saturated the cap.
  // 15 gives ~5 concurrent tickets × 3 roles headroom. Users hitting this
  // can raise it further via config.delegation.maxConcurrent.
  maxConcurrent: 15,          // D-63
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
