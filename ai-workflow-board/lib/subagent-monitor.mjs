// ─── Subagent monitor client ─────────────────────────────────────
// Tracks every Claude CLI subagent spawned by this plugin and reports its
// stream-json traffic to the AWB server so the web UI can render a live
// transcript across every agent machine.
//
// Pattern: each spawn site calls `register(...)` with a unique subagent_id
// and a description; that returns a Tap object with `inLine(line)` /
// `outLine(line)` / `end({...})`. The tap batches lines (200ms or 50 lines)
// and POSTs them to /api/agent-subagents/:id/lines so we don't hammer the
// server with one POST per token. Failures are logged but never thrown — a
// degraded monitor must NEVER block the actual subagent traffic.

import { randomUUID } from 'crypto';
import { REQUEST_TIMEOUT_MS } from './constants.mjs';
import { log } from './logging.mjs';

const FLUSH_INTERVAL_MS = 200;
const FLUSH_LINE_THRESHOLD = 50;

export class SubagentMonitor {
  #config;
  #workspaceId;
  #enabled;

  /**
   * @param {object} config loaded plugin config
   * @param {string|null} workspaceId optional — server falls back to the API
   *   key's bound workspace when the plugin omits this. Plugin doesn't know
   *   workspace_id at startup since agent.json doesn't carry it.
   */
  constructor(config, workspaceId) {
    this.#config = config;
    this.#workspaceId = workspaceId || null;
    // On by default; opt out via config.subagent_monitor.enabled = false.
    this.#enabled = this.#config?.subagent_monitor?.enabled !== false;
    if (this.#enabled) {
      log(`[subagent-monitor] enabled (workspace=${this.#workspaceId ? this.#workspaceId.slice(0, 8) + '...' : 'auto-bind via api key'})`);
    } else {
      log('[subagent-monitor] disabled (config.subagent_monitor.enabled=false)');
    }
  }

  /**
   * Register a freshly-spawned subagent. Returns a tap object whose methods
   * are no-ops when the monitor is disabled, so callers don't need to
   * branch on `enabled` at every callsite.
   */
  register({ kind, sessionKey, pid, label, ticketId, ticketTitle, role }) {
    if (!this.#enabled) return makeNoopTap();
    const subagentId = randomUUID();
    const startedAt = new Date().toISOString();

    // Fire register POST in background — even if it fails, we still buffer
    // and try to flush; server simply ignores unknown ids.
    const body = {
      subagent_id: subagentId,
      kind,
      session_key: sessionKey || '',
      pid: pid || 0,
      started_at: startedAt,
      label,
    };
    if (this.#workspaceId) body.workspace_id = this.#workspaceId;
    if (ticketId) body.ticket_id = ticketId;
    if (ticketTitle) body.ticket_title = ticketTitle;
    if (role) body.role = role;
    this.#post('/api/agent-subagents', body).catch(() => {});

    return new SubagentTap(this, subagentId, startedAt);
  }

  async _flushLines(subagentId, lines) {
    if (!this.#enabled || !lines.length) return;
    await this.#post(`/api/agent-subagents/${encodeURIComponent(subagentId)}/lines`, { lines });
  }

  async _end(subagentId, info) {
    if (!this.#enabled) return;
    await this.#post(`/api/agent-subagents/${encodeURIComponent(subagentId)}/end`, info || {});
  }

  async #post(path, body) {
    try {
      const url = `${this.#config.url.replace(/\/$/, '')}${path}`;
      await fetch(url, {
        method: 'POST',
        headers: {
          'X-Agent-Key': this.#config.apiKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch (err) {
      log(`[subagent-monitor] POST ${path} failed: ${err.message}`);
    }
  }
}

class SubagentTap {
  #monitor;
  #subagentId;
  #buffer = [];
  #flushTimer = null;
  #ended = false;
  startedAt;

  constructor(monitor, subagentId, startedAt) {
    this.#monitor = monitor;
    this.#subagentId = subagentId;
    this.startedAt = startedAt;
  }

  inLine(line) { this.#append('in', line); }
  outLine(line) { this.#append('out', line); }

  #append(direction, line) {
    if (this.#ended || !line) return;
    this.#buffer.push({ direction, line, ts: new Date().toISOString() });
    if (this.#buffer.length >= FLUSH_LINE_THRESHOLD) {
      this.#flushNow();
    } else if (!this.#flushTimer) {
      this.#flushTimer = setTimeout(() => this.#flushNow(), FLUSH_INTERVAL_MS);
    }
  }

  #flushNow() {
    if (this.#flushTimer) { clearTimeout(this.#flushTimer); this.#flushTimer = null; }
    const batch = this.#buffer.splice(0);
    if (!batch.length) return;
    this.#monitor._flushLines(this.#subagentId, batch).catch(() => {});
  }

  async end(info) {
    if (this.#ended) return;
    this.#ended = true;
    this.#flushNow();
    // Slight delay so the line POST lands before the end POST when both are
    // racing — server tolerates either order, but ordered makes UI render
    // cleaner (no "ended" flicker before the last lines).
    setTimeout(() => this.#monitor._end(this.#subagentId, info).catch(() => {}), 50);
  }
}

function makeNoopTap() {
  return {
    inLine() {},
    outLine() {},
    async end() {},
    startedAt: new Date().toISOString(),
  };
}
