// ─── Presence Heartbeat (Phase 3 D-52 / D-53) ─────────────

import { HEARTBEAT_INTERVAL_MS, REQUEST_TIMEOUT_MS } from './constants.mjs';
import { log } from './logging.mjs';

/**
 * Periodically stamp `last_seen_at` on the AWB Agent row so the dashboard
 * keeps this agent marked online. The server's 90-second sweep flips
 * is_online=0 the moment a heartbeat lapses past that window, so we tick
 * every HEARTBEAT_INTERVAL_MS (30s by default).
 *
 * v0.35.1: switched from MCP `ping` tool to a single REST POST. The MCP
 * variant required a 4-step session dance (initialize → notifications/
 * initialized → tools/call → DELETE) per heartbeat — every proxy did this
 * every 30s, and at scale (multiple proxies per agent, many agents) it
 * dominated MCP session churn (one create + one close per tick) and
 * flooded the server log. POST /api/agent/ping is the same DB update with
 * a single round-trip.
 *
 * Fires once immediately on start() so the dashboard reflects online
 * status within the first second of the proxy's lifetime instead of
 * waiting 30s for the first tick.
 */
export class PresenceHeartbeat {
  #config;
  #agentId;
  #timer = null;
  #stopped = false;

  constructor(config, agentId) {
    this.#config = config;
    this.#agentId = agentId;
  }

  start() {
    if (!this.#agentId) {
      log('Presence heartbeat skipped — agent_id not in agent.json (run /ai-workflow-board:setup)');
      return;
    }
    this.#stopped = false;
    // Fire once immediately, then on interval
    this.#ping().catch((err) => log(`Presence ping (initial) failed: ${err.message}`));
    this.#timer = setInterval(() => {
      this.#ping().catch((err) => log(`Presence ping failed: ${err.message}`));
    }, HEARTBEAT_INTERVAL_MS);
    this.#timer.unref?.();
    log(`Presence heartbeat started (agent=${this.#agentId.slice(0, 8)} interval=${HEARTBEAT_INTERVAL_MS / 1000}s)`);
  }

  stop() {
    this.#stopped = true;
    if (this.#timer) {
      clearInterval(this.#timer);
      this.#timer = null;
    }
  }

  async #ping() {
    if (this.#stopped) return;
    const url = `${this.#config.url.replace(/\/$/, '')}/api/agent/ping`;
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'X-Agent-Key': this.#config.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ agent_id: this.#agentId }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!resp.ok) {
      throw new Error(`POST /api/agent/ping HTTP ${resp.status}`);
    }
    await resp.text().catch(() => null);
  }
}
