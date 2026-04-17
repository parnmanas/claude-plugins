// ─── Presence Heartbeat (Phase 3 D-52 / D-53) ─────────────

import { HEARTBEAT_INTERVAL_MS, REQUEST_TIMEOUT_MS } from './constants.mjs';
import { log } from './logging.mjs';

/**
 * Periodically call the `ping` MCP tool against AWB so this agent stays
 * marked online in the dashboard. Without this, nothing on the Claude side
 * ever touches `last_seen_at`, and the server's 90-second sweep would
 * immediately mark us offline.
 *
 * Mechanism: open a short-lived MCP session (initialize → tools/call ping)
 * every HEARTBEAT_INTERVAL_MS. The session is torn down after each ping —
 * this is cheaper than keeping a persistent session alive across the whole
 * proxy lifetime and avoids Mcp-Session-Id contention with the Claude CLI's
 * own session (which flows through stdio on this same proxy).
 *
 * Fires once immediately on start() so the dashboard reflects online
 * status within the first second of the proxy's lifetime instead of waiting
 * 30s for the first tick.
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
    const base = this.#config.url.replace(/\/$/, '');
    const url = `${base}/mcp`;
    const headers = {
      Authorization: `Bearer ${this.#config.apiKey}`,
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
    };

    // Step 1: initialize to get Mcp-Session-Id
    const initResp = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2025-03-26',
          capabilities: { experimental: { 'awb/schemaVersion': { version: 2 } } },
          clientInfo: { name: 'awb-presence-heartbeat', version: '1.0.0' },
        },
      }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!initResp.ok) {
      throw new Error(`initialize HTTP ${initResp.status}`);
    }
    const sid = initResp.headers.get('mcp-session-id');
    if (!sid) throw new Error('initialize did not return Mcp-Session-Id');
    // Drain response body so the connection releases
    await initResp.text().catch(() => null);

    const sessionHeaders = { ...headers, 'Mcp-Session-Id': sid };

    // Step 2: notifications/initialized (required by MCP spec before tool calls)
    await fetch(url, {
      method: 'POST',
      headers: sessionHeaders,
      body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    }).then((r) => r.text().catch(() => null));

    // Step 3: tools/call ping
    const pingResp = await fetch(url, {
      method: 'POST',
      headers: sessionHeaders,
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tools/call',
        params: {
          name: 'ping',
          arguments: { agent_id: this.#agentId },
        },
      }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!pingResp.ok) {
      throw new Error(`tools/call ping HTTP ${pingResp.status}`);
    }
    await pingResp.text().catch(() => null);
    log(`Presence ping ok (agent=${this.#agentId.slice(0, 8)})`);

    // Step 4: DELETE session to free server-side state
    fetch(url, { method: 'DELETE', headers: sessionHeaders, signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) })
      .then((r) => r.text().catch(() => null))
      .catch(() => { /* ignore — server handles expired sessions via TTL */ });
  }
}
