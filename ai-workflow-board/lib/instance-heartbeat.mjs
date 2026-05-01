// ─── Instance Heartbeat (Phase 3 — Agent Manager dashboard) ────────────────
//
// Periodically POST per-process metadata to AWB so the admin
// `/admin/agent-manager` page can render a list of every daemon / proxy that
// is currently alive and heartbeating against the server.
//
// PresenceHeartbeat already stamps Agent.last_seen_at — that flag is enough
// for the existing online/offline indicator, but it collapses every running
// process for one agent down to a single bit. This heartbeat preserves the
// per-process fan-out the dashboard needs:
//
//   - mode             daemon | proxy
//   - hostname         os.hostname()
//   - plugin_version   read from plugin.json by the caller
//   - cli              the adapter we booted with (claude, gemini, …)
//   - cli_adapters     all known adapters this binary exposes
//   - pid              process pid
//   - started_at       boot time of the process (set once)
//
// Cadence: same 30s clock as PresenceHeartbeat. Server's TTL is 90s (3
// missed heartbeats) so a forgotten process drops off the dashboard within
// roughly two cycles.

import { hostname } from 'os';
import { randomUUID } from 'crypto';
import { HEARTBEAT_INTERVAL_MS, REQUEST_TIMEOUT_MS } from './constants.mjs';
import { log } from './logging.mjs';

export class InstanceHeartbeat {
  #config;
  #agentId;
  #payloadFactory;
  #instanceId;
  #startedAt;
  #timer = null;
  #stopped = false;

  /**
   * @param {object} config            ~/.claude/channels/awb/config.json
   * @param {string|null} agentId      resolved agent id (or null until ready)
   * @param {object} meta              { mode, version, cli, cliAdapters }
   */
  constructor(config, agentId, meta) {
    this.#config = config;
    this.#agentId = agentId;
    this.#instanceId = randomUUID();
    this.#startedAt = new Date().toISOString();
    const cliAdapters = Array.isArray(meta?.cliAdapters)
      ? meta.cliAdapters.map((s) => String(s)).filter(Boolean)
      : [];
    this.#payloadFactory = () => ({
      instance_id: this.#instanceId,
      agent_id: this.#agentId,
      workspace_id: config?.workspace_id || null,
      mode: meta?.mode === 'daemon' ? 'daemon' : 'proxy',
      hostname: hostname() || 'unknown',
      plugin_version: String(meta?.version || 'unknown'),
      cli: String(meta?.cli || 'claude'),
      cli_adapters: cliAdapters,
      pid: process.pid,
      started_at: this.#startedAt,
    });
  }

  start() {
    if (!this.#agentId) {
      log('Instance heartbeat skipped — agent_id not in agent.json (run /awb:setup)');
      return;
    }
    this.#stopped = false;
    this.#post().catch((err) => log(`Instance heartbeat (initial) failed: ${err.message}`));
    this.#timer = setInterval(() => {
      this.#post().catch((err) => log(`Instance heartbeat failed: ${err.message}`));
    }, HEARTBEAT_INTERVAL_MS);
    this.#timer.unref?.();
    log(`Instance heartbeat started (instance=${this.#instanceId.slice(0, 8)}…)`);
  }

  stop() {
    this.#stopped = true;
    if (this.#timer) {
      clearInterval(this.#timer);
      this.#timer = null;
    }
  }

  get instanceId() {
    return this.#instanceId;
  }

  async #post() {
    if (this.#stopped) return;
    const payload = this.#payloadFactory();
    if (!payload.agent_id) return;
    const url = `${this.#config.url.replace(/\/$/, '')}/api/agent/instance-heartbeat`;
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'X-Agent-Key': this.#config.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!resp.ok) {
      // 404 is the expected response from a server older than v0.39 — keep the
      // log low-noise so an upgrade lag doesn't spam the proxy log.
      if (resp.status === 404) return;
      throw new Error(`POST /api/agent/instance-heartbeat HTTP ${resp.status}`);
    }
    await resp.text().catch(() => null);
  }
}
