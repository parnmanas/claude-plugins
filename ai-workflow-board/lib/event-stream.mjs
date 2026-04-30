// ─── SSE Event Stream ─────────────────────────────────────

import { networkInterfaces } from 'os';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { RECONNECT_INITIAL_MS, RECONNECT_MAX_MS } from './constants.mjs';
import { log } from './logging.mjs';
import { EventDispatcher } from './event-dispatcher.mjs';

// Plugin version — read once at module load from the same plugin.json
// the marketplace consults, so we never drift out of sync with the
// actual installed version. Falls back to 'unknown' if the file moves
// in some weird install layout.
const PLUGIN_VERSION = (() => {
  try {
    const here = dirname(fileURLToPath(import.meta.url));
    const raw = readFileSync(join(here, '..', '.claude-plugin', 'plugin.json'), 'utf8');
    return String(JSON.parse(raw).version || '').trim() || 'unknown';
  } catch {
    return 'unknown';
  }
})();

/**
 * Best-effort local IP. Picks the first non-internal IPv4 address
 * across all NICs. Returns 'unknown' on hosts where /api isn't
 * reachable from a routable interface (rare). Server prefers this
 * value over its own x-real-ip / x-forwarded-for inference because
 * reverse proxies obscure the true peer; the plugin knows what NIC
 * it actually connects from.
 */
function detectLocalIp() {
  try {
    const ifaces = networkInterfaces();
    for (const list of Object.values(ifaces)) {
      if (!list) continue;
      for (const addr of list) {
        if (addr.family === 'IPv4' && !addr.internal) return addr.address;
      }
    }
  } catch { /* fall through */ }
  return 'unknown';
}

/**
 * Connect to AWB's SSE /api/events/stream and forward ticket events
 * as claude/channel notifications. Reconnects with exponential backoff.
 *
 * AWB SSE event types (from events.controller.ts):
 *   - board_update:  ticket/comment CRUD (entity_type, action, field_changed, actor_name)
 *   - agent_trigger: trigger assigned to agent (role, trigger_id, agent_id)
 *   - agent_typing:  typing indicator (ignored by proxy)
 *
 * Responsibilities are narrow by design:
 *   - HTTP fetch + AbortController + reconnect backoff
 *   - SSE line parsing (event: / data: / blank-line terminator)
 *   - Handing each (eventType, raw) pair to the injected EventDispatcher
 *
 * Dispatch decisions (persistent session vs one-shot subagent vs legacy
 * main-session forward) live in EventDispatcher — see lib/event-dispatcher.mjs.
 */
export class EventStream {
  #url;
  #retryDelay = RECONNECT_INITIAL_MS;
  #abortController = null;
  #stopped = false;
  #dispatcher;
  #onConnect;

  /**
   * Back-compat constructor — accepts the legacy 5-arg form so proxy.mjs and
   * existing tests keep working without modification. Internally builds an
   * EventDispatcher from the injected managers.
   *
   * `onConnect` (6th positional, optional) fires after every successful SSE
   * connect — both initial and reconnect. Used by proxy.mjs to kick an
   * immediate presence ping so the dashboard recovers ONLINE within seconds
   * of a server restart instead of waiting up to a full heartbeat interval.
   */
  constructor(config, subagentManager = null, chatSessionManager = null, ticketSessionManager = null, fsBrowser = null, onConnect = null) {
    this.#url = `${config.url.replace(/\/$/, '')}/api/events/stream?token=${encodeURIComponent(config.apiKey)}`;
    this.#dispatcher = new EventDispatcher(config, {
      subagentManager,
      chatSessionManager,
      ticketSessionManager,
      fsBrowser,
    });
    this.#onConnect = onConnect;
  }

  start() {
    this.#stopped = false;
    this.#connect();
  }

  stop() {
    this.#stopped = true;
    this.#abortController?.abort();
  }

  async #connect() {
    if (this.#stopped) return;

    try {
      this.#abortController = new AbortController();
      // X-Plugin-Ip / X-Plugin-Version let the server stamp Agent
      // Details with the plugin's actual local IP + version instead
      // of inferring from x-real-ip / x-forwarded-for (which gets
      // mangled by reverse proxies). Old plugins that don't ship
      // these headers fall back to 'unknown' on the server side.
      const resp = await fetch(this.#url, {
        headers: {
          Accept: 'text/event-stream',
          'X-Plugin-Ip': detectLocalIp(),
          'X-Plugin-Version': PLUGIN_VERSION,
        },
        signal: this.#abortController.signal,
      });

      if (!resp.ok) {
        log(`SSE error: ${resp.status} ${resp.statusText}`);
        this.#scheduleReconnect();
        return;
      }

      log('SSE connected');
      this.#retryDelay = RECONNECT_INITIAL_MS;
      // Kick an immediate presence ping so dashboard ONLINE recovers fast
      // after a server restart (the SSE reconnect is our earliest signal that
      // the server is back). Errors swallowed inside the callback.
      try { this.#onConnect?.(); } catch { /* hook errors must not block stream read */ }
      await this.#readStream(resp.body);

      // Stream ended cleanly — reconnect
      log('SSE stream ended, reconnecting...');
      this.#scheduleReconnect();
    } catch (err) {
      if (err.name === 'AbortError') return;
      log(`SSE error: ${err.message}`);
      this.#scheduleReconnect();
    }
  }

  async #readStream(body) {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let eventType = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop(); // keep incomplete line

      for (const line of lines) {
        if (line.startsWith('event: ')) {
          // SSE event type field — NestJS sets this from MessageEvent.type
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const data = line.slice(6).trim();
          if (data) this.#dispatcher.dispatch(eventType, data);
          // Reset after processing data (SSE spec: dispatch on blank line,
          // but we process eagerly since each event: + data: pair is atomic)
          eventType = '';
        } else if (line === '') {
          // Blank line = end of SSE event block
          eventType = '';
        }
      }
    }
  }

  #scheduleReconnect() {
    if (this.#stopped) return;
    setTimeout(() => this.#connect(), this.#retryDelay);
    this.#retryDelay = Math.min(this.#retryDelay * 1.5, RECONNECT_MAX_MS);
  }

  // Test-only accessors — only exposed when AWB_TEST_MODE is set. Lets Plan 04-04
  // integration tests invoke the private handlers without opening a real SSE stream.
  _testDispatchTrigger(raw) { return this.#dispatcher.handleTrigger(raw); }
  _testDispatchChatRequest(raw) { return this.#dispatcher.handleChatRequest(raw); }
  _testDispatchChatRoomMessage(raw) { return this.#dispatcher.handleChatRoomMessage(raw); }
}
