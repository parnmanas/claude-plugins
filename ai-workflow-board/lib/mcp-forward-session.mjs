// ─── MCP Forward Session Manager ──────────────────────────
//
// Owns the sessionId used to forward Claude CLI's MCP JSON-RPC requests to
// the AWB server. Three things this solves that the old inline
// `let sessionId = null` couldn't:
//
//   1. Session expiry recovery. AWB evicts MCP sessions after 10 min idle
//      (see apps/server/.../session-store.ts SESSION_TTL_MS). If Claude CLI
//      goes quiet past that window, the next request (usually a periodic
//      `tools/list` refresh) returns HTTP 404 "Session not found. Please
//      re-initialize.". The old proxy forwarded that error to Claude CLI,
//      which interpreted it as server death and closed stdin — killing the
//      proxy. Now: stale session is detected, we re-initialize silently,
//      and retry the original request. Claude CLI never sees the blip.
//
//   2. Idle keepalive. PresenceHeartbeat uses throwaway sessions so it can't
//      refresh the forward session's lastActivity. We now ping tools/list on
//      the forward session every 4 min (< TTL/2) so it never goes stale in
//      the first place. The re-init path stays as a safety net for cases
//      keepalive can't catch (server restart, transient 5xx).
//
//   3. Network / transient failures. fetch errors (ECONNRESET, timeout,
//      502/503/504) get retried with exponential backoff before being
//      returned to Claude CLI. A single bad minute shouldn't kill the proxy.
//
// Concurrency: readline fires 'line' events without awaiting the async
// handler, so multiple forward() calls may run in parallel. #initPromise
// dedupes concurrent re-init so parallel requests share one handshake.

import { REQUEST_TIMEOUT_MS } from './constants.mjs';
import { log } from './logging.mjs';

// Server's session TTL is 10 min idle. Ping faster than TTL/2 so a missed
// tick doesn't push us past the threshold.
const KEEPALIVE_INTERVAL_MS = 4 * 60 * 1000;

// Retry budget for network-layer failures (not stale-session — that retries
// once regardless, since we have a deterministic recovery).
const MAX_NETWORK_RETRIES = 3;
const RETRY_BACKOFF_MS = [1000, 2000, 4000];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class McpForwardSession {
  #mcpUrl;
  #apiKey;
  #sessionId = null;
  #initPromise = null;
  // Cached copy of Claude CLI's original `initialize` request — replayed
  // verbatim for silent re-init so the server sees identical capabilities.
  #cachedInitMsg = null;
  #keepaliveTimer = null;
  #stopped = false;
  #onReinit;

  /**
   * `onReinit` (3rd positional, optional) fires after a successful silent
   * re-initialize. Used by proxy.mjs to kick an immediate presence ping so
   * the dashboard recovers ONLINE within seconds of a server restart.
   */
  constructor(mcpUrl, apiKey, onReinit = null) {
    this.#mcpUrl = mcpUrl;
    this.#apiKey = apiKey;
    this.#onReinit = onReinit;
  }

  get sessionId() { return this.#sessionId; }

  /**
   * Forward Claude CLI's FIRST initialize request. Caches the payload so we
   * can replay it during silent re-init, stores the returned sessionId, and
   * starts the keepalive ticker. Returns `{ body, lines, sessionId }` so the
   * proxy can patch + send the response to Claude.
   */
  async handleClaudeInitialize(msg) {
    this.#cachedInitMsg = JSON.parse(JSON.stringify(msg));
    const result = await this.#doFetch(msg, { skipSessionHeader: true });
    if (result.sessionId) this.#sessionId = result.sessionId;
    this.#startKeepalive();
    log(`Forward session initialized (sid=${(this.#sessionId || '').slice(0, 8)})`);
    return result;
  }

  /**
   * Forward a non-initialize request. Handles:
   *   - stale session (HTTP 404 "Session not found"): re-init, retry once
   *   - network/5xx: exponential backoff retry up to MAX_NETWORK_RETRIES
   * Transparent to caller — same return shape as #doFetch.
   */
  async forward(msg) {
    if (!this.#sessionId) {
      await this.#reinitializeSilently();
    }

    let lastErr = null;
    let staleRetried = false;

    for (let attempt = 0; attempt <= MAX_NETWORK_RETRIES; attempt++) {
      // Snapshot the sid we're about to send with so we can detect whether a
      // concurrent forward() already rotated it behind us during the fetch.
      const sidAtRequest = this.#sessionId;
      try {
        const result = await this.#doFetch(msg, { skipSessionHeader: false });

        if (result.staleSession) {
          if (staleRetried) {
            // Already re-inited once and still got stale — something else is
            // wrong. Bail so the caller can surface a proper error.
            throw new Error('Forward session repeatedly stale after re-init');
          }
          if (this.#sessionId === sidAtRequest) {
            // We're the first to notice the expiry. Clear and re-init.
            log('Forward session stale — re-initializing and retrying');
            this.#sessionId = null;
            await this.#reinitializeSilently();
          } else {
            // A concurrent forward() already rotated to a fresh sid while
            // our request was in flight. Just retry with the current sid —
            // no reason to trigger a second re-init.
            log('Forward session stale — another call already recovered, retrying');
          }
          staleRetried = true;
          continue;
        }

        if (result.sessionId) this.#sessionId = result.sessionId;
        return result;
      } catch (err) {
        lastErr = err;
        if (attempt < MAX_NETWORK_RETRIES) {
          const delay = RETRY_BACKOFF_MS[attempt] ?? 4000;
          log(`Forward error (${err.message}) — retry ${attempt + 1}/${MAX_NETWORK_RETRIES} in ${delay}ms`);
          await sleep(delay);
        }
      }
    }
    throw lastErr;
  }

  stop() {
    this.#stopped = true;
    if (this.#keepaliveTimer) {
      clearInterval(this.#keepaliveTimer);
      this.#keepaliveTimer = null;
    }
  }

  // ─── Internal ───────────────────────────────────────────

  async #doFetch(msg, { skipSessionHeader }) {
    const headers = {
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
      Authorization: `Bearer ${this.#apiKey}`,
    };
    if (!skipSessionHeader && this.#sessionId) {
      headers['mcp-session-id'] = this.#sessionId;
    }

    const resp = await fetch(this.#mcpUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify(msg),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });

    const newSessionId = resp.headers.get('mcp-session-id') || this.#sessionId;
    const contentType = resp.headers.get('content-type') || '';

    // AWB mcp.controller.ts:313-321 — stale session returns HTTP 404 JSON-RPC error
    // with message "Session not found. Please re-initialize.". Treat that as a signal
    // to recover rather than a real error to surface to Claude.
    if (resp.status === 404) {
      const text = await resp.text().catch(() => '');
      if (/session not found/i.test(text) || /re-initialize/i.test(text)) {
        return { staleSession: true };
      }
      let body = null;
      if (text.trim()) { try { body = JSON.parse(text); } catch { /* malformed */ } }
      return { sessionId: newSessionId, body };
    }

    // Retry-worthy server errors bubble up as exceptions so the retry loop picks them up.
    if (resp.status >= 500) {
      await resp.text().catch(() => null);
      throw new Error(`HTTP ${resp.status} ${resp.statusText}`);
    }

    if (contentType.includes('text/event-stream')) {
      const text = await resp.text();
      const lines = text.split('\n')
        .filter((l) => l.startsWith('data: '))
        .map((l) => l.slice(6).trim())
        .filter(Boolean);
      return { sessionId: newSessionId, lines };
    }

    const text = await resp.text();
    let body = null;
    if (text.trim()) {
      try { body = JSON.parse(text); } catch { /* malformed */ }
    }
    return { sessionId: newSessionId, body };
  }

  /**
   * Re-initialize the forward session without Claude CLI's involvement.
   * Claude CLI initializes once at startup and never again — so we replay
   * its cached payload (with its capabilities) against the server to get a
   * fresh sessionId, matching the initial handshake exactly. Dedupes
   * concurrent callers via #initPromise.
   */
  async #reinitializeSilently() {
    if (this.#initPromise) return this.#initPromise;
    this.#initPromise = (async () => {
      try {
        const initMsg = this.#cachedInitMsg || this.#buildMinimalInit();
        const headers = {
          'Content-Type': 'application/json',
          Accept: 'application/json, text/event-stream',
          Authorization: `Bearer ${this.#apiKey}`,
        };
        const resp = await fetch(this.#mcpUrl, {
          method: 'POST',
          headers,
          body: JSON.stringify(initMsg),
          signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
        });
        if (!resp.ok) throw new Error(`re-initialize HTTP ${resp.status}`);
        const sid = resp.headers.get('mcp-session-id');
        if (!sid) throw new Error('re-initialize returned no mcp-session-id');
        await resp.text().catch(() => null);
        this.#sessionId = sid;
        log(`Forward session re-initialized (sid=${sid.slice(0, 8)})`);
        // Kick an immediate presence ping — re-init means the server just
        // came back, so don't wait up to 30s for the next heartbeat tick to
        // recover dashboard ONLINE. Errors swallowed.
        try { this.#onReinit?.(); } catch { /* hook errors must not break re-init */ }
      } finally {
        this.#initPromise = null;
      }
    })();
    return this.#initPromise;
  }

  #buildMinimalInit() {
    return {
      jsonrpc: '2.0',
      id: 1,
      method: 'initialize',
      params: {
        protocolVersion: '2025-03-26',
        capabilities: { experimental: { 'awb/schemaVersion': { version: 2 } } },
        clientInfo: { name: 'awb-proxy-forward', version: '1.0.0' },
      },
    };
  }

  #startKeepalive() {
    if (this.#keepaliveTimer) return;
    this.#keepaliveTimer = setInterval(() => {
      if (this.#stopped) return;
      this.#pingForwardSession().catch((err) => log(`Forward keepalive failed: ${err.message}`));
    }, KEEPALIVE_INTERVAL_MS);
    this.#keepaliveTimer.unref?.();
  }

  /**
   * Touch the forward session to reset its server-side TTL. tools/list is
   * cheap (no side effects) and exists on every AWB server. If the session
   * is already gone we reinit eagerly so the next real forward() doesn't
   * eat the reinit latency.
   */
  async #pingForwardSession() {
    if (!this.#sessionId) {
      await this.#reinitializeSilently();
      return;
    }
    const headers = {
      'Content-Type': 'application/json',
      Accept: 'application/json, text/event-stream',
      Authorization: `Bearer ${this.#apiKey}`,
      'mcp-session-id': this.#sessionId,
    };
    const resp = await fetch(this.#mcpUrl, {
      method: 'POST',
      headers,
      body: JSON.stringify({ jsonrpc: '2.0', id: 'awb-forward-keepalive', method: 'tools/list' }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (resp.status === 404) {
      log('Forward keepalive: session gone — reinitializing');
      this.#sessionId = null;
      await resp.text().catch(() => null);
      await this.#reinitializeSilently();
      return;
    }
    if (!resp.ok) {
      log(`Forward keepalive: HTTP ${resp.status}`);
    }
    await resp.text().catch(() => null);
  }
}
