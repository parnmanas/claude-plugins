#!/usr/bin/env node

/**
 * AWB MCP Proxy — stdio-to-HTTP bridge with Channel support
 *
 * Architecture:
 *   Claude CLI <--stdio--> proxy.mjs <--HTTP--> AWB Server
 *                                    <--SSE---  AWB /api/events/stream
 *
 * Two responsibilities:
 * 1. Proxy: Forward MCP JSON-RPC messages between Claude CLI (stdio) and AWB server (HTTP)
 * 2. Channel: Listen to AWB's SSE event stream and deliver agent_trigger events to Claude
 *
 * The proxy intercepts the MCP `initialize` handshake to inject `claude/channel`
 * capability — without this, Claude CLI ignores channel notifications.
 *
 * Config: ~/.claude/channels/awb/config.json
 * { "url": "https://awb.example.com:7700", "apiKey": "awb_..." }
 */

import { readFileSync, existsSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';
import { createInterface } from 'readline';

// ─── Constants ────────────────────────────────────────────

const CONFIG_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'config.json',
);
const RECONNECT_INITIAL_MS = 2000;
const RECONNECT_MAX_MS = 30000;
const REQUEST_TIMEOUT_MS = 30000;

const CHANNEL_INSTRUCTIONS = [
  'This server uses push-based triggers via SSE.',
  'If you receive <channel> events with type="agent_trigger", react to them immediately — claim the ticket, read it, and process it.',
  'Do NOT poll or create cron jobs for get_pending_triggers or subscribe_events — triggers arrive automatically via push.',
].join('\n');

// ─── Helpers ──────────────────────────────────────────────

/** Guard against unhandled errors crashing Claude CLI */
process.on('uncaughtException', (err) => {
  log(`Uncaught error: ${err.message}`);
});
process.on('unhandledRejection', (err) => {
  log(`Unhandled rejection: ${err}`);
});

function log(msg) {
  process.stderr.write(`[AWB] ${msg}\n`);
}

function send(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function sendError(id, code, message) {
  send({ jsonrpc: '2.0', id: id ?? null, error: { code, message } });
}

/** Send a channel notification to Claude — the core delivery mechanism */
function sendChannelEvent(content, meta = {}) {
  send({
    jsonrpc: '2.0',
    method: 'notifications/claude/channel',
    params: { content, meta },
  });
}

function loadConfig() {
  if (!existsSync(CONFIG_PATH)) return null;
  try {
    return JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
  } catch {
    return null;
  }
}

// ─── SSE Event Stream ─────────────────────────────────────

/**
 * Connect to AWB's SSE /api/events/stream and forward agent_trigger events
 * as claude/channel notifications. Reconnects with exponential backoff.
 *
 * AWB SSE format (from events.controller.ts):
 *   event: agent_trigger
 *   data: {"event_type":"agent_trigger","ticket_id":"...","action":"assignee",
 *          "field_changed":"<trigger_id>","actor_name":"<agent_id>","timestamp":"..."}
 */
class EventStream {
  #url;
  #retryDelay = RECONNECT_INITIAL_MS;
  #abortController = null;
  #stopped = false;

  constructor(config) {
    this.#url = `${config.url.replace(/\/$/, '')}/api/events/stream?token=${encodeURIComponent(config.apiKey)}`;
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
      const resp = await fetch(this.#url, {
        headers: { Accept: 'text/event-stream' },
        signal: this.#abortController.signal,
      });

      if (!resp.ok) {
        log(`SSE error: ${resp.status} ${resp.statusText}`);
        this.#scheduleReconnect();
        return;
      }

      log('SSE connected');
      this.#retryDelay = RECONNECT_INITIAL_MS;
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
          if (data && eventType === 'agent_trigger') {
            this.#handleTrigger(data);
          }
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

  #handleTrigger(raw) {
    try {
      const ev = JSON.parse(raw);
      // Map AWB event fields → channel notification meta
      // AWB events.controller uses: action=role, field_changed=trigger_id, actor_name=agent_id
      sendChannelEvent(
        `[AWB Trigger] ticket=${ev.ticket_id} role=${ev.action} trigger=${ev.field_changed}`,
        {
          type: 'agent_trigger',
          ticket_id: ev.ticket_id || '',
          trigger_id: ev.field_changed || '',
          agent_id: ev.actor_name || '',
          role: ev.action || '',
          timestamp: ev.timestamp || new Date().toISOString(),
        },
      );
      log(`Trigger forwarded: ticket=${ev.ticket_id} role=${ev.action}`);
    } catch (err) {
      log(`Failed to parse trigger: ${err.message}`);
    }
  }

  #scheduleReconnect() {
    if (this.#stopped) return;
    setTimeout(() => this.#connect(), this.#retryDelay);
    this.#retryDelay = Math.min(this.#retryDelay * 1.5, RECONNECT_MAX_MS);
  }
}

// ─── MCP Proxy ────────────────────────────────────────────

/**
 * Forward a JSON-RPC message to the AWB MCP server over HTTP.
 * Returns { body, sessionId } for JSON responses,
 * or { lines, sessionId } for SSE (streaming) responses.
 */
async function forwardToServer(mcpUrl, apiKey, msg, sessionId) {
  const headers = {
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
    Authorization: `Bearer ${apiKey}`,
  };
  if (sessionId) headers['mcp-session-id'] = sessionId;

  const resp = await fetch(mcpUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify(msg),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });

  const newSessionId = resp.headers.get('mcp-session-id') || sessionId;
  const contentType = resp.headers.get('content-type') || '';

  if (contentType.includes('text/event-stream')) {
    // SSE response — extract data: lines as separate JSON-RPC messages
    const text = await resp.text();
    const lines = text.split('\n')
      .filter(l => l.startsWith('data: '))
      .map(l => l.slice(6).trim())
      .filter(Boolean);
    return { sessionId: newSessionId, lines };
  }

  // JSON response
  const text = await resp.text();
  let body = null;
  if (text.trim()) {
    try { body = JSON.parse(text); } catch { /* malformed */ }
  }
  return { sessionId: newSessionId, body };
}

/**
 * Patch the initialize response to declare claude/channel capability.
 * Without this, Claude CLI won't process notifications/claude/channel messages.
 * Ref: Discord plugin declares this via MCP SDK capabilities.experimental['claude/channel']
 */
function patchInitializeResponse(body) {
  if (!body?.result) return body;

  // Inject channel capability
  const caps = body.result.capabilities ??= {};
  const exp = caps.experimental ??= {};
  exp['claude/channel'] = {};

  // Append channel instructions
  const existing = body.result.instructions || '';
  body.result.instructions = [existing, '', CHANNEL_INSTRUCTIONS]
    .join('\n').trim();

  return body;
}

// ─── Entry Points ─────────────────────────────────────────

/** Handle the not-configured state — respond to MCP handshake with empty tools */
function runUnconfigured(rl) {
  log('Not configured. Run /awb:setup <server-url> <api-key> to connect.');

  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      if (msg.method === 'initialize') {
        send({
          jsonrpc: '2.0',
          id: msg.id,
          result: {
            protocolVersion: '2025-03-26',
            capabilities: { tools: {} },
            serverInfo: { name: 'ai-workflow-board', version: '0.2.0' },
          },
        });
      } else if (msg.method === 'tools/list') {
        send({ jsonrpc: '2.0', id: msg.id, result: { tools: [] } });
      } else if (msg.method === 'notifications/initialized') {
        // no response
      } else if (msg.id !== undefined) {
        sendError(msg.id, -32000, 'AWB not configured. Run /awb:setup <server-url> <api-key>');
      }
    } catch { /* ignore malformed */ }
  });
}

/** Main proxy — bridges stdio MCP to remote AWB server + SSE channel */
function runProxy(rl, config) {
  const mcpUrl = config.url.replace(/\/$/, '') + '/mcp';
  let sessionId = null;
  let eventStream = null;

  rl.on('line', async (line) => {
    let msg;
    try { msg = JSON.parse(line); } catch { return; }

    // ── Intercept: initialize ──────────────────────────────
    // Patch the server's response to include claude/channel capability
    if (msg.method === 'initialize') {
      try {
        const result = await forwardToServer(mcpUrl, config.apiKey, msg, sessionId);
        if (result.sessionId) sessionId = result.sessionId;
        send(patchInitializeResponse(result.body));
      } catch (err) {
        log(`Initialize error: ${err.message}`);
        if (msg.id !== undefined) sendError(msg.id, -32000, `AWB proxy error: ${err.message}`);
      }
      return;
    }

    // ── Intercept: initialized notification ────────────────
    // Start SSE stream AFTER handshake completes (Claude is ready to receive)
    if (msg.method === 'notifications/initialized') {
      if (!eventStream) {
        eventStream = new EventStream(config);
        eventStream.start();
        log('SSE event stream started (post-handshake)');
      }
      return;
    }

    // ── Forward everything else to AWB server ──────────────
    try {
      const result = await forwardToServer(mcpUrl, config.apiKey, msg, sessionId);
      if (result.sessionId) sessionId = result.sessionId;

      if (result.lines) {
        // SSE response — write each JSON-RPC message directly
        for (const line of result.lines) {
          process.stdout.write(line + '\n');
        }
      } else if (result.body && msg.id !== undefined) {
        send(result.body);
      }
    } catch (err) {
      log(`Proxy error: ${err.message}`);
      if (msg.id !== undefined) {
        const errMsg = err.cause?.code === 'ECONNREFUSED'
          ? `AWB server unreachable at ${config.url}. Is the server running?`
          : `AWB proxy error: ${err.message}`;
        sendError(msg.id, -32000, errMsg);
      }
    }
  });

  rl.on('close', () => {
    eventStream?.stop();
    process.exit(0);
  });

  log(`Proxy ready (server: ${config.url})`);
}

// ─── Main ─────────────────────────────────────────────────

const config = loadConfig();
const rl = createInterface({ input: process.stdin });

if (!config?.url || !config?.apiKey) {
  runUnconfigured(rl);
} else {
  runProxy(rl, config);
}
