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

import { readFileSync, existsSync, appendFileSync, mkdirSync, statSync, renameSync } from 'fs';
import { promises as fsp } from 'fs';
import { join, dirname } from 'path';
import { homedir } from 'os';
import { createInterface } from 'readline';
import { spawn } from 'child_process';

// ─── Constants ────────────────────────────────────────────

const CONFIG_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'config.json',
);
const AGENT_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'agent.json',
);
const RECONNECT_INITIAL_MS = 2000;
const RECONNECT_MAX_MS = 30000;
const REQUEST_TIMEOUT_MS = 30000;
const HEARTBEAT_INTERVAL_MS = 30_000;  // Phase 3 D-52 presence — must be < sweep's 90s threshold

const CHANNEL_INSTRUCTIONS = [
  'This server uses push-based event delivery via SSE.',
  'You will receive <channel> events for all ticket activity:',
  '  - type="agent_trigger": A trigger assigned to you — claim the ticket, read it, and process it.',
  '  - type="board_update": A ticket was updated (comment added, status changed, field edited, etc.).',
  'Do NOT poll or create cron jobs for get_pending_triggers or subscribe_events — events arrive automatically via push.',
].join('\n');

// ─── Delegation Constants (Phase 4 D-55..D-75) ────────────
const SUBAGENTS_BASE_DIR = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'subagents',
);
const SUBAGENTS_PERSIST_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'subagents.json',
);
const DELEGATION_DEFAULTS = Object.freeze({
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
const TTL_SWEEP_INTERVAL_MS = 60_000;
const SIGTERM_GRACE_MS = 5_000;
const STOP_GRACE_MS = 2_000;

// ─── Helpers ──────────────────────────────────────────────

// ─── File logger ──────────────────────────────────────────
// Persist logs to disk so crashes and exit causes survive the process.
// Rotates at 5 MB by renaming to `.1` (single-gen rotation — cheap, no deps).

const LOG_DIR = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb',
);
const LOG_PATH = join(LOG_DIR, 'proxy.log');
const LOG_MAX_BYTES = 5 * 1024 * 1024;

try { mkdirSync(LOG_DIR, { recursive: true }); } catch { /* ignore */ }

function writeLogLine(line) {
  try {
    const st = statSync(LOG_PATH);
    if (st.size > LOG_MAX_BYTES) {
      try { renameSync(LOG_PATH, LOG_PATH + '.1'); } catch { /* ignore */ }
    }
  } catch { /* file may not exist yet */ }
  try { appendFileSync(LOG_PATH, line); } catch { /* disk full, readonly fs, etc. */ }
}

function log(msg) {
  const line = `[${new Date().toISOString()}] [pid=${process.pid}] ${msg}\n`;
  try { process.stderr.write(`[AWB] ${msg}\n`); } catch { /* ignore */ }
  writeLogLine(line);
}

// ─── Crash / exit instrumentation ─────────────────────────
// Claude CLI keeps MCP servers on stdio pipes; if anything pushes this process
// toward exit we want the cause recorded. `exit` is sync-only, so the final
// line is written via appendFileSync. SIGPIPE on stdout is handled explicitly
// because an unhandled EPIPE kills Node by default.

process.on('uncaughtException', (err) => {
  log(`Uncaught error: ${err?.stack || err?.message || err}`);
});
process.on('unhandledRejection', (err) => {
  log(`Unhandled rejection: ${err?.stack || err?.message || err}`);
});
process.on('exit', (code) => {
  writeLogLine(`[${new Date().toISOString()}] [pid=${process.pid}] EXIT code=${code}\n`);
});
for (const sig of ['SIGTERM', 'SIGINT', 'SIGHUP', 'SIGPIPE']) {
  process.on(sig, () => {
    log(`Received ${sig}`);
    if (sig !== 'SIGPIPE') process.exit(0);
  });
}
process.stdout.on('error', (err) => {
  log(`stdout error: code=${err?.code} msg=${err?.message} stack=${err?.stack?.split('\n')[1] || ''}`);
  // EPIPE usually means Claude CLI closed its read end — no point staying up.
  if (err?.code === 'EPIPE') process.exit(0);
});
process.stderr.on('error', () => { /* swallow; stderr loss is non-fatal */ });

// DIAG v0.6.9: trace stdin lifecycle — fires BEFORE rl.on('close') so we can see
// whether Claude CLI sent any final line or just dropped the pipe silently.
process.stdin.on('end', () => log('[DIAG] stdin end event (EOF from Claude CLI)'));
process.stdin.on('error', (err) => log(`[DIAG] stdin error: code=${err?.code} msg=${err?.message}`));
process.stdin.on('close', () => log('[DIAG] stdin close event'));

function send(obj) {
  const payload = JSON.stringify(obj) + '\n';
  // DIAG v0.6.9: record every outbound write so we can correlate with stdin close.
  // method + id/params-type + byte length; truncate content to keep log small.
  try {
    const method = obj?.method || (obj?.error ? 'error' : obj?.result ? 'result' : '?');
    const metaType = obj?.params?.meta?.type ?? '';
    log(`[DIAG] stdout.write method=${method} metaType=${metaType} bytes=${Buffer.byteLength(payload)}`);
  } catch { /* ignore diag failure */ }
  const ok = process.stdout.write(payload);
  if (!ok) log('[DIAG] stdout.write returned false (backpressure)');
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
    const raw = JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
    // Normalize delegation section — merge user values over defaults, preserving backward
    // compat when the section is absent (existing users see no behavior change in proxy.mjs
    // unless Plan 04-03 consumers go live).
    raw.delegation = { ...DELEGATION_DEFAULTS, ...(raw.delegation || {}) };
    return raw;
  } catch {
    return null;
  }
}

/**
 * Load cached agent identity from ~/.claude/channels/awb/agent.json.
 * Written by /ai-workflow-board:setup. Used by PresenceHeartbeat to know
 * which agent_id to ping. Returns the parsed object (even if agent_id is null)
 * or null if the file is missing/unparseable.
 */
function loadAgentInfo() {
  if (!existsSync(AGENT_PATH)) return null;
  try {
    const raw = JSON.parse(readFileSync(AGENT_PATH, 'utf8'));
    return raw && typeof raw === 'object' ? raw : null;
  } catch {
    return null;
  }
}

/**
 * Resolve agent_id via MCP whoami tool call if agent.json exists but has null agent_id.
 * Writes the resolved UUID back to agent.json so subsequent proxy restarts skip this step.
 */
async function resolveAgentId(config) {
  const info = loadAgentInfo();
  if (!info) return null; // no agent.json at all
  if (typeof info.agent_id === 'string' && info.agent_id) return info.agent_id; // already resolved

  log('agent_id is null — resolving via MCP whoami...');
  const base = config.url.replace(/\/$/, '');
  const url = `${base}/mcp`;
  const headers = {
    Authorization: `Bearer ${config.apiKey}`,
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
  };

  try {
    // Step 1: initialize
    const initResp = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        jsonrpc: '2.0', id: 1, method: 'initialize',
        params: {
          protocolVersion: '2025-03-26',
          capabilities: { experimental: { 'awb/schemaVersion': { version: 2 } } },
          clientInfo: { name: 'awb-agent-resolve', version: '1.0.0' },
        },
      }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!initResp.ok) throw new Error(`initialize HTTP ${initResp.status}`);
    const sid = initResp.headers.get('mcp-session-id');
    if (!sid) throw new Error('initialize did not return Mcp-Session-Id');
    await initResp.text().catch(() => null);

    const sessionHeaders = { ...headers, 'Mcp-Session-Id': sid };

    // Step 2: notifications/initialized
    await fetch(url, {
      method: 'POST', headers: sessionHeaders,
      body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    }).then((r) => r.text().catch(() => null));

    // Step 3: tools/call whoami
    const whoamiResp = await fetch(url, {
      method: 'POST', headers: sessionHeaders,
      body: JSON.stringify({
        jsonrpc: '2.0', id: 2, method: 'tools/call',
        params: { name: 'whoami', arguments: {} },
      }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!whoamiResp.ok) throw new Error(`whoami HTTP ${whoamiResp.status}`);
    const whoamiBody = await whoamiResp.json();

    // Step 4: DELETE session
    fetch(url, { method: 'DELETE', headers: sessionHeaders, signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) })
      .then((r) => r.text().catch(() => null)).catch(() => {});

    // Extract agent_id from whoami response
    const content = whoamiBody?.result?.content;
    if (!Array.isArray(content) || !content[0]?.text) throw new Error('unexpected whoami response shape');
    const parsed = JSON.parse(content[0].text);
    const agentId = parsed?.agent_id;
    if (!agentId || typeof agentId !== 'string') throw new Error(`whoami returned no agent_id: ${content[0].text}`);

    // Write back to agent.json
    info.agent_id = agentId;
    info._note = `agent_id resolved automatically by proxy at ${new Date().toISOString()}`;
    await fsp.writeFile(AGENT_PATH, JSON.stringify(info, null, 2) + '\n', 'utf8');
    log(`agent_id resolved: ${agentId.slice(0, 8)}...`);
    return agentId;
  } catch (err) {
    log(`agent_id resolve failed: ${err.message}`);
    return null;
  }
}

// ─── Delegation helpers (Phase 4 D-60 / D-61) ─────────────

/**
 * Fetch a fresh ticket with comments from AWB REST (D-60).
 * Used by #handleTrigger to compose the subagent task prompt.
 * Returns null on any failure; caller falls back to embedded trigger payload fields.
 */
async function fetchTicketContext(config, ticketId) {
  if (!ticketId) return null;
  try {
    const url = `${config.url.replace(/\/$/, '')}/api/tickets/${encodeURIComponent(ticketId)}`;
    const resp = await fetch(url, {
      headers: {
        Authorization: `Bearer ${config.apiKey}`,
        Accept: 'application/json',
      },
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!resp.ok) {
      log(`Ticket fetch failed: ${resp.status} ${resp.statusText} (ticket=${ticketId})`);
      return null;
    }
    return await resp.json();
  } catch (err) {
    log(`Ticket fetch error: ${err.message} (ticket=${ticketId})`);
    return null;
  }
}

/**
 * Compose the task text for a trigger subagent. Pure function.
 * - `ticket` may be null (fetch failed) — falls back to IDs + embedded prompts only.
 * - Caps recent comments to last 5 to bound prompt size.
 * - role_prompt is injected separately via --append-system-prompt (NOT here).
 * Produces the POSITIONAL prompt arg passed as the last argv to `claude --print`.
 */
function composeTriggerPrompt(ticket, rolePrompt, ticketPrompt, fallbackTicketId) {
  const lines = [];
  lines.push('You are an AWB subagent responding to an assigned trigger.');
  lines.push('');
  if (ticket) {
    lines.push(`Ticket ID: ${ticket.id}`);
    if (ticket.title) lines.push(`Title: ${ticket.title}`);
    if (ticket.description) {
      lines.push('');
      lines.push('Description:');
      lines.push(ticket.description);
    }
    if (ticketPrompt) {
      lines.push('');
      lines.push('Ticket instructions:');
      lines.push(ticketPrompt);
    } else if (ticket.prompt_text) {
      lines.push('');
      lines.push('Ticket instructions:');
      lines.push(ticket.prompt_text);
    }
    const comments = Array.isArray(ticket.comments) ? ticket.comments.slice(-5) : [];
    if (comments.length > 0) {
      lines.push('');
      lines.push('Recent comments (newest last):');
      for (const c of comments) {
        const who = c.author_name || c.agent_name || 'unknown';
        const when = c.created_at || '';
        const body = (c.body || c.content || '').slice(0, 2000);
        lines.push(`- [${when}] ${who}: ${body}`);
      }
    }
  } else {
    lines.push(`Ticket ID: ${fallbackTicketId || 'unknown'}`);
    lines.push('(Fresh ticket context fetch failed — using embedded trigger payload only.)');
    if (ticketPrompt) {
      lines.push('');
      lines.push('Ticket instructions:');
      lines.push(ticketPrompt);
    }
  }
  lines.push('');
  lines.push('Instructions:');
  lines.push('- Use AWB MCP tools (mcp__awb__*) to perform the work.');
  lines.push('- Claim the ticket if not already claimed.');
  lines.push('- Leave a comment on the ticket when done describing what you did.');
  lines.push('- Move the ticket to the next column when the work is complete.');
  return lines.join('\n');
}

/**
 * Compose the task text for a chat subagent. Pure function.
 * role_prompt is injected via --append-system-prompt (not here).
 * `history` is the last N messages (chronological); `newMessage` is the user's latest message.
 */
function composeChatPrompt(rolePrompt, history, newMessage) {
  const lines = [];
  lines.push('You are an AWB chat subagent responding to a user message in a live conversation.');
  lines.push('');
  if (Array.isArray(history) && history.length > 0) {
    lines.push('Conversation history (oldest first):');
    for (const h of history.slice(-20)) {
      const who = h.sender_type === 'agent' ? 'Agent' : 'User';
      const when = h.created_at || '';
      const content = (h.content || '').slice(0, 2000);
      lines.push(`- [${when}] ${who}: ${content}`);
    }
    lines.push('');
  }
  lines.push('Latest user message:');
  lines.push(newMessage || '');
  lines.push('');
  lines.push('Instructions:');
  lines.push('- Compose a helpful reply using your knowledge and the conversation context.');
  lines.push('- Reply ONLY via the mcp__awb__send_chat_room_message MCP tool (pass the room_id from the chat request context).');
  lines.push('- Do NOT print your reply to stdout — it must go through send_chat_room_message so the user sees it in the web UI.');
  return lines.join('\n');
}

/**
 * Fetch recent chat room messages from AWB REST API.
 * Returns array of {sender_type, sender_name, content, created_at} or empty on failure.
 */
async function fetchChatRoomHistory(config, roomId, limit = 20) {
  if (!roomId) return [];
  try {
    const url = `${config.url.replace(/\/$/, '')}/api/agent/chat-rooms/${encodeURIComponent(roomId)}/messages?limit=${limit}`;
    const resp = await fetch(url, {
      headers: {
        'X-Agent-Key': config.apiKey,
      },
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!resp.ok) {
      log(`Chat room history fetch failed: ${resp.status} (room=${roomId})`);
      return [];
    }
    const data = await resp.json();
    return Array.isArray(data) ? data : (data.messages || []);
  } catch (err) {
    log(`Chat room history fetch error: ${err.message} (room=${roomId})`);
    return [];
  }
}

/**
 * Compose the task text for a chat room message subagent. Pure function.
 * `history` is recent messages (chronological); `newMessage` is the incoming message.
 * role_prompt is injected via --append-system-prompt (not here).
 */
function composeChatRoomPrompt(roomId, history, newMessage) {
  const lines = [];
  lines.push('You are an AWB chat subagent responding to a user message in a chat room.');
  lines.push('');
  lines.push(`Room ID: ${roomId}`);
  lines.push('');
  if (Array.isArray(history) && history.length > 0) {
    lines.push('Conversation history (oldest first):');
    for (const h of history.slice(-20)) {
      const who = h.sender_type === 'agent' ? 'Agent' : 'User';
      const name = h.sender_name || h.sender_id || 'unknown';
      const when = h.created_at || '';
      const content = (h.content || '').slice(0, 2000);
      lines.push(`- [${when}] ${who} (${name}): ${content}`);
    }
    lines.push('');
  }
  lines.push('Latest user message:');
  lines.push(newMessage.content || '');
  lines.push(`From: ${newMessage.sender_name || newMessage.sender_id || 'unknown'}`);
  lines.push('');
  lines.push('Instructions:');
  lines.push('- Compose a helpful reply using your knowledge and the conversation context.');
  lines.push(`- Reply ONLY via the mcp__awb__send_chat_room_message MCP tool (room_id: "${roomId}").`);
  lines.push('- Do NOT print your reply to stdout — it must go through send_chat_room_message so the user sees it in the web UI.');
  return lines.join('\n');
}

// ─── Presence Heartbeat (Phase 3 D-52 / D-53) ─────────────

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
class PresenceHeartbeat {
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

// ─── SSE Event Stream ─────────────────────────────────────

/**
 * Connect to AWB's SSE /api/events/stream and forward ticket events
 * as claude/channel notifications. Reconnects with exponential backoff.
 *
 * AWB SSE event types (from events.controller.ts):
 *   - board_update:  ticket/comment CRUD (entity_type, action, field_changed, actor_name)
 *   - agent_trigger: trigger assigned to agent (role, trigger_id, agent_id)
 *   - agent_typing:  typing indicator (ignored by proxy)
 */
class EventStream {
  #url;
  #retryDelay = RECONNECT_INITIAL_MS;
  #abortController = null;
  #stopped = false;
  #config;                    // Phase 4 Plan 04-03 — delegation branch decisions
  #subagentManager;           // Phase 4 Plan 04-03 — spawn target (may be null)
  #chatSessionManager;        // v0.7.0 — persistent per-room chat sessions (may be null)
  #ticketSessionManager;      // v0.8.0 — persistent per-ticket sessions (may be null)

  constructor(config, subagentManager = null, chatSessionManager = null, ticketSessionManager = null) {
    this.#url = `${config.url.replace(/\/$/, '')}/api/events/stream?token=${encodeURIComponent(config.apiKey)}`;
    this.#config = config;
    this.#subagentManager = subagentManager;
    this.#chatSessionManager = chatSessionManager;
    this.#ticketSessionManager = ticketSessionManager;
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
          } else if (data && eventType === 'board_update') {
            this.#handleBoardUpdate(data);
          } else if (data && eventType === 'chat_request') {
            this.#handleChatRequest(data);
          } else if (data && eventType === 'chat_room_message') {
            this.#handleChatRoomMessage(data);
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

  async #handleTrigger(raw) {
    let ev;
    try {
      ev = JSON.parse(raw);
    } catch (err) {
      log(`Failed to parse trigger: ${err.message}`);
      return;
    }

    // Phase 1 flatten-on-emit asymmetry: agent_trigger reads TOP-LEVEL fields
    // (ev.role_prompt, ev.ticket_prompt, ev.ticket_id, ev.field_changed, ev.actor_name).
    // In contrast, chat_request is envelope-native and reads ev.payload.* — see
    // #handleChatRequest for the other side of this asymmetry.

    // D-59: delegation branch — check config flag AND runtime capacity before choosing path
    // Default true when config key absent: undefined !== false evaluates to true.
    const delegationEnabled = this.#config?.delegation?.enabled !== false;
    const persistentTicket = this.#config?.delegation?.persistentTicketSessions !== false;

    // v0.8.0: prefer persistent per-ticket session path. The session stays alive
    // across multiple triggers and board_update events for the same ticket, reusing
    // KV cache and maintaining full context.
    if (delegationEnabled && persistentTicket && this.#ticketSessionManager) {
      try {
        const ticket = await fetchTicketContext(this.#config, ev.ticket_id);
        const rolePrompt = ev.role_prompt || '';
        const ticketPrompt = ev.ticket_prompt || '';

        const result = await this.#ticketSessionManager.dispatchTrigger({
          ticketId: ev.ticket_id || '',
          triggerId: ev.field_changed || '',
          agentId: ev.actor_name || '',
          rolePrompt,
          ticketPrompt,
          ticket,
        });

        if (result.dispatched) {
          log(`Trigger dispatched to ticket session: ticket=${ev.ticket_id} pid=${result.pid}${result.firstTurn ? ' (new session)' : ''}`);
          return;
        }
        if (result.reason === 'duplicate_trigger') {
          log(`Trigger deduped: ticket=${ev.ticket_id} trigger=${ev.field_changed || ''}`);
          return;
        }
        log(`Ticket session dispatch declined (${result.reason}), falling back to one-shot subagent`);
      } catch (err) {
        log(`Ticket session path failed: ${err.message}, falling back to one-shot subagent`);
      }
    }

    // Fallback: one-shot subagent (legacy Phase 4 behavior)
    const canDelegate = delegationEnabled && this.#subagentManager && this.#subagentManager.canSpawn();

    if (canDelegate) {
      try {
        const ticket = await fetchTicketContext(this.#config, ev.ticket_id);
        const rolePrompt = ev.role_prompt || '';
        const ticketPrompt = ev.ticket_prompt || '';
        const taskText = composeTriggerPrompt(ticket, rolePrompt, ticketPrompt, ev.ticket_id);

        const result = await this.#subagentManager.spawn({
          kind: 'trigger',
          taskText,
          rolePrompt,
          triggerId: ev.field_changed || '',
          ticketId: ev.ticket_id || '',
          agentId: ev.actor_name || '',
        });

        if (result.spawned) {
          log(`Trigger dispatched to subagent: ticket=${ev.ticket_id} pid=${result.pid}`);
          return;
        }
        log(`Subagent spawn declined (${result.reason}), falling back to main session forward`);
      } catch (err) {
        log(`Delegation path failed: ${err.message}, falling back to main session forward`);
      }
    }

    // ── Legacy Phase 1 pass-through path ─────────────────────────────
    // Runs when: delegation disabled OR subagentManager missing OR canSpawn false
    // OR spawn declined OR delegation path threw. Preserves exact Phase 1 behavior
    // for users who set delegation.enabled: false (or who have no delegation config).
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
    log(`Trigger forwarded (legacy path): ticket=${ev.ticket_id} role=${ev.action}`);
  }

  async #handleChatRequest(raw) {
    let ev;
    try {
      ev = JSON.parse(raw);
    } catch (err) {
      log(`Failed to parse chat_request: ${err.message}`);
      return;
    }

    // ASYMMETRY NOTICE: chat_request ships envelope-native (Plan 04-01), so fields
    // live under ev.payload.*  —  NOT at the top level like agent_trigger. This is
    // the counterpart to the flatten-on-emit path in #handleTrigger above. See
    // 01-02-SUMMARY.md:203 and 04-01-SUMMARY.md for the rationale.
    const payload = ev.payload || {};
    // Default true when config key absent: undefined !== false evaluates to true.
    const delegationEnabled = this.#config?.delegation?.enabled !== false;
    const persistentChat = this.#config?.delegation?.persistentChatSessions !== false;

    // v0.7.0: prefer the persistent per-room session path when we have a room_id.
    // Falls back to the legacy per-message spawn when the flag is disabled OR the
    // event carries no room_id (shouldn't happen after the server-side fix, but the
    // older SubagentManager path still covers the degenerate case).
    if (delegationEnabled && persistentChat && this.#chatSessionManager && payload.room_id) {
      try {
        const result = await this.#chatSessionManager.dispatch({
          roomId: payload.room_id,
          senderId: payload.user_id || '',
          senderName: '',
          createdAt: ev.timestamp || '',
          content: payload.new_message || '',
          rolePrompt: payload.role_prompt || '',
        });
        if (result.dispatched) {
          log(`Chat request dispatched to session: room=${payload.room_id} pid=${result.pid}${result.firstTurn ? ' (new session)' : ''}`);
          return;
        }
        if (result.reason === 'duplicate_chat') {
          log(`Chat request deduped: room=${payload.room_id} user=${payload.user_id} ts=${ev.timestamp || ''}`);
          return;
        }
        log(`Chat session dispatch declined (${result.reason}), falling back to legacy path`);
      } catch (err) {
        log(`Chat session path failed: ${err.message}, falling back to legacy path`);
      }
    }

    const canDelegate = delegationEnabled && this.#subagentManager && this.#subagentManager.canSpawn();

    if (canDelegate) {
      const rolePrompt = payload.role_prompt || '';
      const history = Array.isArray(payload.history) ? payload.history : [];
      const newMessage = payload.new_message || '';
      const taskText = composeChatPrompt(rolePrompt, history, newMessage);

      try {
        const result = await this.#subagentManager.spawn({
          kind: 'chat',
          taskText,
          rolePrompt,
          // v0.6.11: unified dedup key so chat_request and chat_room_message — both emitted
          // for the same user message (same savedMsg.created_at) — collide on spawn().
          // Previously each handler used a different key format and double-spawned.
          chatRequestId: payload.user_id
            ? `msg:${payload.user_id}:${ev.timestamp || ''}`
            : undefined,
          ticketId: payload.ticket_id || '',
          agentId: payload.agent_id || '',
        });

        if (result.spawned) {
          log(`Chat request dispatched to subagent: agent=${payload.agent_id} pid=${result.pid}`);
          return;
        }
        log(`Chat subagent spawn declined (${result.reason}), falling back to main session forward`);
      } catch (err) {
        log(`Chat delegation path failed: ${err.message}, falling back to main session forward`);
      }
    }

    // ── Fallback: notify main session ───────────────────────────────
    // Runs when: delegation disabled OR subagentManager missing OR canSpawn false
    // OR spawn declined OR delegation path threw.
    sendChannelEvent(
      `[AWB Chat Request] agent=${payload.agent_id} user=${payload.user_id} room=${payload.room_id || ''}`,
      {
        type: 'chat_request',
        agent_id: payload.agent_id || '',
        user_id: payload.user_id || '',
        room_id: payload.room_id || '',
        ticket_id: payload.ticket_id || '',
        new_message: payload.new_message || '',
        timestamp: ev.timestamp || new Date().toISOString(),
      },
    );
    log(`Chat request forwarded (fallback path): agent=${payload.agent_id} user=${payload.user_id}`);
  }

  #handleBoardUpdate(raw) {
    try {
      const ev = JSON.parse(raw);
      // entity_type: 'ticket' | 'comment' | 'child_ticket' etc.
      // action: 'created' | 'updated' | 'moved' | 'deleted' | 'status_changed'

      // v0.8.0: if a persistent ticket session exists for this ticket, forward the
      // update there so the subagent can react in context. Don't send to main session.
      if (this.#ticketSessionManager && ev.ticket_id) {
        const forwarded = this.#ticketSessionManager.forwardBoardUpdate(ev.ticket_id, ev);
        if (forwarded) {
          log(`Board update forwarded to ticket session: ticket=${ev.ticket_id} ${ev.entity_type}.${ev.action}`);
          return;
        }
      }

      // No live ticket session — forward to main session as before
      const label = ev.entity_type === 'comment' ? 'Comment' : 'Update';
      sendChannelEvent(
        `[AWB ${label}] ticket=${ev.ticket_id} ${ev.entity_type}.${ev.action}${ev.field_changed ? ` field=${ev.field_changed}` : ''} by=${ev.actor_name}`,
        {
          type: 'board_update',
          ticket_id: ev.ticket_id || '',
          entity_type: ev.entity_type || '',
          action: ev.action || '',
          field_changed: ev.field_changed || '',
          actor_name: ev.actor_name || '',
          timestamp: ev.timestamp || new Date().toISOString(),
        },
      );
      log(`Board update forwarded: ticket=${ev.ticket_id} ${ev.entity_type}.${ev.action}`);
    } catch (err) {
      log(`Failed to parse board_update: ${err.message}`);
    }
  }

  async #setChatRoomTyping(roomId, isTyping) {
    try {
      const agentInfo = loadAgentInfo();
      const url = `${this.#config.url.replace(/\/$/, '')}/api/agent/chat-rooms/${encodeURIComponent(roomId)}/typing`;
      await fetch(url, {
        method: 'POST',
        headers: {
          'X-Agent-Key': this.#config.apiKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          agent_id: agentInfo?.agent_id || '',
          agent_name: agentInfo?.name || agentInfo?.agent_name || 'Agent',
          is_typing: isTyping,
        }),
        signal: AbortSignal.timeout(5000),
      });
    } catch (err) {
      log(`setChatRoomTyping failed: ${err.message}`);
    }
  }

  async #handleChatRoomMessage(raw) {
    let ev;
    try {
      ev = JSON.parse(raw);
    } catch (err) {
      log(`Failed to parse chat_room_message: ${err.message}`);
      return;
    }

    const p = ev.payload || ev;

    // Always record the message into the per-room history ring — gives warm
    // context to any late-starting session, and keeps agent replies in view.
    this.#chatSessionManager?.recordRoomMessage(p);

    // Skip messages sent by agents to avoid self-reply loops
    if (p.sender_type === 'agent') {
      log(`Chat room message from agent (${p.sender_name || p.sender_id}) — skipping delegation`);
      return;
    }

    // Signal typing=true before dispatching — client auto-clears on message arrival
    if (p.room_id) {
      await this.#setChatRoomTyping(p.room_id, true);
    }

    const delegationEnabled = this.#config?.delegation?.enabled !== false;
    const persistentChat = this.#config?.delegation?.persistentChatSessions !== false;

    if (delegationEnabled && persistentChat && this.#chatSessionManager && p.room_id) {
      try {
        const result = await this.#chatSessionManager.dispatch({
          roomId: p.room_id,
          senderId: p.sender_id || '',
          senderName: p.sender_name || '',
          createdAt: p.created_at || '',
          content: p.content || '',
          rolePrompt: p.role_prompt || '',
        });
        if (result.dispatched) {
          log(`Chat room message dispatched to session: room=${p.room_id} pid=${result.pid}${result.firstTurn ? ' (new session)' : ''}`);
          return;
        }
        if (result.reason === 'duplicate_chat') {
          log(`Chat room message deduped: room=${p.room_id} sender=${p.sender_id} ts=${p.created_at || ''}`);
          return;
        }
        log(`Chat room session dispatch declined (${result.reason}), falling back to legacy path`);
      } catch (err) {
        log(`Chat room session path failed: ${err.message}, falling back to legacy path`);
      }
    }

    const canDelegate = delegationEnabled && this.#subagentManager && this.#subagentManager.canSpawn();

    if (canDelegate) {
      try {
        const history = await fetchChatRoomHistory(this.#config, p.room_id);
        const rolePrompt = p.role_prompt || '';
        const taskText = composeChatRoomPrompt(p.room_id, history, {
          content: p.content || '',
          sender_name: p.sender_name || '',
          sender_id: p.sender_id || '',
        });

        const result = await this.#subagentManager.spawn({
          kind: 'chat',
          taskText,
          rolePrompt,
          chatRequestId: `msg:${p.sender_id}:${p.created_at || ''}`,
          ticketId: '',
          agentId: '',
        });

        if (result.spawned) {
          log(`Chat room message dispatched to subagent: room=${p.room_id} pid=${result.pid}`);
          return;
        }
        log(`Chat room subagent spawn declined (${result.reason}), falling back to main session forward`);
      } catch (err) {
        log(`Chat room delegation path failed: ${err.message}, falling back to main session forward`);
      }
    }

    // ── Fallback: notify main session ───────────────────────────────
    sendChannelEvent(
      `[AWB Chat] room=${p.room_id} from=${p.sender_name || p.sender_id} "${(p.content || '').slice(0, 80)}"`,
      {
        type: 'chat_room_message',
        room_id: p.room_id || '',
        sender_type: p.sender_type || '',
        sender_id: p.sender_id || '',
        sender_name: p.sender_name || '',
        content: p.content || '',
        timestamp: p.created_at || new Date().toISOString(),
      },
    );
    log(`Chat room message forwarded (fallback path): room=${p.room_id} sender=${p.sender_name || p.sender_id}`);
  }

  #scheduleReconnect() {
    if (this.#stopped) return;
    setTimeout(() => this.#connect(), this.#retryDelay);
    this.#retryDelay = Math.min(this.#retryDelay * 1.5, RECONNECT_MAX_MS);
  }

  // Test-only accessor — only exposed when AWB_TEST_MODE is set. Lets Plan 04-04
  // integration tests invoke the private handlers without opening a real SSE stream.
  _testDispatchTrigger(raw) { return this.#handleTrigger(raw); }
  _testDispatchChatRequest(raw) { return this.#handleChatRequest(raw); }
  _testDispatchChatRoomMessage(raw) { return this.#handleChatRoomMessage(raw); }
}

// ─── Subagent Manager (Phase 4 D-55..D-75) ────────────────
/**
 * Owns the lifecycle of Claude CLI subagent child processes.
 *
 * Consumers (Plan 04-03 #handleTrigger and #handleChatRequest) call spawn(spec)
 * with a composed task prompt; the manager handles MCP config file writing, process
 * spawning, PID tracking, stdout/stderr capture, TTL enforcement, persistence, and
 * exit-driven cleanup + completion notification.
 *
 * This class is behaviorally inert until Plan 04-03 wires consumers — Plan 04-02
 * ships it with no caller, only a node --test suite that injects a fake claudeBin.
 *
 * Pitfalls guarded:
 *  - Pitfall 1: MCP config file uses {"mcpServers": {...}} wrapper shape
 *  - Pitfall 3: SIGTERM/SIGINT/exit handlers clean up children on proxy shutdown
 *  - Pitfall 4: Concurrency cap reserves slot synchronously before async spawn
 *  - Pitfall 6: stdio: ['ignore', 'pipe', 'pipe'] closes child stdin
 *  - Pitfall 7: #persist() strips process_handle before JSON.stringify
 */
class SubagentManager {
  #map = new Map();              // pid → SubagentRecord, AND reservationId → {kind: 'reservation', ...}
  #config;
  #sweepTimer = null;
  #reservationCounter = 0;
  #persistPath;
  #pidDir;
  #initialized = false;

  constructor(config) {
    this.#config = config;
    this.#persistPath = SUBAGENTS_PERSIST_PATH;
    this.#pidDir = SUBAGENTS_BASE_DIR;
  }

  /** Idempotent init: create dirs, reconcile persisted records, start TTL sweep. */
  async init() {
    if (this.#initialized) return;
    this.#initialized = true;
    try {
      // mode 0700 so only the user can read config files containing the Bearer token
      await fsp.mkdir(this.#pidDir, { recursive: true, mode: 0o700 });
    } catch (err) {
      log(`SubagentManager: mkdir failed: ${err.message}`);
    }
    await this.#reconcileOnStart();
    this.#sweepTimer = setInterval(() => this.#sweep(), TTL_SWEEP_INTERVAL_MS);
    // Prevent the sweep timer from keeping the event loop alive past proxy shutdown
    if (typeof this.#sweepTimer.unref === 'function') this.#sweepTimer.unref();
    log(`SubagentManager initialized (pidDir=${this.#pidDir}, cap=${this.#config.delegation.maxConcurrent}, ttl=${this.#config.delegation.ttlMinutes}min)`);
  }

  /** Count non-reservation records. True if room exists under maxConcurrent. */
  canSpawn() {
    const active = this.#activeCount();
    return active < this.#config.delegation.maxConcurrent;
  }

  #activeCount() {
    let n = 0;
    for (const rec of this.#map.values()) {
      if (rec.kind !== 'reservation') n++;
      else n++; // reservations also consume a slot to close the check-then-act race
    }
    return n;
  }

  /**
   * Spawn a subagent. spec = {
   *   kind: 'trigger' | 'chat',
   *   taskText: string,              // positional prompt passed as the last argv
   *   rolePrompt: string,            // injected via --append-system-prompt
   *   triggerId?: string,            // dedup key for trigger kind
   *   chatRequestId?: string,        // dedup key for chat kind
   *   ticketId?: string,
   *   agentId?: string,
   * }
   * Returns { spawned: boolean, pid?: number, reason?: string }
   */
  async spawn(spec) {
    // Dedup by trigger_id (Pitfall 5: SSE reconnect duplicate trigger)
    if (spec.triggerId) {
      for (const rec of this.#map.values()) {
        if (rec.kind !== 'reservation' && rec.trigger_id === spec.triggerId) {
          return { spawned: false, reason: 'duplicate_trigger' };
        }
      }
    }

    // Strategy B fix: dedup by chatRequestId — prevents double-spawn when chat_request and
    // chat_room_message both arrive for the same user message and both attempt delegation.
    // Uses a separate check from triggerId to avoid namespace collisions between the two dedup keys.
    if (spec.chatRequestId) {
      for (const rec of this.#map.values()) {
        if (rec.kind !== 'reservation' && rec.chat_request_id === spec.chatRequestId) {
          return { spawned: false, reason: 'duplicate_chat' };
        }
      }
    }

    // Concurrency cap check — synchronous reservation closes Pitfall 4 race
    if (!this.canSpawn()) {
      return { spawned: false, reason: 'cap_reached' };
    }
    const reservationId = -(++this.#reservationCounter);
    this.#map.set(reservationId, { kind: 'reservation', started_at: Date.now() });

    let configPath = null;
    try {
      // Write per-subagent MCP config file. Pitfall 7 (v0.6.11): the child Claude CLI
      // reads --mcp-config lazily after spawn returns. We must NOT rename the file
      // after spawn — the child will fail with "MCP config file not found". Keep the
      // file at its original path for the child's entire lifetime; cleanup on exit.
      configPath = join(
        this.#pidDir,
        `cfg-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
      );
      await fsp.mkdir(dirname(configPath), { recursive: true, mode: 0o700 });

      // CRITICAL (Pitfall 1): wrapper shape. Bare {serverName: {...}} is REJECTED.
      const mcpConfig = {
        mcpServers: {
          awb: {
            type: 'http',
            url: `${this.#config.url.replace(/\/$/, '')}/mcp`,
            headers: { Authorization: `Bearer ${this.#config.apiKey}` },
          },
        },
      };
      await fsp.writeFile(configPath, JSON.stringify(mcpConfig), { mode: 0o600 });

      // Spawn. Pitfall 6: stdio[0]='ignore' closes child stdin.
      // All argv values are separate array elements — never shell-interpolated.
      const args = [
        '--print',
        '--output-format', 'json',
        '--mcp-config', configPath,
        '--strict-mcp-config',
        '--allowedTools', 'mcp__awb__*',
        '--append-system-prompt', spec.rolePrompt || '',
        '--dangerously-skip-permissions',
        spec.taskText,
      ];
      // detached:true puts the child in its own process group so signals aimed
      // at the proxy's pgrp (SIGHUP from a closing terminal, SIGINT from Ctrl+C
      // in the parent Claude CLI) don't cascade and kill in-flight work.
      // TTL sweep and explicit SIGTERM/SIGKILL by pid still work.
      const child = spawn(this.#config.delegation.claudeBin, args, {
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: true,
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
      });
      child.unref();

      const pid = child.pid;
      if (!pid) {
        await fsp.unlink(configPath).catch(() => {});
        this.#map.delete(reservationId);
        return { spawned: false, reason: 'spawn_failed' };
      }

      const record = {
        pid,
        kind: spec.kind,
        trigger_id: spec.triggerId || null,
        chat_request_id: spec.chatRequestId || null,
        ticket_id: spec.ticketId || null,
        agent_id: spec.agentId || null,
        started_at: Date.now(),
        expected_completion_at: Date.now() + this.#config.delegation.ttlMinutes * 60_000,
        config_path: configPath,
        process_handle: child,
      };
      this.#map.delete(reservationId);
      this.#map.set(pid, record);
      this.#persist();

      this.#wireExitHandler(child, pid);
      this.#wireStdioCapture(child, pid);

      log(`Subagent spawned: pid=${pid} kind=${spec.kind} ticket=${spec.ticketId || '-'}`);
      return { spawned: true, pid };
    } catch (err) {
      this.#map.delete(reservationId);
      if (configPath) {
        await fsp.unlink(configPath).catch(() => {});
      }
      log(`Subagent spawn error: ${err.message}`);
      return { spawned: false, reason: 'exception' };
    }
  }

  #wireExitHandler(child, pid) {
    child.once('exit', async (code, signal) => {
      const record = this.#map.get(pid);
      if (!record) return; // already cleaned by sweep
      const durationSec = Math.round((Date.now() - record.started_at) / 1000);
      this.#map.delete(pid);
      this.#persist();
      try {
        await fsp.unlink(record.config_path);
      } catch { /* best-effort */ }
      // Plan 04-03 wraps sendChannelEvent() here to notify the main session.
      // Plan 04-02 leaves a log line only; consumers are not yet wired.
      log(`Subagent exit: pid=${pid} kind=${record.kind} code=${code} signal=${signal || '-'} duration=${durationSec}s`);
      // Expose hook for Plan 04-03 test spy — stored on the instance for test visibility
      if (typeof this.onExit === 'function') {
        try { this.onExit({ pid, record, code, signal, durationSec }); } catch { /* ignore */ }
      }
    });
    child.once('error', (err) => {
      log(`Subagent spawn error pid=${pid}: ${err.message}`);
    });
  }

  #wireStdioCapture(child, pid) {
    if (child.stdout) {
      const rlOut = createInterface({ input: child.stdout });
      rlOut.on('line', (line) => log(`[subagent:${pid}] ${line}`));
    }
    if (child.stderr) {
      const rlErr = createInterface({ input: child.stderr });
      rlErr.on('line', (line) => log(`[subagent:${pid}:err] ${line}`));
    }
  }

  #sweep() {
    const now = Date.now();
    for (const [pid, record] of this.#map.entries()) {
      if (record.kind === 'reservation') continue;
      // Existence check — process may have died without our exit handler firing
      try {
        process.kill(pid, 0);
      } catch (err) {
        if (err.code === 'ESRCH' || err.code === 'EPERM') {
          log(`Sweep: pid=${pid} no longer alive, removing record`);
          this.#map.delete(pid);
          fsp.rm(dirname(record.config_path), { recursive: true, force: true }).catch(() => {});
          continue;
        }
      }
      // TTL enforcement
      if (now >= record.expected_completion_at) {
        log(`Sweep: pid=${pid} exceeded TTL, sending SIGTERM`);
        try { process.kill(pid, 'SIGTERM'); } catch { /* already dead */ }
        setTimeout(() => {
          try {
            process.kill(pid, 0);
            log(`Sweep: pid=${pid} still alive after SIGTERM grace, sending SIGKILL`);
            try { process.kill(pid, 'SIGKILL'); } catch { /* ignore */ }
          } catch { /* already exited */ }
        }, SIGTERM_GRACE_MS);
      }
    }
    this.#persist();
  }

  async #reconcileOnStart() {
    let raw;
    try {
      raw = await fsp.readFile(this.#persistPath, 'utf8');
    } catch { return; }
    let persisted;
    try {
      persisted = JSON.parse(raw).pids || [];
    } catch { return; }

    let revived = 0, dropped = 0;
    for (const rec of persisted) {
      if (!rec || !rec.pid) continue;
      try {
        process.kill(rec.pid, 0);
        // Alive — revive record. process_handle is null because we didn't spawn it this session.
        this.#map.set(rec.pid, { ...rec, process_handle: null });
        revived++;
      } catch (err) {
        if (err.code === 'ESRCH' || err.code === 'EPERM') dropped++;
      }
    }
    if (revived || dropped) {
      log(`SubagentManager reconciled: revived=${revived} dropped=${dropped}`);
    }
    this.#persist();
  }

  #persist() {
    // Pitfall 7: strip process_handle (circular ChildProcess refs throw on JSON.stringify)
    const pids = [];
    for (const rec of this.#map.values()) {
      if (rec.kind === 'reservation') continue;
      const { process_handle, ...serializable } = rec;
      pids.push(serializable);
    }
    // Fire-and-forget; log on failure
    fsp.writeFile(this.#persistPath, JSON.stringify({ pids }, null, 2))
      .catch((err) => log(`SubagentManager persist failed: ${err.message}`));
  }

  /** Graceful shutdown: SIGTERM all children, give STOP_GRACE_MS, then SIGKILL survivors. */
  async stop() {
    if (this.#sweepTimer) {
      clearInterval(this.#sweepTimer);
      this.#sweepTimer = null;
    }
    const pids = [];
    for (const [pid, rec] of this.#map.entries()) {
      if (rec.kind === 'reservation') continue;
      pids.push(pid);
      try { process.kill(pid, 'SIGTERM'); } catch { /* dead */ }
    }
    if (pids.length === 0) {
      this.#map.clear();
      return;
    }
    await new Promise((r) => setTimeout(r, STOP_GRACE_MS));
    for (const pid of pids) {
      try { process.kill(pid, 'SIGKILL'); } catch { /* gone */ }
    }
    this.#map.clear();
    try {
      await fsp.writeFile(this.#persistPath, JSON.stringify({ pids: [] }, null, 2));
    } catch { /* best-effort */ }
    log(`SubagentManager stopped (terminated ${pids.length} children)`);
  }

  /** Test-only accessor: snapshot of active records (stripped of process_handle). */
  _snapshot() {
    const out = [];
    for (const rec of this.#map.values()) {
      if (rec.kind === 'reservation') continue;
      const { process_handle, ...serializable } = rec;
      out.push(serializable);
    }
    return out;
  }
}

// ─── Chat Session Manager (v0.7.0 persistent per-room chat subagents) ────
/**
 * Keeps one Claude CLI child alive per chat room so that successive messages
 * reuse the same KV cache instead of paying cold-start + MCP handshake per turn.
 *
 * The child is launched with `--input-format stream-json --output-format stream-json`.
 * First turn text is composed with composeChatRoomPrompt() and seeded with recent
 * room history (from the proxy's in-memory ring buffer, fallback: REST history).
 * Subsequent turns are lean user-turn NDJSON lines written to the child's stdin.
 *
 * Lifecycle bounds keep the memory footprint finite:
 *   - IDLE_TTL (idleMinutes):         no traffic → stdin.end() → child exits
 *   - MAX_TURNS (maxTurnsPerSession): respawn on next message (fresh context)
 *   - CAP (maxConcurrent):            LRU-evict oldest-idle before spawn
 *
 * Dedup: `msg:${sender_id}:${created_at}` collides for chat_request and
 * chat_room_message covering the same savedMessage, so each user turn dispatches
 * exactly once even though two SSE events arrive.
 */
class ChatSessionManager {
  #config;
  #sessions = new Map();         // roomId → session
  #historyRing = new Map();      // roomId → ChatRoomMessagePayload[] (max 30)
  #dedupSet = new Set();
  #dedupQueue = [];              // fifo for bounded dedup
  #DEDUP_MAX = 200;
  #HISTORY_MAX = 30;

  constructor(config) {
    this.#config = config;
  }

  /** Called from SSE reader for every chat_room_message we see — warms the ring. */
  recordRoomMessage(payload) {
    const rid = payload?.room_id;
    if (!rid) return;
    let buf = this.#historyRing.get(rid);
    if (!buf) { buf = []; this.#historyRing.set(rid, buf); }
    buf.push({
      sender_type: payload.sender_type,
      sender_id: payload.sender_id,
      sender_name: payload.sender_name,
      content: payload.content,
      created_at: payload.created_at,
    });
    while (buf.length > this.#HISTORY_MAX) buf.shift();
  }

  /**
   * Dispatch a user turn into the room's live session, spawning one if needed.
   * spec = { roomId, senderId, senderName, createdAt, content, rolePrompt }
   * Returns { dispatched: boolean, pid?: number, reason?: string, firstTurn?: boolean }.
   */
  async dispatch(spec) {
    if (!spec.roomId) return { dispatched: false, reason: 'no_room' };

    // Dedup: same user message may arrive via both chat_request and chat_room_message
    const dedupKey = `msg:${spec.senderId || ''}:${spec.createdAt || ''}`;
    if (this.#dedupSet.has(dedupKey)) {
      return { dispatched: false, reason: 'duplicate_chat' };
    }
    this.#rememberDedup(dedupKey);

    let sess = this.#sessions.get(spec.roomId);

    if (sess) {
      // Existing live session — just stream another user turn into stdin.
      this.#writeTurn(sess, spec.content || '');
      sess.turnCount++;
      sess.lastTouchedAt = Date.now();
      this.#resetIdleTimer(sess);
      const maxTurns = this.#config.delegation.maxTurnsPerSession ?? 30;
      if (sess.turnCount >= maxTurns) {
        log(`[chat-session] room=${spec.roomId} hit maxTurns=${maxTurns}, closing stdin for respawn`);
        try { sess.child.stdin.end(); } catch { /* already closed */ }
      }
      return { dispatched: true, pid: sess.pid };
    }

    // No live session — need to spawn. Check cap; try LRU-evict oldest idle on overflow.
    const cap = this.#config.delegation.maxConcurrent ?? 5;
    if (this.#sessions.size >= cap) {
      const evicted = this.#evictLru();
      if (!evicted) return { dispatched: false, reason: 'cap_busy' };
    }

    // Bootstrap history: ring first, REST as fallback.
    let history = (this.#historyRing.get(spec.roomId) || []).slice();
    if (history.length === 0) {
      try { history = await fetchChatRoomHistory(this.#config, spec.roomId); } catch { history = []; }
    }
    const firstTurnText = composeChatRoomPrompt(spec.roomId, history, {
      content: spec.content || '',
      sender_name: spec.senderName || '',
      sender_id: spec.senderId || '',
    });

    const spawned = await this.#spawn(spec.roomId, spec.rolePrompt || '', firstTurnText);
    if (!spawned) return { dispatched: false, reason: 'spawn_failed' };
    this.#sessions.set(spec.roomId, spawned);
    return { dispatched: true, pid: spawned.pid, firstTurn: true };
  }

  async #spawn(roomId, rolePrompt, firstTurnText) {
    let configPath = null;
    try {
      configPath = join(
        SUBAGENTS_BASE_DIR,
        `cfg-chat-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
      );
      await fsp.mkdir(dirname(configPath), { recursive: true, mode: 0o700 });
      const mcpConfig = {
        mcpServers: {
          awb: {
            type: 'http',
            url: `${this.#config.url.replace(/\/$/, '')}/mcp`,
            headers: { Authorization: `Bearer ${this.#config.apiKey}` },
          },
        },
      };
      await fsp.writeFile(configPath, JSON.stringify(mcpConfig), { mode: 0o600 });

      const args = [
        '--input-format', 'stream-json',
        '--output-format', 'stream-json',
        '--mcp-config', configPath,
        '--strict-mcp-config',
        '--allowedTools', 'mcp__awb__*',
        '--append-system-prompt', rolePrompt || '',
        '--dangerously-skip-permissions',
      ];
      const child = spawn(this.#config.delegation.claudeBin, args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        detached: true,
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
      });
      child.unref();

      if (!child.pid) {
        await fsp.unlink(configPath).catch(() => {});
        return null;
      }

      const sess = {
        roomId,
        pid: child.pid,
        child,
        configPath,
        turnCount: 0,
        startedAt: Date.now(),
        lastTouchedAt: Date.now(),
        idleTimer: null,
      };
      this.#wireStdio(sess);
      this.#wireExit(sess);

      log(`Subagent spawned: pid=${sess.pid} kind=chat_session room=${roomId}`);

      // Seed the first turn — role prompt + history + the current user message.
      this.#writeTurn(sess, firstTurnText);
      sess.turnCount = 1;
      this.#resetIdleTimer(sess);
      return sess;
    } catch (err) {
      log(`[chat-session] spawn error room=${roomId}: ${err.message}`);
      if (configPath) await fsp.unlink(configPath).catch(() => {});
      return null;
    }
  }

  #writeTurn(sess, text) {
    const obj = {
      type: 'user',
      message: { role: 'user', content: [{ type: 'text', text: String(text) }] },
    };
    try {
      sess.child.stdin.write(JSON.stringify(obj) + '\n');
      log(`[chat-session] dispatched turn room=${sess.roomId} pid=${sess.pid} turn=${sess.turnCount + 1} bytes=${Buffer.byteLength(text)}`);
    } catch (err) {
      log(`[chat-session] stdin write failed pid=${sess.pid}: ${err.message}`);
    }
  }

  #wireStdio(sess) {
    if (sess.child.stdout) {
      const rlOut = createInterface({ input: sess.child.stdout });
      rlOut.on('line', (line) => {
        // stream-json: one JSON object per line. Log result events; everything
        // else (assistant/system/tool_use) gets a terse line for debugging.
        try {
          const obj = JSON.parse(line);
          if (obj?.type === 'result') {
            log(`[chat-session:${sess.pid}] result subtype=${obj.subtype || '-'} is_error=${obj.is_error ?? '-'}`);
          } else if (obj?.type) {
            // Keep these quiet — they are frequent and mostly noise in the proxy log.
          }
        } catch {
          // non-JSON (rare); ignore
        }
      });
    }
    if (sess.child.stderr) {
      const rlErr = createInterface({ input: sess.child.stderr });
      rlErr.on('line', (line) => log(`[chat-session:${sess.pid}:err] ${line}`));
    }
  }

  #wireExit(sess) {
    sess.child.once('exit', async (code, signal) => {
      if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
      const durationSec = Math.round((Date.now() - sess.startedAt) / 1000);
      log(`[chat-session] exit pid=${sess.pid} room=${sess.roomId} code=${code} signal=${signal || '-'} turns=${sess.turnCount} duration=${durationSec}s`);
      if (this.#sessions.get(sess.roomId) === sess) this.#sessions.delete(sess.roomId);
      if (sess.configPath) {
        try { await fsp.unlink(sess.configPath); } catch { /* best-effort */ }
      }
    });
    sess.child.once('error', (err) => log(`[chat-session] child error pid=${sess.pid}: ${err.message}`));
  }

  #resetIdleTimer(sess) {
    if (sess.idleTimer) clearTimeout(sess.idleTimer);
    const mins = this.#config.delegation.idleMinutes ?? 10;
    sess.idleTimer = setTimeout(() => {
      log(`[chat-session] idle, closing stdin room=${sess.roomId} pid=${sess.pid}`);
      try { sess.child.stdin.end(); } catch { /* already closed */ }
    }, mins * 60_000);
    if (typeof sess.idleTimer.unref === 'function') sess.idleTimer.unref();
  }

  #evictLru() {
    let oldestKey = null;
    let oldest = Infinity;
    for (const [k, s] of this.#sessions.entries()) {
      if (s.lastTouchedAt < oldest) { oldest = s.lastTouchedAt; oldestKey = k; }
    }
    if (!oldestKey) return false;
    const s = this.#sessions.get(oldestKey);
    log(`[chat-session] evicting lru room=${oldestKey} pid=${s.pid}`);
    if (s.idleTimer) { clearTimeout(s.idleTimer); s.idleTimer = null; }
    try { s.child.stdin.end(); } catch { /* already closed */ }
    this.#sessions.delete(oldestKey);
    return true;
  }

  #rememberDedup(key) {
    this.#dedupSet.add(key);
    this.#dedupQueue.push(key);
    while (this.#dedupQueue.length > this.#DEDUP_MAX) {
      const old = this.#dedupQueue.shift();
      this.#dedupSet.delete(old);
    }
  }

  async stop() {
    const sessions = Array.from(this.#sessions.values());
    for (const sess of sessions) {
      if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
      try { sess.child.stdin.end(); } catch { /* ignore */ }
      try { process.kill(sess.pid, 'SIGTERM'); } catch { /* dead */ }
    }
    if (sessions.length === 0) { this.#sessions.clear(); return; }
    await new Promise((r) => setTimeout(r, STOP_GRACE_MS));
    for (const sess of sessions) {
      try { process.kill(sess.pid, 'SIGKILL'); } catch { /* gone */ }
    }
    this.#sessions.clear();
    log(`ChatSessionManager stopped (terminated ${sessions.length} sessions)`);
  }

  /** Test-only accessor: snapshot of active sessions. */
  _snapshot() {
    return Array.from(this.#sessions.values()).map((s) => ({
      roomId: s.roomId,
      pid: s.pid,
      turnCount: s.turnCount,
      startedAt: s.startedAt,
      lastTouchedAt: s.lastTouchedAt,
    }));
  }
}

// ─── Ticket Session Manager (v0.8.0 persistent per-ticket subagents) ────
/**
 * Keeps one Claude CLI child alive per ticket so that successive events
 * (trigger, board_update, comments) reuse the same KV cache and context.
 *
 * Mirrors ChatSessionManager's lifecycle model:
 *   - IDLE_TTL:   no events → stdin.end() → child exits
 *   - MAX_TURNS:  respawn on next event (fresh context)
 *   - CAP:        LRU-evict oldest-idle before spawn
 *
 * Trigger events spawn a new session with full ticket context.
 * Board_update events are forwarded to existing sessions as follow-up turns.
 */
class TicketSessionManager {
  #config;
  #sessions = new Map();         // ticketId → session
  #dedupSet = new Set();
  #dedupQueue = [];
  #DEDUP_MAX = 200;

  constructor(config) {
    this.#config = config;
  }

  /**
   * Dispatch a trigger into the ticket's live session, spawning one if needed.
   * spec = { ticketId, triggerId, agentId, rolePrompt, ticketPrompt, ticket }
   * Returns { dispatched: boolean, pid?: number, reason?: string, firstTurn?: boolean }
   */
  async dispatchTrigger(spec) {
    if (!spec.ticketId) return { dispatched: false, reason: 'no_ticket' };

    // Dedup by triggerId
    if (spec.triggerId) {
      const dedupKey = `trigger:${spec.triggerId}`;
      if (this.#dedupSet.has(dedupKey)) {
        return { dispatched: false, reason: 'duplicate_trigger' };
      }
      this.#rememberDedup(dedupKey);
    }

    let sess = this.#sessions.get(spec.ticketId);

    if (sess) {
      // Existing live session — send the trigger as a follow-up turn
      const turnText = this.#composeTriggerTurn(spec);
      this.#writeTurn(sess, turnText);
      sess.turnCount++;
      sess.lastTouchedAt = Date.now();
      this.#resetIdleTimer(sess);
      const maxTurns = this.#config.delegation.maxTurnsPerSession ?? 30;
      if (sess.turnCount >= maxTurns) {
        log(`[ticket-session] ticket=${spec.ticketId} hit maxTurns=${maxTurns}, closing for respawn`);
        try { sess.child.stdin.end(); } catch { /* already closed */ }
      }
      return { dispatched: true, pid: sess.pid };
    }

    // No live session — spawn. Check cap; LRU-evict on overflow.
    const cap = this.#config.delegation.maxConcurrent ?? 5;
    if (this.#sessions.size >= cap) {
      const evicted = this.#evictLru();
      if (!evicted) return { dispatched: false, reason: 'cap_busy' };
    }

    const firstTurnText = composeTriggerPrompt(
      spec.ticket, spec.rolePrompt || '', spec.ticketPrompt || '', spec.ticketId,
    );
    const spawned = await this.#spawn(spec.ticketId, spec.rolePrompt || '', firstTurnText);
    if (!spawned) return { dispatched: false, reason: 'spawn_failed' };
    this.#sessions.set(spec.ticketId, spawned);
    return { dispatched: true, pid: spawned.pid, firstTurn: true };
  }

  /**
   * Forward a board_update event to an existing ticket session.
   * Returns true if delivered, false if no live session exists for this ticket.
   */
  forwardBoardUpdate(ticketId, ev) {
    const sess = this.#sessions.get(ticketId);
    if (!sess) return false;

    const lines = [];
    lines.push(`[Board Update] The ticket you are working on was updated:`);
    lines.push(`  Event: ${ev.entity_type || 'unknown'}.${ev.action || 'unknown'}`);
    if (ev.field_changed) lines.push(`  Field changed: ${ev.field_changed}`);
    if (ev.actor_name) lines.push(`  By: ${ev.actor_name}`);
    lines.push('');
    lines.push('Review the change and adjust your work if needed. Use mcp__awb__get_ticket to fetch the latest ticket state.');

    this.#writeTurn(sess, lines.join('\n'));
    sess.turnCount++;
    sess.lastTouchedAt = Date.now();
    this.#resetIdleTimer(sess);
    return true;
  }

  #composeTriggerTurn(spec) {
    const lines = [];
    lines.push(`[New Trigger] A new trigger arrived for the ticket you are already working on.`);
    if (spec.ticketPrompt) {
      lines.push('');
      lines.push('Updated instructions:');
      lines.push(spec.ticketPrompt);
    }
    lines.push('');
    lines.push('Use mcp__awb__get_ticket to fetch the latest ticket state and continue your work.');
    return lines.join('\n');
  }

  async #spawn(ticketId, rolePrompt, firstTurnText) {
    let configPath = null;
    try {
      configPath = join(
        SUBAGENTS_BASE_DIR,
        `cfg-ticket-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
      );
      await fsp.mkdir(dirname(configPath), { recursive: true, mode: 0o700 });
      const mcpConfig = {
        mcpServers: {
          awb: {
            type: 'http',
            url: `${this.#config.url.replace(/\/$/, '')}/mcp`,
            headers: { Authorization: `Bearer ${this.#config.apiKey}` },
          },
        },
      };
      await fsp.writeFile(configPath, JSON.stringify(mcpConfig), { mode: 0o600 });

      const args = [
        '--input-format', 'stream-json',
        '--output-format', 'stream-json',
        '--mcp-config', configPath,
        '--strict-mcp-config',
        '--allowedTools', 'mcp__awb__*',
        '--append-system-prompt', rolePrompt || '',
        '--dangerously-skip-permissions',
      ];
      const child = spawn(this.#config.delegation.claudeBin, args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        detached: true,
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
      });
      child.unref();

      if (!child.pid) {
        await fsp.unlink(configPath).catch(() => {});
        return null;
      }

      const sess = {
        ticketId,
        pid: child.pid,
        child,
        configPath,
        turnCount: 0,
        startedAt: Date.now(),
        lastTouchedAt: Date.now(),
        idleTimer: null,
      };
      this.#wireStdio(sess);
      this.#wireExit(sess);

      log(`Subagent spawned: pid=${sess.pid} kind=ticket_session ticket=${ticketId}`);

      this.#writeTurn(sess, firstTurnText);
      sess.turnCount = 1;
      this.#resetIdleTimer(sess);
      return sess;
    } catch (err) {
      log(`[ticket-session] spawn error ticket=${ticketId}: ${err.message}`);
      if (configPath) await fsp.unlink(configPath).catch(() => {});
      return null;
    }
  }

  #writeTurn(sess, text) {
    const obj = {
      type: 'user',
      message: { role: 'user', content: [{ type: 'text', text: String(text) }] },
    };
    try {
      sess.child.stdin.write(JSON.stringify(obj) + '\n');
      log(`[ticket-session] dispatched turn ticket=${sess.ticketId} pid=${sess.pid} turn=${sess.turnCount + 1} bytes=${Buffer.byteLength(text)}`);
    } catch (err) {
      log(`[ticket-session] stdin write failed pid=${sess.pid}: ${err.message}`);
    }
  }

  #wireStdio(sess) {
    if (sess.child.stdout) {
      const rlOut = createInterface({ input: sess.child.stdout });
      rlOut.on('line', (line) => {
        try {
          const obj = JSON.parse(line);
          if (obj?.type === 'result') {
            log(`[ticket-session:${sess.pid}] result subtype=${obj.subtype || '-'} is_error=${obj.is_error ?? '-'}`);
          }
        } catch { /* non-JSON; ignore */ }
      });
    }
    if (sess.child.stderr) {
      const rlErr = createInterface({ input: sess.child.stderr });
      rlErr.on('line', (line) => log(`[ticket-session:${sess.pid}:err] ${line}`));
    }
  }

  #wireExit(sess) {
    sess.child.once('exit', async (code, signal) => {
      if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
      const durationSec = Math.round((Date.now() - sess.startedAt) / 1000);
      log(`[ticket-session] exit pid=${sess.pid} ticket=${sess.ticketId} code=${code} signal=${signal || '-'} turns=${sess.turnCount} duration=${durationSec}s`);
      if (this.#sessions.get(sess.ticketId) === sess) this.#sessions.delete(sess.ticketId);
      if (sess.configPath) {
        try { await fsp.unlink(sess.configPath); } catch { /* best-effort */ }
      }
    });
    sess.child.once('error', (err) => log(`[ticket-session] child error pid=${sess.pid}: ${err.message}`));
  }

  #resetIdleTimer(sess) {
    if (sess.idleTimer) clearTimeout(sess.idleTimer);
    const mins = this.#config.delegation.idleMinutes ?? 10;
    sess.idleTimer = setTimeout(() => {
      log(`[ticket-session] idle, closing stdin ticket=${sess.ticketId} pid=${sess.pid}`);
      try { sess.child.stdin.end(); } catch { /* already closed */ }
    }, mins * 60_000);
    if (typeof sess.idleTimer.unref === 'function') sess.idleTimer.unref();
  }

  #evictLru() {
    let oldestKey = null;
    let oldest = Infinity;
    for (const [k, s] of this.#sessions.entries()) {
      if (s.lastTouchedAt < oldest) { oldest = s.lastTouchedAt; oldestKey = k; }
    }
    if (!oldestKey) return false;
    const s = this.#sessions.get(oldestKey);
    log(`[ticket-session] evicting lru ticket=${oldestKey} pid=${s.pid}`);
    if (s.idleTimer) { clearTimeout(s.idleTimer); s.idleTimer = null; }
    try { s.child.stdin.end(); } catch { /* already closed */ }
    this.#sessions.delete(oldestKey);
    return true;
  }

  #rememberDedup(key) {
    this.#dedupSet.add(key);
    this.#dedupQueue.push(key);
    while (this.#dedupQueue.length > this.#DEDUP_MAX) {
      const old = this.#dedupQueue.shift();
      this.#dedupSet.delete(old);
    }
  }

  async stop() {
    const sessions = Array.from(this.#sessions.values());
    for (const sess of sessions) {
      if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
      try { sess.child.stdin.end(); } catch { /* ignore */ }
      try { process.kill(sess.pid, 'SIGTERM'); } catch { /* dead */ }
    }
    if (sessions.length === 0) { this.#sessions.clear(); return; }
    await new Promise((r) => setTimeout(r, STOP_GRACE_MS));
    for (const sess of sessions) {
      try { process.kill(sess.pid, 'SIGKILL'); } catch { /* gone */ }
    }
    this.#sessions.clear();
    log(`TicketSessionManager stopped (terminated ${sessions.length} sessions)`);
  }

  /** Test-only accessor: snapshot of active sessions. */
  _snapshot() {
    return Array.from(this.#sessions.values()).map((s) => ({
      ticketId: s.ticketId,
      pid: s.pid,
      turnCount: s.turnCount,
      startedAt: s.startedAt,
      lastTouchedAt: s.lastTouchedAt,
    }));
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

  // Phase 3 D-52: presence heartbeat. Resolves agent_id from agent.json (or via MCP whoami
  // if null), then pings every 30s so the dashboard keeps this agent marked online.
  // No-op if agent.json is missing — nothing pings, nothing breaks.
  let resolvedAgentId = null;
  const agentIdReady = resolveAgentId(config).then((id) => { resolvedAgentId = id; return id; });
  const presenceHeartbeat = { _real: null };

  // Phase 4 Plan 04-02: instantiate SubagentManager. Plan 04-03 now wires #handleTrigger
  // and #handleChatRequest consumers + the onExit completion notification below.
  const subagentManager = new SubagentManager(config);
  // Fire-and-forget init; log on failure. init() is idempotent and defers TTL sweep to setInterval.
  subagentManager.init().catch((err) => log(`SubagentManager init failed: ${err.message}`));

  // v0.7.0: persistent per-room chat sessions (separate lifecycle from trigger subagents).
  const chatSessionManager = new ChatSessionManager(config);

  // v0.8.0: persistent per-ticket sessions (trigger + board_update routed to same subagent).
  const ticketSessionManager = new TicketSessionManager(config);

  // Phase 4 D-69: completion notification. Fires for every subagent exit
  // (normal completion, non-zero failure, or TTL SIGTERM/SIGKILL timeout).
  // SubagentManager invokes this inside its exit handler — see Plan 04-02 #wireExitHandler.
  subagentManager.onExit = ({ pid, record, code, signal, durationSec }) => {
    const label = record.kind === 'chat' ? 'Chat Subagent' : 'Subagent';
    let msg;
    if (signal === 'SIGTERM' || signal === 'SIGKILL') {
      msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} TIMED OUT after ${durationSec}s`;
    } else if (code === 0) {
      msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} completed (duration=${durationSec}s)`;
    } else {
      msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} FAILED (exit=${code}, duration=${durationSec}s, see proxy logs)`;
    }
    // Strategy A fix: do NOT send notifications/claude/channel for subagent_complete.
    // Any sendChannelEvent() causes Claude CLI to close proxy stdin within ms, killing the proxy.
    log(msg);
  };

  const shutdownHandler = async (signal) => {
    log(`Proxy received ${signal} — terminating subagents`);
    presenceHeartbeat._real?.stop();
    eventStream?.stop();
    try { await subagentManager.stop(); } catch (err) { log(`shutdown: ${err.message}`); }
    try { await chatSessionManager.stop(); } catch (err) { log(`shutdown (chat): ${err.message}`); }
    try { await ticketSessionManager.stop(); } catch (err) { log(`shutdown (ticket): ${err.message}`); }
    process.exit(0);
  };
  process.once('SIGTERM', () => shutdownHandler('SIGTERM'));
  process.once('SIGINT', () => shutdownHandler('SIGINT'));

  rl.on('line', async (line) => {
    // DIAG v0.6.9: log EVERY inbound line — truncated — so we can see what (if anything)
    // Claude CLI sends right before closing stdin.
    try {
      const preview = line.length > 160 ? line.slice(0, 160) + '…' : line;
      log(`[DIAG] stdin.line bytes=${Buffer.byteLength(line)} preview=${preview}`);
    } catch { /* ignore */ }
    let msg;
    try { msg = JSON.parse(line); } catch { return; }

    // ── Intercept: initialize ──────────────────────────────
    // Patch the server's response to include claude/channel capability
    if (msg.method === 'initialize') {
      try {
        // Inject awb/schemaVersion so the server's schemaVersion gate accepts this
        // proxy session. Claude CLI's raw initialize doesn't include it, causing the
        // server to reject with "MCP proxy schemaVersion mismatch".
        msg.params ??= {};
        msg.params.capabilities ??= {};
        msg.params.capabilities.experimental ??= {};
        msg.params.capabilities.experimental['awb/schemaVersion'] = { version: 2 };
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
        eventStream = new EventStream(config, subagentManager, chatSessionManager, ticketSessionManager);
        eventStream.start();
        // Wait for agent_id resolution, then start heartbeat
        agentIdReady.then((agentId) => {
          presenceHeartbeat._real = new PresenceHeartbeat(config, agentId);
          presenceHeartbeat._real.start();
        });
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

  rl.on('close', async () => {
    // stdin close = parent Claude CLI went away (exit, terminal closed, crash).
    // We tear down our own I/O but deliberately ORPHAN any running subagents —
    // they were already dispatched and should finish their work. Killing them
    // here was the cause of v0.6.7 "dispatch then immediate SIGTERM" losses.
    log('stdin closed — orphaning subagents and exiting proxy');
    eventStream?.stop();
    presenceHeartbeat._real?.stop();
    process.exit(0);
  });

  log(`Proxy ready (server: ${config.url})`);
}

// ─── Main (only when executed directly, not when imported for tests) ───

const isDirectExecution =
  import.meta.url === `file://${process.argv[1]}` ||
  (typeof process.argv[1] === 'string' && process.argv[1].endsWith('proxy.mjs'));

if (isDirectExecution) {
  const config = loadConfig();
  const rl = createInterface({ input: process.stdin });

  if (!config?.url || !config?.apiKey) {
    runUnconfigured(rl);
  } else {
    runProxy(rl, config);
  }
}

export {
  SubagentManager,
  ChatSessionManager,
  TicketSessionManager,
  DELEGATION_DEFAULTS,
  loadConfig,
  EventStream,
  fetchTicketContext,
  composeTriggerPrompt,
  composeChatPrompt,
  composeChatRoomPrompt,
  fetchChatRoomHistory,
};

// Test-only seams — only exported when AWB_TEST_MODE is set. Plan 04-04 integration
// tests use these to invoke the private #handleTrigger / #handleChatRequest / #handleChatRoomMessage handlers
// without opening a real SSE stream. Each seam creates a transient EventStream bound
// to the provided (config, subagentManager) tuple and dispatches the raw payload.
export const _testDispatchTrigger =
  process.env.AWB_TEST_MODE === 'true'
    ? (config, subagentManager, raw, ticketSessionManager = null) =>
        new EventStream(config, subagentManager, null, ticketSessionManager)._testDispatchTrigger(raw)
    : undefined;

export const _testDispatchChatRequest =
  process.env.AWB_TEST_MODE === 'true'
    ? (config, subagentManager, raw, chatSessionManager = null) =>
        new EventStream(config, subagentManager, chatSessionManager)._testDispatchChatRequest(raw)
    : undefined;

export const _testDispatchChatRoomMessage =
  process.env.AWB_TEST_MODE === 'true'
    ? (config, subagentManager, raw, chatSessionManager = null) =>
        new EventStream(config, subagentManager, chatSessionManager)._testDispatchChatRoomMessage(raw)
    : undefined;
