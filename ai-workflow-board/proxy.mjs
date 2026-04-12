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
  ttlMinutes: 15,             // D-67
  claudeBin: 'claude',        // D-75 — overridable for test stubs
  appendSystemPromptMode: 'role_only', // D-75 — reserved for Plan 04-03 prompt composition
});
const TTL_SWEEP_INTERVAL_MS = 60_000;
const SIGTERM_GRACE_MS = 5_000;
const STOP_GRACE_MS = 2_000;

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
    const url = `${config.url.replace(/\/$/, '')}/api/chat-rooms/${encodeURIComponent(roomId)}/messages?limit=${limit}`;
    const resp = await fetch(url, {
      headers: {
        Authorization: `Bearer ${config.apiKey}`,
        Accept: 'application/json',
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

  constructor(config, subagentManager = null) {
    this.#url = `${config.url.replace(/\/$/, '')}/api/events/stream?token=${encodeURIComponent(config.apiKey)}`;
    this.#config = config;
    this.#subagentManager = subagentManager;
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
    const canDelegate = delegationEnabled && this.#subagentManager && this.#subagentManager.canSpawn();

    if (canDelegate) {
      try {
        // D-60: fetch fresh ticket context (best-effort; null → composeTriggerPrompt handles the null case)
        const ticket = await fetchTicketContext(this.#config, ev.ticket_id);
        // D-61: role_prompt and ticket_prompt are at TOP LEVEL of the flatten-on-emit shape
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
          // Dispatch notification (D-59): lightweight "dispatched" line, NOT the full trigger payload
          sendChannelEvent(
            `[AWB Subagent] Dispatched ticket=${ev.ticket_id} trigger=${ev.field_changed} pid=${result.pid}`,
            {
              type: 'subagent_dispatched',
              subagent_kind: 'trigger',
              ticket_id: ev.ticket_id || '',
              trigger_id: ev.field_changed || '',
              agent_id: ev.actor_name || '',
              pid: result.pid,
              timestamp: ev.timestamp || new Date().toISOString(),
            },
          );
          log(`Trigger dispatched to subagent: ticket=${ev.ticket_id} pid=${result.pid}`);
          return;
        }
        // spawn() returned {spawned: false, reason} — log and fall through to legacy path
        log(`Subagent spawn declined (${result.reason}), falling back to main session forward`);
      } catch (err) {
        log(`Delegation path failed: ${err.message}, falling back to main session forward`);
        // fall through to legacy path
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
          // Dedup key: agent + user + ticket + timestamp — protects against SSE reconnect duplicates
          chatRequestId: payload.user_id
            ? `${payload.agent_id}:${payload.user_id}:${payload.ticket_id || 'global'}:${ev.timestamp || ''}`
            : undefined,
          ticketId: payload.ticket_id || '',
          agentId: payload.agent_id || '',
        });

        if (result.spawned) {
          sendChannelEvent(
            `[AWB Chat Subagent] Dispatched agent=${payload.agent_id} user=${payload.user_id} pid=${result.pid}`,
            {
              type: 'subagent_dispatched',
              subagent_kind: 'chat',
              agent_id: payload.agent_id || '',
              user_id: payload.user_id || '',
              ticket_id: payload.ticket_id || '',
              pid: result.pid,
              timestamp: ev.timestamp || new Date().toISOString(),
            },
          );
          log(`Chat request dispatched to subagent: agent=${payload.agent_id} pid=${result.pid}`);
          return;
        }
        // spawn() returned {spawned: false, reason} — log and fall through to fallback path
        log(`Chat subagent spawn declined (${result.reason}), falling back to main session forward`);
      } catch (err) {
        log(`Chat delegation path failed: ${err.message}, falling back to main session forward`);
        // fall through to fallback path
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

  async #handleChatRoomMessage(raw) {
    let ev;
    try {
      ev = JSON.parse(raw);
    } catch (err) {
      log(`Failed to parse chat_room_message: ${err.message}`);
      return;
    }

    const p = ev.payload || ev;

    // Skip messages sent by agents to avoid self-reply loops
    if (p.sender_type === 'agent') {
      log(`Chat room message from agent (${p.sender_name || p.sender_id}) — skipping delegation`);
      return;
    }

    const delegationEnabled = this.#config?.delegation?.enabled !== false;
    const canDelegate = delegationEnabled && this.#subagentManager && this.#subagentManager.canSpawn();

    if (canDelegate) {
      try {
        // Fetch recent conversation history for context
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
          chatRequestId: `room:${p.room_id}:${p.sender_id}:${p.created_at || ''}`,
          ticketId: '',
          agentId: '',
        });

        if (result.spawned) {
          sendChannelEvent(
            `[AWB Chat Room Subagent] Dispatched room=${p.room_id} sender=${p.sender_name || p.sender_id} pid=${result.pid}`,
            {
              type: 'subagent_dispatched',
              subagent_kind: 'chat',
              room_id: p.room_id || '',
              sender_id: p.sender_id || '',
              sender_name: p.sender_name || '',
              pid: result.pid,
              timestamp: p.created_at || new Date().toISOString(),
            },
          );
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

    // Concurrency cap check — synchronous reservation closes Pitfall 4 race
    if (!this.canSpawn()) {
      return { spawned: false, reason: 'cap_reached' };
    }
    const reservationId = -(++this.#reservationCounter);
    this.#map.set(reservationId, { kind: 'reservation', started_at: Date.now() });

    let finalConfigPath = null;
    try {
      // Step A: write per-subagent MCP config file to a temp path first (we don't have pid yet)
      const tmpConfigPath = join(
        this.#pidDir,
        `pending-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
      );
      await fsp.mkdir(dirname(tmpConfigPath), { recursive: true, mode: 0o700 });

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
      await fsp.writeFile(tmpConfigPath, JSON.stringify(mcpConfig), { mode: 0o600 });

      // Step B: spawn. Pitfall 6: stdio[0]='ignore' closes child stdin.
      // All argv values are separate array elements — never shell-interpolated.
      const args = [
        '--print',
        '--output-format', 'json',
        '--mcp-config', tmpConfigPath,
        '--strict-mcp-config',
        '--allowedTools', 'mcp__awb__*',
        '--append-system-prompt', spec.rolePrompt || '',
        '--dangerously-skip-permissions',
        spec.taskText,
      ];
      const child = spawn(this.#config.delegation.claudeBin, args, {
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: false,     // D-62: signals propagate to children
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
      });

      const pid = child.pid;
      if (!pid) {
        await fsp.unlink(tmpConfigPath).catch(() => {});
        this.#map.delete(reservationId);
        return { spawned: false, reason: 'spawn_failed' };
      }

      // Step C: rename config file into a pid-keyed directory
      finalConfigPath = join(this.#pidDir, String(pid), 'mcp-config.json');
      await fsp.mkdir(dirname(finalConfigPath), { recursive: true, mode: 0o700 });
      await fsp.rename(tmpConfigPath, finalConfigPath);

      // Step D: register record
      const record = {
        pid,
        kind: spec.kind,
        trigger_id: spec.triggerId || null,
        chat_request_id: spec.chatRequestId || null,
        ticket_id: spec.ticketId || null,
        agent_id: spec.agentId || null,
        started_at: Date.now(),
        expected_completion_at: Date.now() + this.#config.delegation.ttlMinutes * 60_000,
        config_path: finalConfigPath,
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
      if (finalConfigPath) {
        await fsp.rm(dirname(finalConfigPath), { recursive: true, force: true }).catch(() => {});
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
        await fsp.rm(dirname(record.config_path), { recursive: true, force: true });
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
    try {
      sendChannelEvent(msg, {
        type: 'subagent_complete',
        subagent_kind: record.kind,
        ticket_id: record.ticket_id || '',
        trigger_id: record.trigger_id || '',
        agent_id: record.agent_id || '',
        pid,
        exit_code: code ?? null,
        signal: signal ?? null,
        duration_sec: durationSec,
      });
    } catch (err) {
      log(`Completion notification failed: ${err.message}`);
    }
  };

  const shutdownHandler = async (signal) => {
    log(`Proxy received ${signal} — terminating subagents`);
    presenceHeartbeat._real?.stop();
    eventStream?.stop();
    try { await subagentManager.stop(); } catch (err) { log(`shutdown: ${err.message}`); }
    process.exit(0);
  };
  process.once('SIGTERM', () => shutdownHandler('SIGTERM'));
  process.once('SIGINT', () => shutdownHandler('SIGINT'));

  rl.on('line', async (line) => {
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
        eventStream = new EventStream(config, subagentManager);
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
    log('stdin closed — shutting down proxy');
    eventStream?.stop();
    presenceHeartbeat._real?.stop();
    try { await subagentManager.stop(); } catch { /* ignore */ }
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
    ? (config, subagentManager, raw) =>
        new EventStream(config, subagentManager)._testDispatchTrigger(raw)
    : undefined;

export const _testDispatchChatRequest =
  process.env.AWB_TEST_MODE === 'true'
    ? (config, subagentManager, raw) =>
        new EventStream(config, subagentManager)._testDispatchChatRequest(raw)
    : undefined;

export const _testDispatchChatRoomMessage =
  process.env.AWB_TEST_MODE === 'true'
    ? (config, subagentManager, raw) =>
        new EventStream(config, subagentManager)._testDispatchChatRoomMessage(raw)
    : undefined;
