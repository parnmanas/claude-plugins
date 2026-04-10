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
const RECONNECT_INITIAL_MS = 2000;
const RECONNECT_MAX_MS = 30000;
const REQUEST_TIMEOUT_MS = 30000;

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
          } else if (data && eventType === 'board_update') {
            this.#handleBoardUpdate(data);
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

  #scheduleReconnect() {
    if (this.#stopped) return;
    setTimeout(() => this.#connect(), this.#retryDelay);
    this.#retryDelay = Math.min(this.#retryDelay * 1.5, RECONNECT_MAX_MS);
  }
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

  // Phase 4 Plan 04-02: instantiate SubagentManager. Behaviorally inert until Plan 04-03
  // wires #handleTrigger / #handleChatRequest consumers. The signal handlers below ensure
  // orphan cleanup on proxy shutdown regardless of whether consumers are live.
  const subagentManager = new SubagentManager(config);
  // Fire-and-forget init; log on failure. init() is idempotent and defers TTL sweep to setInterval.
  subagentManager.init().catch((err) => log(`SubagentManager init failed: ${err.message}`));

  const shutdownHandler = async (signal) => {
    log(`Proxy received ${signal} — terminating subagents`);
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

  rl.on('close', async () => {
    eventStream?.stop();
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

export { SubagentManager, DELEGATION_DEFAULTS, loadConfig };
