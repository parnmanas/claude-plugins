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

import { createInterface } from 'readline';

import {
  REQUEST_TIMEOUT_MS,
  CHANNEL_INSTRUCTIONS,
  DELEGATION_DEFAULTS,
} from './lib/constants.mjs';
import { log, send, sendError } from './lib/logging.mjs';
import { loadConfig, resolveAgentId } from './lib/config.mjs';
import { fetchTicketContext, fetchChatRoomHistory } from './lib/rest.mjs';
import {
  composeTriggerPrompt,
  composeChatPrompt,
  composeChatRoomPrompt,
} from './lib/prompts.mjs';
import { PresenceHeartbeat } from './lib/presence-heartbeat.mjs';
import { EventStream } from './lib/event-stream.mjs';
import { SubagentManager } from './lib/subagent-manager.mjs';
import { ChatSessionManager } from './lib/chat-session-manager.mjs';
import { TicketSessionManager } from './lib/ticket-session-manager.mjs';
import { uploadIfNewErrors } from './lib/error-log-uploader.mjs';
import { onFlushThreshold } from './lib/event-log-recorder.mjs';

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
  let uploadTimer = null;

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
    if (uploadTimer) { clearInterval(uploadTimer); uploadTimer = null; }
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
          // v0.15.0: 30-second periodic tick + threshold-driven immediate flush.
          // Event-log entries need to reach the admin Agent Logs viewer fast
          // enough to actually debug "did the plugin see this event?" — the
          // old 10-minute cadence made the feature feel broken even when it
          // worked. Errors still piggyback on the same upload so we don't
          // multiply POSTs.
          const fireUpload = () => uploadIfNewErrors(config, agentId, '0.19.0').catch(() => {});
          fireUpload();
          uploadTimer = setInterval(fireUpload, 30 * 1000);
          if (typeof uploadTimer.unref === 'function') uploadTimer.unref();
          // Kick an out-of-band upload when the event buffer crosses 10 entries
          // so a burst of SSE events doesn't sit around for 30s.
          onFlushThreshold(fireUpload);
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
