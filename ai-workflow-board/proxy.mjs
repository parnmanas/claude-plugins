#!/usr/bin/env node

/**
 * AWB MCP Proxy — stdio-to-HTTP bridge with Channel support
 *
 * Reads connection config from ~/.claude/channels/awb/config.json
 * and proxies MCP JSON-RPC over stdio to the remote AWB server.
 *
 * When loaded as a channel (--channels), emits notifications/claude/channel
 * for incoming agent triggers via SSE stream.
 * When loaded as a regular MCP server, triggers are available via
 * get_pending_triggers / subscribe_events tools (polling fallback).
 *
 * Config format:
 * {
 *   "url": "https://awb.example.com:7700",
 *   "apiKey": "awb_..."
 * }
 */

import { readFileSync, existsSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';
import { createInterface } from 'readline';

const CONFIG_PATH = join(homedir(), '.claude', 'channels', 'awb', 'config.json');

// Prevent unhandled errors from crashing the process (and Claude CLI with it)
process.on('uncaughtException', (err) => {
  process.stderr.write(`[AWB] Uncaught error: ${err.message}\n`);
});
process.on('unhandledRejection', (err) => {
  process.stderr.write(`[AWB] Unhandled rejection: ${err}\n`);
});

function loadConfig() {
  if (!existsSync(CONFIG_PATH)) {
    return null;
  }
  try {
    return JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
  } catch {
    return null;
  }
}

function sendResponse(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

function sendError(id, code, message) {
  sendResponse({ jsonrpc: '2.0', id: id ?? null, error: { code, message } });
}

/** Send a channel notification to Claude (works only when loaded as channel) */
function sendChannelNotification(content, meta = {}) {
  sendResponse({
    jsonrpc: '2.0',
    method: 'notifications/claude/channel',
    params: { content, meta },
  });
}

function handleNotConfigured(rl) {
  process.stderr.write('[AWB] Not configured. Run /awb:setup <server-url> <api-key> to connect.\n');

  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      if (msg.method === 'initialize') {
        sendResponse({
          jsonrpc: '2.0',
          id: msg.id,
          result: {
            protocolVersion: '2025-03-26',
            capabilities: { tools: {} },
            serverInfo: { name: 'ai-workflow-board', version: '0.1.0' },
          },
        });
      } else if (msg.method === 'tools/list') {
        sendResponse({ jsonrpc: '2.0', id: msg.id, result: { tools: [] } });
      } else if (msg.method === 'notifications/initialized') {
        // notification — no response needed
      } else if (msg.id !== undefined) {
        sendError(msg.id, -32000, 'AWB not configured. Run /awb:setup <server-url> <api-key>');
      }
    } catch {}
  });
}

// ─── SSE Event Stream for Channel Push ─────────────────────

/**
 * Connect to AWB's SSE event stream and forward agent_trigger events
 * as channel notifications to Claude.
 */
function startEventStream(config) {
  const streamUrl = `${config.url.replace(/\/$/, '')}/api/events/stream?token=${encodeURIComponent(config.apiKey)}`;
  let retryDelay = 2000;
  let abortController = null;

  async function connect() {
    try {
      abortController = new AbortController();
      const resp = await fetch(streamUrl, {
        headers: { 'Accept': 'text/event-stream' },
        signal: abortController.signal,
      });

      if (!resp.ok) {
        process.stderr.write(`[AWB] SSE stream error: ${resp.status} ${resp.statusText}\n`);
        scheduleReconnect();
        return;
      }

      process.stderr.write('[AWB] SSE event stream connected\n');
      retryDelay = 2000; // reset on successful connection

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop(); // keep incomplete line in buffer

        let eventType = '';
        for (const line of lines) {
          if (line.startsWith('event: ')) {
            eventType = line.slice(7).trim();
          } else if (line.startsWith('data: ')) {
            const data = line.slice(6).trim();
            if (data && eventType === 'agent_trigger') {
              handleTriggerEvent(data);
            }
            eventType = '';
          } else if (line === '') {
            eventType = '';
          }
        }
      }

      // Stream ended cleanly — reconnect
      process.stderr.write('[AWB] SSE stream ended, reconnecting...\n');
      scheduleReconnect();
    } catch (err) {
      if (err.name === 'AbortError') return; // intentional disconnect
      process.stderr.write(`[AWB] SSE stream error: ${err.message}\n`);
      scheduleReconnect();
    }
  }

  function handleTriggerEvent(data) {
    try {
      const event = JSON.parse(data);
      // event from events.controller: { event_type, ticket_id, entity_type, action (role),
      //   field_changed (trigger_id), actor_name (agent_id), timestamp }
      const content = `[AWB Trigger] ticket=${event.ticket_id} role=${event.action} trigger=${event.field_changed}`;
      sendChannelNotification(content, {
        type: 'agent_trigger',
        ticket_id: event.ticket_id || '',
        trigger_id: event.field_changed || '',
        agent_id: event.actor_name || '',
        role: event.action || '',
        timestamp: event.timestamp || new Date().toISOString(),
      });
      process.stderr.write(`[AWB] Trigger forwarded: ticket=${event.ticket_id} role=${event.action}\n`);
    } catch (err) {
      process.stderr.write(`[AWB] Failed to parse trigger event: ${err.message}\n`);
    }
  }

  function scheduleReconnect() {
    setTimeout(connect, retryDelay);
    retryDelay = Math.min(retryDelay * 1.5, 30000); // exponential backoff, max 30s
  }

  // Return cleanup function
  connect();
  return () => {
    if (abortController) abortController.abort();
  };
}

// ─── Main Proxy ────────────────────────────────────────────

function handleProxy(rl, config) {
  const mcpUrl = config.url.replace(/\/$/, '') + '/mcp';
  let sessionId = null;
  let stopStream = null;

  rl.on('line', async (line) => {
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      return; // malformed JSON, ignore
    }

    // Intercept initialize to inject channel capability
    if (msg.method === 'initialize') {
      try {
        const result = await forwardToServer(mcpUrl, config, msg, sessionId);
        if (result.sessionId) sessionId = result.sessionId;

        // Patch capabilities to include channel support
        const body = result.body;
        if (body.result && body.result.capabilities) {
          if (!body.result.capabilities.experimental) {
            body.result.capabilities.experimental = {};
          }
          body.result.capabilities.experimental['claude/channel'] = {};
        }

        // Add instructions for hybrid mode
        if (body.result) {
          body.result.instructions = [
            body.result.instructions || '',
            '',
            'This server supports both push and poll modes.',
            'If you receive <channel> events with type="agent_trigger", react to them immediately — claim the ticket, read it, and process it.',
            'If no <channel> events arrive, use get_pending_triggers or subscribe_events to poll for work.',
          ].join('\n').trim();
        }

        sendResponse(body);
      } catch (err) {
        process.stderr.write(`[AWB] Initialize error: ${err.message}\n`);
        if (msg.id !== undefined) {
          sendError(msg.id, -32000, `AWB proxy error: ${err.message}`);
        }
      }
      return;
    }

    // Start SSE stream after initialized notification
    if (msg.method === 'notifications/initialized') {
      if (!stopStream) {
        stopStream = startEventStream(config);
      }
      // notification — no response needed
      return;
    }

    // All other messages: forward to server
    try {
      const result = await forwardToServer(mcpUrl, config, msg, sessionId);
      if (result.sessionId) sessionId = result.sessionId;
      if (result.body && msg.id !== undefined) {
        sendResponse(result.body);
      } else if (result.lines) {
        // SSE response — multiple JSON-RPC messages
        for (const line of result.lines) {
          process.stdout.write(line + '\n');
        }
      }
    } catch (err) {
      process.stderr.write(`[AWB] Proxy error: ${err.message}\n`);

      // Return MCP error response so Claude CLI doesn't hang
      if (msg.id !== undefined) {
        const errMsg = err.cause?.code === 'ECONNREFUSED'
          ? `AWB server unreachable at ${config.url}. Is the server running?`
          : `AWB proxy error: ${err.message}`;
        sendError(msg.id, -32000, errMsg);
      }
      // If it was a notification (no id), just log and continue
    }
  });

  rl.on('close', () => {
    if (stopStream) stopStream();
    process.exit(0);
  });

  process.stderr.write(`[AWB] Proxy ready (server: ${config.url})\n`);
}

async function forwardToServer(mcpUrl, config, msg, sessionId) {
  const headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json, text/event-stream',
    'Authorization': `Bearer ${config.apiKey}`,
  };
  if (sessionId) {
    headers['mcp-session-id'] = sessionId;
  }

  const resp = await fetch(mcpUrl, {
    method: 'POST',
    headers,
    body: JSON.stringify(msg),
    signal: AbortSignal.timeout(30000),
  });

  const newSessionId = resp.headers.get('mcp-session-id');
  const contentType = resp.headers.get('content-type') || '';

  if (contentType.includes('text/event-stream')) {
    const text = await resp.text();
    const lines = [];
    for (const chunk of text.split('\n')) {
      if (chunk.startsWith('data: ')) {
        const data = chunk.slice(6).trim();
        if (data) lines.push(data);
      }
    }
    return { sessionId: newSessionId || sessionId, lines };
  } else {
    const text = await resp.text();
    let body = null;
    if (text.trim()) {
      try {
        body = JSON.parse(text);
      } catch {
        body = null;
      }
    }
    return { sessionId: newSessionId || sessionId, body };
  }
}

// ─── Main ──────────────────────────────────────────────────

const config = loadConfig();
const rl = createInterface({ input: process.stdin });

if (!config || !config.url || !config.apiKey) {
  handleNotConfigured(rl);
} else {
  handleProxy(rl, config);
}
