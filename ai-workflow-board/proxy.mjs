#!/usr/bin/env node

/**
 * AWB MCP Proxy — stdio-to-HTTP bridge for Claude CLI.
 *
 *   Claude CLI <--stdio--> proxy.mjs <--HTTP--> AWB Server (/mcp)
 *
 * Single responsibility: forward MCP JSON-RPC traffic. SSE channel events,
 * subagent delegation, persistent ticket/chat sessions, and CLI adapters
 * moved to the standalone @awb/agent-manager package (apps/agent-manager
 * inside the ai-workflow-board repo).
 *
 * Config: ~/.claude/channels/awb/config.json
 *   { "url": "https://awb.example.com:7700", "apiKey": "awb_..." }
 */

import { createInterface } from 'readline';
import { loadConfig } from './lib/config.mjs';
import { log, send, sendError } from './lib/logging.mjs';
import { McpForwardSession } from './lib/mcp-forward-session.mjs';

/** Handle the not-configured state — respond to MCP handshake with empty tools. */
function runUnconfigured(rl) {
  log('Not configured. Run /awb:setup <server-url> <api-key> to connect.');

  rl.on('line', (line) => {
    let msg;
    try { msg = JSON.parse(line); } catch { return; }

    if (msg.method === 'initialize') {
      send({
        jsonrpc: '2.0',
        id: msg.id,
        result: {
          protocolVersion: '2025-03-26',
          capabilities: { tools: {} },
          serverInfo: { name: 'ai-workflow-board', version: '0.0.0-unconfigured' },
        },
      });
    } else if (msg.method === 'tools/list') {
      send({ jsonrpc: '2.0', id: msg.id, result: { tools: [] } });
    } else if (msg.id !== undefined && msg.method !== 'notifications/initialized') {
      sendError(msg.id, -32000, 'AWB not configured. Run /awb:setup <server-url> <api-key>');
    }
  });
}

function runProxy(rl, config) {
  const mcpUrl = config.url.replace(/\/$/, '') + '/mcp';
  log(`Proxy starting (server=${config.url})`);

  const forwardSession = new McpForwardSession(mcpUrl, config.apiKey);

  const shutdown = (signal) => {
    log(`Proxy received ${signal} — shutting down`);
    forwardSession.stop();
    process.exit(0);
  };
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  process.once('SIGINT', () => shutdown('SIGINT'));

  rl.on('line', async (line) => {
    let msg;
    try { msg = JSON.parse(line); } catch { return; }

    if (msg.method === 'initialize') {
      try {
        // Inject awb/schemaVersion so the server's schemaVersion gate accepts
        // this proxy session. Claude CLI's raw initialize doesn't include it.
        msg.params ??= {};
        msg.params.capabilities ??= {};
        msg.params.capabilities.experimental ??= {};
        msg.params.capabilities.experimental['awb/schemaVersion'] = { version: 2 };
        const result = await forwardSession.handleClaudeInitialize(msg);
        send(result.body);
      } catch (err) {
        log(`Initialize error: ${err.message}`);
        if (msg.id !== undefined) sendError(msg.id, -32000, `AWB proxy error: ${err.message}`);
      }
      return;
    }

    if (msg.method === 'notifications/initialized') return;

    try {
      const result = await forwardSession.forward(msg);
      if (result.lines) {
        for (const l of result.lines) process.stdout.write(l + '\n');
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

  rl.on('close', () => shutdown('stdin-close'));

  log(`Proxy ready (server: ${config.url})`);
}

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

export { McpForwardSession, loadConfig };
