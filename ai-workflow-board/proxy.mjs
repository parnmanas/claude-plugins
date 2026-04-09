#!/usr/bin/env node

/**
 * AWB MCP Proxy — stdio-to-HTTP bridge
 *
 * Reads connection config from ~/.claude/channels/awb/config.json
 * and proxies MCP JSON-RPC over stdio to the remote AWB server.
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

function handleProxy(rl, config) {
  const mcpUrl = config.url.replace(/\/$/, '') + '/mcp';
  let sessionId = null;

  rl.on('line', async (line) => {
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      return; // malformed JSON, ignore
    }

    try {
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

      // Capture session ID from first response
      const sid = resp.headers.get('mcp-session-id');
      if (sid) sessionId = sid;

      const contentType = resp.headers.get('content-type') || '';

      if (contentType.includes('text/event-stream')) {
        const text = await resp.text();
        for (const chunk of text.split('\n')) {
          if (chunk.startsWith('data: ')) {
            const data = chunk.slice(6).trim();
            if (data) {
              process.stdout.write(data + '\n');
            }
          }
        }
      } else {
        const body = await resp.text();
        if (body.trim()) {
          process.stdout.write(body + '\n');
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
    process.exit(0);
  });

  process.stderr.write(`[AWB] Proxy ready (server: ${config.url})\n`);
}

// ─── Main ───────────────────────────────────────────────────

const config = loadConfig();
const rl = createInterface({ input: process.stdin });

if (!config || !config.url || !config.apiKey) {
  handleNotConfigured(rl);
} else {
  handleProxy(rl, config);
}
