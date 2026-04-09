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

const config = loadConfig();

if (!config || !config.url || !config.apiKey) {
  // Return a helpful error via MCP protocol then exit
  process.stderr.write(
    '[AWB] Not configured. Run /awb:setup <server-url> <api-key> to connect.\n'
  );
  // Keep stdin open briefly so Claude Code sees the server started
  // but respond to initialize with an error hint
  const rl = createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      if (msg.method === 'initialize') {
        const resp = {
          jsonrpc: '2.0',
          id: msg.id,
          result: {
            protocolVersion: '2025-03-26',
            capabilities: { tools: {} },
            serverInfo: { name: 'ai-workflow-board', version: '0.1.0' },
          },
        };
        process.stdout.write(JSON.stringify(resp) + '\n');
      } else if (msg.method === 'tools/list') {
        const resp = {
          jsonrpc: '2.0',
          id: msg.id,
          result: { tools: [] },
        };
        process.stdout.write(JSON.stringify(resp) + '\n');
      } else if (msg.id !== undefined) {
        const resp = {
          jsonrpc: '2.0',
          id: msg.id,
          error: { code: -32000, message: 'AWB not configured. Run /awb:setup <server-url> <api-key>' },
        };
        process.stdout.write(JSON.stringify(resp) + '\n');
      }
    } catch {}
  });
} else {
  // Configured — proxy stdio to remote HTTP MCP server
  const mcpUrl = config.url.replace(/\/$/, '') + '/mcp';
  let sessionId = null;

  const rl = createInterface({ input: process.stdin });

  rl.on('line', async (line) => {
    try {
      const msg = JSON.parse(line);

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
      });

      // Capture session ID from first response
      const sid = resp.headers.get('mcp-session-id');
      if (sid) sessionId = sid;

      const contentType = resp.headers.get('content-type') || '';

      if (contentType.includes('text/event-stream')) {
        // SSE response — parse events and forward
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
        // JSON response
        const body = await resp.text();
        if (body.trim()) {
          process.stdout.write(body + '\n');
        }
      }
    } catch (err) {
      process.stderr.write(`[AWB] Proxy error: ${err.message}\n`);
    }
  });

  rl.on('close', () => {
    process.exit(0);
  });

  process.stderr.write(`[AWB] Connected to ${config.url}\n`);
}
