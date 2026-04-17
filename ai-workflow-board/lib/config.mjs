// ─── Config + agent identity loaders ─────────────────────
// Reads ~/.claude/channels/awb/{config,agent}.json. Exposes a `resolveAgentId`
// helper that fills in the cached agent.json when /awb:setup left agent_id
// null (e.g., because the setup skill ran before the server had a record).

import { readFileSync, existsSync } from 'fs';
import { promises as fsp } from 'fs';
import {
  CONFIG_PATH,
  AGENT_PATH,
  DELEGATION_DEFAULTS,
  REQUEST_TIMEOUT_MS,
} from './constants.mjs';
import { log } from './logging.mjs';

export function loadConfig() {
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
export function loadAgentInfo() {
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
export async function resolveAgentId(config) {
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
