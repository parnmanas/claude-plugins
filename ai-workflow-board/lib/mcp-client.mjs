// ─── MCP HTTP client helper ───────────────────────────────
// Thin wrapper around AWB's Streamable HTTP /mcp endpoint for short-lived
// JSON-RPC sessions. Used by presence-heartbeat and session-manager
// lifecycle hooks (set/clear current_task).
//
// Each call opens a fresh session: initialize → notifications/initialized →
// tools/call → DELETE. Cheaper than holding a persistent session and avoids
// Mcp-Session-Id contention with the Claude CLI's own stdio session that
// flows through the same proxy.

import { REQUEST_TIMEOUT_MS } from './constants.mjs';
import { log } from './logging.mjs';

/**
 * Open a short-lived MCP session against the AWB server, call a single tool,
 * and tear the session down. Returns the parsed `result` field of the
 * tools/call JSON-RPC response, or throws on transport / protocol failure.
 *
 * Errors are returned (not thrown) for the caller to inspect when the
 * tools/call itself returned a JSON-RPC error envelope. Callers that want
 * fire-and-forget semantics should `.catch(() => null)`.
 */
export async function callMcpTool(config, toolName, toolArgs, opts = {}) {
  const base = (config?.url || '').replace(/\/$/, '');
  if (!base) throw new Error('callMcpTool: config.url missing');
  if (!config?.apiKey) throw new Error('callMcpTool: config.apiKey missing');
  const url = `${base}/mcp`;
  const timeoutMs = opts.timeoutMs ?? REQUEST_TIMEOUT_MS;

  const baseHeaders = {
    Authorization: `Bearer ${config.apiKey}`,
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
  };

  // Step 1: initialize → grab Mcp-Session-Id
  const initResp = await fetch(url, {
    method: 'POST',
    headers: baseHeaders,
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'initialize',
      params: {
        protocolVersion: '2025-03-26',
        capabilities: { experimental: { 'awb/schemaVersion': { version: 2 } } },
        clientInfo: { name: opts.clientName || 'awb-plugin-tool', version: '1.0.0' },
      },
    }),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!initResp.ok) {
    throw new Error(`initialize HTTP ${initResp.status}`);
  }
  const sid = initResp.headers.get('mcp-session-id');
  if (!sid) throw new Error('initialize did not return Mcp-Session-Id');
  await initResp.text().catch(() => null);

  const sessionHeaders = { ...baseHeaders, 'Mcp-Session-Id': sid };

  // Step 2: notifications/initialized — required before tool calls
  await fetch(url, {
    method: 'POST',
    headers: sessionHeaders,
    body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }),
    signal: AbortSignal.timeout(timeoutMs),
  }).then((r) => r.text().catch(() => null));

  // Step 3: tools/call
  let result = null;
  try {
    const callResp = await fetch(url, {
      method: 'POST',
      headers: sessionHeaders,
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 2,
        method: 'tools/call',
        params: { name: toolName, arguments: toolArgs || {} },
      }),
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!callResp.ok) {
      throw new Error(`tools/call ${toolName} HTTP ${callResp.status}`);
    }
    result = await parseStreamableResponse(callResp);
  } finally {
    // Step 4: DELETE session — fire-and-forget
    fetch(url, { method: 'DELETE', headers: sessionHeaders, signal: AbortSignal.timeout(timeoutMs) })
      .then((r) => r.text().catch(() => null))
      .catch(() => { /* ignore — server TTL will reap */ });
  }

  return result;
}

/**
 * AWB's MCP endpoint accepts both application/json and text/event-stream
 * responses (Streamable HTTP). The response body for a single JSON-RPC reply
 * is either a plain JSON object or a one-frame SSE stream — both forms carry
 * the same `{ jsonrpc, id, result }` envelope. This helper handles either.
 */
async function parseStreamableResponse(resp) {
  const contentType = resp.headers.get('content-type') || '';
  const text = await resp.text();
  if (!text) return null;

  if (contentType.includes('text/event-stream')) {
    // SSE frame: "event: message\ndata: {...}\n\n" — parse the data line.
    for (const line of text.split('\n')) {
      const m = /^data:\s*(.+)$/.exec(line);
      if (m) {
        try { return JSON.parse(m[1]); } catch { /* keep scanning */ }
      }
    }
    return null;
  }

  try { return JSON.parse(text); } catch { return null; }
}

/**
 * Convenience: extract the structured tool result out of the JSON-RPC envelope.
 * AWB tools return `{ content: [{ type: 'text', text: '<json>' }] }`; the inner
 * text is JSON-encoded. Returns null when the envelope shape is unexpected.
 */
export function unwrapToolResult(rpcResponse) {
  const content = rpcResponse?.result?.content;
  if (!Array.isArray(content) || content.length === 0) return null;
  const text = content[0]?.text;
  if (typeof text !== 'string') return null;
  try { return JSON.parse(text); } catch { return text; }
}

/**
 * Fire-and-forget tool call. Logs failures but never throws — for use in
 * subagent lifecycle hooks where a transient signal failure should not
 * impact the subagent's actual exit handling.
 */
export async function fireAndForgetTool(config, toolName, toolArgs) {
  try {
    await callMcpTool(config, toolName, toolArgs);
  } catch (err) {
    log(`MCP tool ${toolName} failed (fire-and-forget): ${err.message}`);
  }
}
