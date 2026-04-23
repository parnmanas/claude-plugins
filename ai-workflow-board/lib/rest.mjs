// ─── REST fetchers ────────────────────────────────────────
// Thin HTTP helpers that hit AWB REST endpoints. Kept free of class references
// so they can be imported by EventStream and every session manager without
// circular-import risk.

import { REQUEST_TIMEOUT_MS } from './constants.mjs';
import { log } from './logging.mjs';

/**
 * Fetch a fresh ticket with comments from AWB REST (D-60).
 * Used by #handleTrigger to compose the subagent task prompt.
 * Returns null on any failure; caller falls back to embedded trigger payload fields.
 */
export async function fetchTicketContext(config, ticketId) {
  if (!ticketId) return null;
  try {
    // /api/agent/tickets/:id is guarded by AgentAuthGuard (X-Agent-Key), not
    // the session-token AuthGuard behind /api/tickets/:id — the plugin holds
    // an AWB API key, not a user session, so it must hit the agent route.
    const url = `${config.url.replace(/\/$/, '')}/api/agent/tickets/${encodeURIComponent(ticketId)}`;
    const resp = await fetch(url, {
      headers: {
        'X-Agent-Key': config.apiKey,
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
 * Fetch recent chat room messages from AWB REST API.
 * Returns array of {sender_type, sender_name, content, created_at} or empty on failure.
 */
export async function fetchChatRoomHistory(config, roomId, limit = 20) {
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
 * POST the plugin's response for a given fs_request back to AWB. Fire-and-log
 * on failure — the server will timeout the pending request and surface a
 * 504 to the web UI, which is already the honest outcome if delivery fails.
 */
export async function postFsResponse(config, requestId, body) {
  if (!requestId) return;
  try {
    const url = `${config.url.replace(/\/$/, '')}/api/fs/responses/${encodeURIComponent(requestId)}`;
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'X-Agent-Key': config.apiKey,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!resp.ok) {
      log(`fs response POST failed: ${resp.status} ${resp.statusText} (request=${requestId})`);
    }
  } catch (err) {
    log(`fs response POST error: ${err.message} (request=${requestId})`);
  }
}
