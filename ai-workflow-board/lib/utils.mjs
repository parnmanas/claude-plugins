// ─── Delegation utilities ─────────────────────────────────
// Pure helpers (prompt composers + REST fetchers) shared by EventStream and
// the session managers. Kept free of class references so the lib/ modules can
// import these without circular-import risk.

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
export function composeTriggerPrompt(ticket, rolePrompt, ticketPrompt, fallbackTicketId) {
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
export function composeChatPrompt(rolePrompt, history, newMessage) {
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
 * Compose the task text for a chat room message subagent. Pure function.
 * `history` is recent messages (chronological); `newMessage` is the incoming message.
 * role_prompt is injected via --append-system-prompt (not here).
 */
export function composeChatRoomPrompt(roomId, history, newMessage) {
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
