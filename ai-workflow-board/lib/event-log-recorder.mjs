// ─── Event Log Recorder ───────────────────────────────────
// In-memory ring buffer of SSE events the plugin received. Drained by the
// error-log uploader so the same 10-minute batch POST carries both error and
// info-level event rows to /api/agent/error-logs, showing up in the admin
// Agent Logs viewer alongside crash/sse/etc. entries.
//
// Captures the 5 event types that event-dispatcher.mjs routes:
// agent_trigger, board_update, chat_request, chat_room_message, comment_mention.

const MAX_BUFFER = 500;
const MAX_RAW_LINE = 4000;
const MAX_MESSAGE = 2000;

const buffer = [];

function summarize(eventType, ev) {
  switch (eventType) {
    case 'agent_trigger':
      return `[AWB Trigger] ticket=${ev.ticket_id || ''} role=${ev.action || ''} trigger=${ev.field_changed || ''} target=${ev.actor_name || ''}`;
    case 'board_update':
      return `[AWB Update] ticket=${ev.ticket_id || ''} ${ev.entity_type || ''}.${ev.action || ''}${ev.field_changed ? ` field=${ev.field_changed}` : ''} by=${ev.actor_name || ''}`;
    case 'chat_request': {
      const p = ev.payload || {};
      const snippet = (p.new_message || '').slice(0, 120);
      return `[AWB Chat Request] room=${p.room_id || ''} user=${p.user_id || ''} agent=${p.agent_id || ''}: "${snippet}"`;
    }
    case 'chat_room_message': {
      const p = ev.payload || ev;
      const snippet = (p.content || '').slice(0, 120);
      return `[AWB Chat] room=${p.room_id || ''} from=${p.sender_name || p.sender_id || ''} (${p.sender_type || ''}): "${snippet}"`;
    }
    case 'comment_mention': {
      const snippet = (ev.content || '').slice(0, 120);
      return `[AWB Mention] ticket=${ev.ticket_id || ''} comment=${ev.comment_id || ev.field_changed || ''} by=${ev.actor_name || ''}: "${snippet}"`;
    }
    default:
      return `[AWB Event:${eventType}] ticket=${ev.ticket_id || ''}`;
  }
}

/** Record a received SSE event. `raw` is the undecoded JSON data line. */
export function recordEvent(eventType, raw) {
  if (!eventType || !raw) return;
  let ev;
  try { ev = JSON.parse(raw); }
  catch { ev = { _parse_error: true }; }

  const occurredAt = typeof ev.timestamp === 'string' && ev.timestamp
    ? ev.timestamp
    : new Date().toISOString();

  buffer.push({
    occurred_at: occurredAt,
    level: 'info',
    category: eventType,
    message: summarize(eventType, ev).slice(0, MAX_MESSAGE),
    raw_line: raw.slice(0, MAX_RAW_LINE),
    pid: String(process.pid),
  });

  // Drop oldest when oversized — drained on next upload tick anyway.
  if (buffer.length > MAX_BUFFER) buffer.splice(0, buffer.length - MAX_BUFFER);
}

/** Return the buffered entries and clear the buffer. */
export function drainEvents() {
  if (buffer.length === 0) return [];
  const out = buffer.slice();
  buffer.length = 0;
  return out;
}
