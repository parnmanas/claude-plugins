// ─── Event Log Recorder ───────────────────────────────────
// In-memory ring buffer of SSE events the plugin received. Drained by the
// error-log uploader. Two drain triggers: a 30s periodic tick owned by
// proxy.mjs, and an immediate flush when the buffer crosses FLUSH_THRESHOLD
// (wired via onFlushThreshold). Result: info-level event rows land in the
// admin Agent Logs viewer within seconds of arrival instead of waiting out
// the old 10-minute cadence.
//
// Captures the 5 event types that event-dispatcher.mjs routes:
// agent_trigger, board_update, chat_request, chat_room_message, comment_mention.

const MAX_BUFFER = 500;
const MAX_RAW_LINE = 4000;
const MAX_MESSAGE = 2000;

// When the buffer reaches this many entries, signal callers (via the flush
// callback registered with onThresholdReached) to kick off an out-of-schedule
// upload instead of waiting for the 30s tick. Keeps a moderately busy ticket
// from sitting around for half a minute before the user can see events.
const FLUSH_THRESHOLD = 10;

const buffer = [];
let onThresholdReached = null;

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

/**
 * Register a callback invoked when the buffer first crosses FLUSH_THRESHOLD
 * since the last drain. The callback should start an upload; swallowing any
 * errors internally. Only one callback is kept — last registration wins.
 */
export function onFlushThreshold(cb) {
  onThresholdReached = typeof cb === 'function' ? cb : null;
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

  const prevLen = buffer.length;
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

  // Fire the threshold callback once per crossing. The caller is expected
  // to drain soon, which resets the count. We intentionally don't debounce
  // here — the uploader already handles concurrent invocations by draining
  // atomically, and the 30s tick will pick up anything missed.
  if (prevLen < FLUSH_THRESHOLD && buffer.length >= FLUSH_THRESHOLD && onThresholdReached) {
    try { onThresholdReached(); } catch { /* swallow — uploader errors are its own concern */ }
  }
}

/** Return the buffered entries and clear the buffer. */
export function drainEvents() {
  if (buffer.length === 0) return [];
  const out = buffer.slice();
  buffer.length = 0;
  return out;
}
