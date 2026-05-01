// ─── Chat Session Manager (v0.7.0 persistent per-room chat subagents) ────

import { BaseSessionManager } from './base-session-manager.mjs';
import { fetchChatRoomHistory } from './rest.mjs';
import { composeChatRoomPrompt } from './prompts.mjs';

/**
 * Keeps one Claude CLI child alive per chat room so that successive messages
 * reuse the same KV cache instead of paying cold-start + MCP handshake per turn.
 *
 * The child is launched with `--input-format stream-json --output-format stream-json`.
 * First turn text is composed with composeChatRoomPrompt() and seeded with recent
 * room history (from the proxy's in-memory ring buffer, fallback: REST history).
 * Subsequent turns are lean user-turn NDJSON lines written to the child's stdin.
 *
 * Lifecycle bounds keep the memory footprint finite:
 *   - IDLE_TTL (idleMinutes):         no traffic → stdin.end() → child exits
 *   - MAX_TURNS (maxTurnsPerSession): respawn on next message (fresh context)
 *   - CAP (maxConcurrent):            LRU-evict oldest-idle before spawn
 *
 * Dedup: `msg:${sender_id}:${created_at}` collides for chat_request and
 * chat_room_message covering the same savedMessage, so each user turn dispatches
 * exactly once even though two SSE events arrive.
 */
export class ChatSessionManager extends BaseSessionManager {
  #historyRing = new Map();      // roomId → ChatRoomMessagePayload[] (max 30)
  #HISTORY_MAX = 30;

  constructor(config, adapter) {
    super(config, {
      keyField: 'roomId',
      logTag: '[chat-session]',
      cfgPrefix: 'cfg-chat-',
      kindLabel: 'chat_session',
    }, adapter);
  }

  /** Called from SSE reader for every chat_room_message we see — warms the ring. */
  recordRoomMessage(payload) {
    const rid = payload?.room_id;
    if (!rid) return;
    let buf = this.#historyRing.get(rid);
    if (!buf) { buf = []; this.#historyRing.set(rid, buf); }
    buf.push({
      sender_type: payload.sender_type,
      sender_id: payload.sender_id,
      sender_name: payload.sender_name,
      content: payload.content,
      created_at: payload.created_at,
    });
    while (buf.length > this.#HISTORY_MAX) buf.shift();
  }

  /**
   * Dispatch a user turn into the room's live session, spawning one if needed.
   * spec = { roomId, senderId, senderName, createdAt, content, rolePrompt, onProgress? }
   * `onProgress(stage)` is forwarded to the base manager and fires as the
   * subagent progresses through the turn ('thinking' → 'composing').
   * Returns { dispatched: boolean, pid?: number, reason?: string, firstTurn?: boolean }.
   */
  async dispatch(spec) {
    if (!spec.roomId) return { dispatched: false, reason: 'no_room' };

    // Dedup: same user message may arrive via both chat_request and chat_room_message
    const dedupKey = `msg:${spec.senderId || ''}:${spec.createdAt || ''}`;
    if (!this._rememberDedup(dedupKey)) {
      return { dispatched: false, reason: 'duplicate_chat' };
    }

    const sess = this._getSession(spec.roomId);

    if (sess) {
      // Existing live session — just stream another user turn into stdin.
      this._sendFollowUp(sess, spec.content || '', { onProgress: spec.onProgress });
      return { dispatched: true, pid: sess.pid };
    }

    // No live session — need to spawn. Check cap; try LRU-evict oldest idle on overflow.
    if (!this._ensureCapacity()) {
      return { dispatched: false, reason: 'cap_busy' };
    }

    // Bootstrap history: ring first, REST as fallback.
    let history = (this.#historyRing.get(spec.roomId) || []).slice();
    if (history.length === 0) {
      try { history = await fetchChatRoomHistory(this._config, spec.roomId); } catch { history = []; }
    }
    const firstTurnText = composeChatRoomPrompt(spec.roomId, history, {
      content: spec.content || '',
      sender_name: spec.senderName || '',
      sender_id: spec.senderId || '',
    });

    const spawned = await this._spawnSession(spec.roomId, spec.rolePrompt || '', firstTurnText, { onProgress: spec.onProgress });
    if (!spawned) return { dispatched: false, reason: 'spawn_failed' };
    return { dispatched: true, pid: spawned.pid, firstTurn: true };
  }

  /** Test-only accessor: snapshot of active sessions. */
  _snapshot() {
    return Array.from(this._sessions.values()).map((s) => ({
      roomId: s.roomId,
      pid: s.pid,
      turnCount: s.turnCount,
      startedAt: s.startedAt,
      lastTouchedAt: s.lastTouchedAt,
    }));
  }
}

