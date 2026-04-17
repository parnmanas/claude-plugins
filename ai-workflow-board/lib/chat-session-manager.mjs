// ─── Chat Session Manager (v0.7.0 persistent per-room chat subagents) ────

import { promises as fsp } from 'fs';
import { join, dirname } from 'path';
import { createInterface } from 'readline';
import { spawn } from 'child_process';
import { SUBAGENTS_BASE_DIR, STOP_GRACE_MS } from './constants.mjs';
import { log } from './logging.mjs';
import { fetchChatRoomHistory, composeChatRoomPrompt } from './utils.mjs';

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
export class ChatSessionManager {
  #config;
  #sessions = new Map();         // roomId → session
  #historyRing = new Map();      // roomId → ChatRoomMessagePayload[] (max 30)
  #dedupSet = new Set();
  #dedupQueue = [];              // fifo for bounded dedup
  #DEDUP_MAX = 200;
  #HISTORY_MAX = 30;

  constructor(config) {
    this.#config = config;
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
   * spec = { roomId, senderId, senderName, createdAt, content, rolePrompt }
   * Returns { dispatched: boolean, pid?: number, reason?: string, firstTurn?: boolean }.
   */
  async dispatch(spec) {
    if (!spec.roomId) return { dispatched: false, reason: 'no_room' };

    // Dedup: same user message may arrive via both chat_request and chat_room_message
    const dedupKey = `msg:${spec.senderId || ''}:${spec.createdAt || ''}`;
    if (this.#dedupSet.has(dedupKey)) {
      return { dispatched: false, reason: 'duplicate_chat' };
    }
    this.#rememberDedup(dedupKey);

    let sess = this.#sessions.get(spec.roomId);

    if (sess) {
      // Existing live session — just stream another user turn into stdin.
      this.#writeTurn(sess, spec.content || '');
      sess.turnCount++;
      sess.lastTouchedAt = Date.now();
      this.#resetIdleTimer(sess);
      const maxTurns = this.#config.delegation.maxTurnsPerSession ?? 30;
      if (sess.turnCount >= maxTurns) {
        log(`[chat-session] room=${spec.roomId} hit maxTurns=${maxTurns}, closing stdin for respawn`);
        try { sess.child.stdin.end(); } catch { /* already closed */ }
      }
      return { dispatched: true, pid: sess.pid };
    }

    // No live session — need to spawn. Check cap; try LRU-evict oldest idle on overflow.
    const cap = this.#config.delegation.maxConcurrent ?? 5;
    if (this.#sessions.size >= cap) {
      const evicted = this.#evictLru();
      if (!evicted) return { dispatched: false, reason: 'cap_busy' };
    }

    // Bootstrap history: ring first, REST as fallback.
    let history = (this.#historyRing.get(spec.roomId) || []).slice();
    if (history.length === 0) {
      try { history = await fetchChatRoomHistory(this.#config, spec.roomId); } catch { history = []; }
    }
    const firstTurnText = composeChatRoomPrompt(spec.roomId, history, {
      content: spec.content || '',
      sender_name: spec.senderName || '',
      sender_id: spec.senderId || '',
    });

    const spawned = await this.#spawn(spec.roomId, spec.rolePrompt || '', firstTurnText);
    if (!spawned) return { dispatched: false, reason: 'spawn_failed' };
    this.#sessions.set(spec.roomId, spawned);
    return { dispatched: true, pid: spawned.pid, firstTurn: true };
  }

  async #spawn(roomId, rolePrompt, firstTurnText) {
    let configPath = null;
    try {
      configPath = join(
        SUBAGENTS_BASE_DIR,
        `cfg-chat-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
      );
      await fsp.mkdir(dirname(configPath), { recursive: true, mode: 0o700 });
      const mcpConfig = {
        mcpServers: {
          awb: {
            type: 'http',
            url: `${this.#config.url.replace(/\/$/, '')}/mcp`,
            headers: { Authorization: `Bearer ${this.#config.apiKey}` },
          },
        },
      };
      await fsp.writeFile(configPath, JSON.stringify(mcpConfig), { mode: 0o600 });

      const args = [
        '--verbose',
        '--input-format', 'stream-json',
        '--output-format', 'stream-json',
        '--mcp-config', configPath,
        '--strict-mcp-config',
        '--allowedTools', 'mcp__awb__*',
        '--append-system-prompt', rolePrompt || '',
        '--dangerously-skip-permissions',
      ];
      const child = spawn(this.#config.delegation.claudeBin, args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        detached: true,
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
      });
      child.unref();

      if (!child.pid) {
        await fsp.unlink(configPath).catch(() => {});
        return null;
      }

      const sess = {
        roomId,
        pid: child.pid,
        child,
        configPath,
        turnCount: 0,
        startedAt: Date.now(),
        lastTouchedAt: Date.now(),
        idleTimer: null,
      };
      this.#wireStdio(sess);
      this.#wireExit(sess);

      log(`Subagent spawned: pid=${sess.pid} kind=chat_session room=${roomId}`);

      // Seed the first turn — role prompt + history + the current user message.
      this.#writeTurn(sess, firstTurnText);
      sess.turnCount = 1;
      this.#resetIdleTimer(sess);
      return sess;
    } catch (err) {
      log(`[chat-session] spawn error room=${roomId}: ${err.message}`);
      if (configPath) await fsp.unlink(configPath).catch(() => {});
      return null;
    }
  }

  #writeTurn(sess, text) {
    const obj = {
      type: 'user',
      message: { role: 'user', content: [{ type: 'text', text: String(text) }] },
    };
    try {
      sess.child.stdin.write(JSON.stringify(obj) + '\n');
      log(`[chat-session] dispatched turn room=${sess.roomId} pid=${sess.pid} turn=${sess.turnCount + 1} bytes=${Buffer.byteLength(text)}`);
    } catch (err) {
      log(`[chat-session] stdin write failed pid=${sess.pid}: ${err.message}`);
    }
  }

  #wireStdio(sess) {
    if (sess.child.stdout) {
      const rlOut = createInterface({ input: sess.child.stdout });
      rlOut.on('line', (line) => {
        // stream-json: one JSON object per line. Log result events; everything
        // else (assistant/system/tool_use) gets a terse line for debugging.
        try {
          const obj = JSON.parse(line);
          if (obj?.type === 'result') {
            log(`[chat-session:${sess.pid}] result subtype=${obj.subtype || '-'} is_error=${obj.is_error ?? '-'}`);
          } else if (obj?.type) {
            // Keep these quiet — they are frequent and mostly noise in the proxy log.
          }
        } catch {
          // non-JSON (rare); ignore
        }
      });
    }
    if (sess.child.stderr) {
      const rlErr = createInterface({ input: sess.child.stderr });
      rlErr.on('line', (line) => log(`[chat-session:${sess.pid}:err] ${line}`));
    }
  }

  #wireExit(sess) {
    sess.child.once('exit', async (code, signal) => {
      if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
      const durationSec = Math.round((Date.now() - sess.startedAt) / 1000);
      log(`[chat-session] exit pid=${sess.pid} room=${sess.roomId} code=${code} signal=${signal || '-'} turns=${sess.turnCount} duration=${durationSec}s`);
      if (this.#sessions.get(sess.roomId) === sess) this.#sessions.delete(sess.roomId);
      if (sess.configPath) {
        try { await fsp.unlink(sess.configPath); } catch { /* best-effort */ }
      }
    });
    sess.child.once('error', (err) => log(`[chat-session] child error pid=${sess.pid}: ${err.message}`));
  }

  #resetIdleTimer(sess) {
    if (sess.idleTimer) clearTimeout(sess.idleTimer);
    const mins = this.#config.delegation.idleMinutes ?? 10;
    sess.idleTimer = setTimeout(() => {
      log(`[chat-session] idle, closing stdin room=${sess.roomId} pid=${sess.pid}`);
      try { sess.child.stdin.end(); } catch { /* already closed */ }
    }, mins * 60_000);
    if (typeof sess.idleTimer.unref === 'function') sess.idleTimer.unref();
  }

  #evictLru() {
    let oldestKey = null;
    let oldest = Infinity;
    for (const [k, s] of this.#sessions.entries()) {
      if (s.lastTouchedAt < oldest) { oldest = s.lastTouchedAt; oldestKey = k; }
    }
    if (!oldestKey) return false;
    const s = this.#sessions.get(oldestKey);
    log(`[chat-session] evicting lru room=${oldestKey} pid=${s.pid}`);
    if (s.idleTimer) { clearTimeout(s.idleTimer); s.idleTimer = null; }
    try { s.child.stdin.end(); } catch { /* already closed */ }
    this.#sessions.delete(oldestKey);
    return true;
  }

  #rememberDedup(key) {
    this.#dedupSet.add(key);
    this.#dedupQueue.push(key);
    while (this.#dedupQueue.length > this.#DEDUP_MAX) {
      const old = this.#dedupQueue.shift();
      this.#dedupSet.delete(old);
    }
  }

  async stop() {
    const sessions = Array.from(this.#sessions.values());
    for (const sess of sessions) {
      if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
      try { sess.child.stdin.end(); } catch { /* ignore */ }
      try { process.kill(sess.pid, 'SIGTERM'); } catch { /* dead */ }
    }
    if (sessions.length === 0) { this.#sessions.clear(); return; }
    await new Promise((r) => setTimeout(r, STOP_GRACE_MS));
    for (const sess of sessions) {
      try { process.kill(sess.pid, 'SIGKILL'); } catch { /* gone */ }
    }
    this.#sessions.clear();
    log(`ChatSessionManager stopped (terminated ${sessions.length} sessions)`);
  }

  /** Test-only accessor: snapshot of active sessions. */
  _snapshot() {
    return Array.from(this.#sessions.values()).map((s) => ({
      roomId: s.roomId,
      pid: s.pid,
      turnCount: s.turnCount,
      startedAt: s.startedAt,
      lastTouchedAt: s.lastTouchedAt,
    }));
  }
}
