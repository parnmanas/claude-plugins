// ─── Base Session Manager ─────────────────────────────────
// Shared lifecycle skeleton for persistent per-key Claude CLI children.
// ChatSessionManager (key = roomId) and TicketSessionManager (key = ticketId)
// both extend this class.
//
// The base class owns:
//   - the #sessions Map, cap enforcement + LRU eviction
//   - spawn (mcp-config creation, CLI args, child.spawn, wireStdio/wireExit)
//   - writeTurn / resetIdleTimer / maxTurns respawn trigger
//   - bounded dedup set+queue helpers
//   - graceful stop() (SIGTERM → grace → SIGKILL)
//
// Subclasses customise per-kind behaviour via:
//   - constructor options { keyField, logTag, cfgPrefix, kindLabel }
//   - their own dispatch*() public methods that call this.spawnSession()
//     and this.writeTurnToSession() after composing the turn text.
//
// There is no ECMA "protected"; members that subclasses may touch are exposed
// as regular (non-#) methods prefixed with `_` and documented here. Private
// state stays behind `#`.

import { promises as fsp } from 'fs';
import { join, dirname } from 'path';
import { createInterface } from 'readline';
import { spawn } from 'child_process';
import { SUBAGENTS_BASE_DIR, STOP_GRACE_MS } from './constants.mjs';
import { log } from './logging.mjs';
import { resolveClaudeBin } from './claude-bin-resolver.mjs';

export class BaseSessionManager {
  #config;
  #sessions = new Map();           // sessionKey → session record
  #dedupSet = new Set();
  #dedupQueue = [];
  #DEDUP_MAX = 200;

  // Subclass-injected descriptors, set in constructor.
  #keyField;    // e.g. 'roomId' | 'ticketId' — field name on session records
  #logTag;      // e.g. '[chat-session]' | '[ticket-session]' — prefix for internal logs
  #cfgPrefix;   // e.g. 'cfg-chat-' | 'cfg-ticket-' — mcp-config tempfile name prefix
  #kindLabel;   // e.g. 'chat_session' | 'ticket_session' — spawn log kind=

  /**
   * @param {object} config delegation config (config.delegation.*)
   * @param {object} options subclass descriptors
   * @param {string} options.keyField   session record field naming the key ('roomId' | 'ticketId')
   * @param {string} options.logTag     log-line prefix ('[chat-session]' | '[ticket-session]')
   * @param {string} options.cfgPrefix  mcp-config tempfile prefix ('cfg-chat-' | 'cfg-ticket-')
   * @param {string} options.kindLabel  spawn-log kind value ('chat_session' | 'ticket_session')
   */
  // v0.32: monitor injected post-construction so the proxy can wire it after
  // resolving the agent's workspace_id. No-op tap until set.
  #monitor = null;

  constructor(config, options) {
    this.#config = config;
    this.#keyField = options.keyField;
    this.#logTag = options.logTag;
    this.#cfgPrefix = options.cfgPrefix;
    this.#kindLabel = options.kindLabel;
  }

  setMonitor(monitor) { this.#monitor = monitor; }

  // ─── Read-only accessors for subclasses ────────────────────────
  get _config() { return this.#config; }
  get _sessions() { return this.#sessions; }

  /** Return the live session for a key, or undefined. */
  _getSession(sessionKey) {
    return this.#sessions.get(sessionKey);
  }

  /**
   * Ensure there is capacity for a new session. LRU-evict oldest-idle on
   * overflow. Returns true when a new session may be spawned.
   */
  _ensureCapacity() {
    const cap = this.#config.delegation.maxConcurrent ?? 5;
    if (this.#sessions.size < cap) return true;
    return this.#evictLru();
  }

  /**
   * Spawn a new Claude CLI child bound to `sessionKey`. Seeds it with
   * `firstTurnText`. Registers the session on success and returns the record.
   * Returns null on spawn failure (caller decides what to do).
   *
   * `options.onProgress(stage)` is invoked as the subagent advances through
   * the turn:
   *   - 'thinking'  — first stdout line received (subagent has the message)
   *   - 'composing' — first assistant content emitted (writing the reply)
   * It is also re-invoked every 10s with the latest stage so callers can
   * refresh client-side typing indicators that auto-expire.
   */
  async _spawnSession(sessionKey, rolePrompt, firstTurnText, { onProgress } = {}) {
    let configPath = null;
    try {
      configPath = join(
        SUBAGENTS_BASE_DIR,
        `${this.#cfgPrefix}${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
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

      // Pid sidecar — written AFTER spawn so it reflects the real child pid.
      // Used by cleanupOrphanSubagents() on proxy startup to reap survivors
      // of a hard proxy crash (SIGKILL / OS reboot / host process vanish).
      // Without a sidecar we'd have no way to tie a leftover cfg file back
      // to a pid, and `detached: true` + `unref()` means these children
      // survive the proxy's death.
      const pidPath = configPath.replace(/\.json$/, '.pid');

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
      const resolvedBin = resolveClaudeBin(this.#config.delegation.claudeBin);
      const child = spawn(resolvedBin, args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        detached: true,
        // v0.32: Windows console-window spam fix — without this, every spawned
        // claude.exe (and each Bash subprocess it launches) flashes a console
        // window even though stdio is already piped to the proxy.
        windowsHide: true,
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
        // Only .cmd/.bat/.ps1 wrappers need cmd.exe; native .exe spawns direct.
        shell: /\.(cmd|bat|ps1)$/i.test(resolvedBin),
      });
      // CRITICAL: attach 'error' listener synchronously BEFORE the pid check
      // below. spawn() emits 'error' async on ENOENT; without a listener the
      // event becomes an uncaughtException that corrupts proxy state.
      // #wireExit attaches another 'error' handler later but only when the
      // spawn succeeded — this early listener covers the failure branch.
      child.once('error', (err) => {
        log(`${this.#logTag} spawn error: code=${err.code || ''} bin=${resolvedBin} msg=${err.message}`);
      });
      child.unref();

      if (!child.pid) {
        await fsp.unlink(configPath).catch(() => {});
        return null;
      }
      // Best-effort: if this write fails we simply lose the orphan-cleanup
      // safety net for THIS subagent, nothing worse.
      await fsp.writeFile(pidPath, String(child.pid), { mode: 0o600 }).catch(() => {});

      const sess = {
        [this.#keyField]: sessionKey,
        pid: child.pid,
        child,
        configPath,
        pidPath,
        turnCount: 0,
        startedAt: Date.now(),
        lastTouchedAt: Date.now(),
        idleTimer: null,
      };
      // v0.32: register with the subagent monitor (no-op when monitor unset
      // or disabled). Tap stays on the session record so wireStdio/writeTurn/
      // wireExit can route lines through it.
      sess.tap = this.#monitor?.register({
        kind: this.#kindLabel === 'chat_session' ? 'chat' : (this.#kindLabel === 'ticket_session' ? 'ticket' : 'oneshot'),
        sessionKey,
        pid: child.pid,
      }) || null;
      this.#wireStdio(sess);
      this.#wireExit(sess);

      log(`Subagent spawned: pid=${sess.pid} kind=${this.#kindLabel} ${this.#keyField}=${sessionKey}`);

      this.#startTurn(sess, onProgress);
      this._writeTurn(sess, firstTurnText);
      sess.turnCount = 1;
      this._resetIdleTimer(sess);
      this.#sessions.set(sessionKey, sess);
      return sess;
    } catch (err) {
      log(`${this.#logTag} spawn error ${this.#keyField}=${sessionKey}: ${err.message}`);
      if (configPath) await fsp.unlink(configPath).catch(() => {});
      return null;
    }
  }

  /**
   * Stream one user turn into an existing session's stdin, bump bookkeeping
   * counters, and (when checkMaxTurns is true) trigger stdin.end() once the
   * turn budget is exhausted. Callers handle dedup + the kind-specific turn
   * text composition before invoking this.
   *
   * `checkMaxTurns` defaults to true (the common case: fresh user/trigger
   * turn). Pass false for passive notifications (e.g. ticket board_update
   * forwards) that should not cause a respawn.
   */
  _sendFollowUp(sess, turnText, { checkMaxTurns = true, onProgress } = {}) {
    this.#startTurn(sess, onProgress);
    this._writeTurn(sess, turnText);
    sess.turnCount++;
    sess.lastTouchedAt = Date.now();
    this._resetIdleTimer(sess);
    if (!checkMaxTurns) return;
    const maxTurns = this.#config.delegation.maxTurnsPerSession ?? 30;
    if (sess.turnCount >= maxTurns) {
      log(`${this.#logTag} ${this.#keyField}=${sess[this.#keyField]} hit maxTurns=${maxTurns}, closing stdin for respawn`);
      try { sess.child.stdin.end(); } catch { /* already closed */ }
    }
  }

  // ─── Turn progress (drives client typing indicators) ───────────
  // A "turn" is one user message → subagent → response cycle. The base class
  // tracks the in-flight turn so it can fire onProgress callbacks when the
  // subagent acknowledges the input ('thinking') and starts producing
  // assistant content ('composing'). A 10s heartbeat re-fires the latest
  // stage so client-side typing indicators (which self-expire after ~15s)
  // stay visible across long subagent runs.

  #startTurn(sess, onProgress) {
    this.#endTurn(sess);
    if (typeof onProgress !== 'function') return;
    const turn = {
      onProgress,
      stage: null, // 'thinking' | 'composing'
      fired: { thinking: false, composing: false },
      heartbeatTimer: null,
    };
    sess._currentTurn = turn;
    turn.heartbeatTimer = setInterval(() => {
      if (sess._currentTurn === turn && turn.stage) {
        try { turn.onProgress(turn.stage); } catch (err) {
          log(`${this.#logTag} onProgress heartbeat error: ${err.message}`);
        }
      }
    }, 10_000);
    if (typeof turn.heartbeatTimer.unref === 'function') turn.heartbeatTimer.unref();
  }

  #endTurn(sess) {
    const turn = sess._currentTurn;
    if (!turn) return;
    if (turn.heartbeatTimer) clearInterval(turn.heartbeatTimer);
    sess._currentTurn = null;
  }

  #advanceTurn(sess, parsedLine) {
    const turn = sess._currentTurn;
    if (!turn) return;
    if (!turn.fired.thinking) {
      turn.fired.thinking = true;
      turn.stage = 'thinking';
      try { turn.onProgress('thinking'); } catch (err) {
        log(`${this.#logTag} onProgress(thinking) error: ${err.message}`);
      }
    }
    if (!turn.fired.composing && parsedLine?.type === 'assistant') {
      turn.fired.composing = true;
      turn.stage = 'composing';
      try { turn.onProgress('composing'); } catch (err) {
        log(`${this.#logTag} onProgress(composing) error: ${err.message}`);
      }
    }
    if (parsedLine?.type === 'result') {
      // Per-session hook: subclasses use this to ack triggers ONLY when the
      // turn actually completed (so silent / errored exits remain
      // unacknowledged and the poller can retry them).
      try { sess.onResult?.(parsedLine); } catch (err) {
        log(`${this.#logTag} onResult error: ${err.message}`);
      }
      this.#endTurn(sess);
    }
  }

  _writeTurn(sess, text) {
    const obj = {
      type: 'user',
      message: { role: 'user', content: [{ type: 'text', text: String(text) }] },
    };
    const wire = JSON.stringify(obj);
    try {
      sess.child.stdin.write(wire + '\n');
      sess.tap?.inLine(wire);
      log(`${this.#logTag} dispatched turn ${this.#keyField}=${sess[this.#keyField]} pid=${sess.pid} turn=${sess.turnCount + 1} bytes=${Buffer.byteLength(text)}`);
    } catch (err) {
      log(`${this.#logTag} stdin write failed pid=${sess.pid}: ${err.message}`);
    }
  }

  #wireStdio(sess) {
    if (sess.child.stdout) {
      const rlOut = createInterface({ input: sess.child.stdout });
      const tag = this.#logTag.replace(/^\[|\]$/g, '');
      rlOut.on('line', (line) => {
        // stream-json: one JSON object per line. First line of a turn drives
        // the 'thinking' progress fire; first assistant line drives 'composing'.
        sess.tap?.outLine(line);
        let obj = null;
        try { obj = JSON.parse(line); } catch { /* non-JSON; skip */ }
        this.#advanceTurn(sess, obj);
        if (obj?.type === 'result') {
          log(`[${tag}:${sess.pid}] result subtype=${obj.subtype || '-'} is_error=${obj.is_error ?? '-'}`);
        }
      });
    }
    if (sess.child.stderr) {
      const rlErr = createInterface({ input: sess.child.stderr });
      const tag = this.#logTag.replace(/^\[|\]$/g, '');
      rlErr.on('line', (line) => log(`[${tag}:${sess.pid}:err] ${line}`));
    }
  }

  #wireExit(sess) {
    sess.child.once('exit', async (code, signal) => {
      if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
      this.#endTurn(sess);
      const durationSec = Math.round((Date.now() - sess.startedAt) / 1000);
      const key = sess[this.#keyField];
      sess.tap?.end({ exit_code: code, signal });
      log(`${this.#logTag} exit pid=${sess.pid} ${this.#keyField}=${key} code=${code} signal=${signal || '-'} turns=${sess.turnCount} duration=${durationSec}s`);
      if (this.#sessions.get(key) === sess) this.#sessions.delete(key);
      if (sess.configPath) {
        try { await fsp.unlink(sess.configPath); } catch { /* best-effort */ }
      }
      if (sess.pidPath) {
        try { await fsp.unlink(sess.pidPath); } catch { /* best-effort */ }
      }
    });
    sess.child.once('error', (err) => log(`${this.#logTag} child error pid=${sess.pid}: ${err.message}`));
  }

  _resetIdleTimer(sess) {
    if (sess.idleTimer) clearTimeout(sess.idleTimer);
    const mins = this.#config.delegation.idleMinutes ?? 10;
    sess.idleTimer = setTimeout(() => {
      log(`${this.#logTag} idle, closing stdin ${this.#keyField}=${sess[this.#keyField]} pid=${sess.pid}`);
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
    log(`${this.#logTag} evicting lru ${this.#keyField}=${oldestKey} pid=${s.pid}`);
    if (s.idleTimer) { clearTimeout(s.idleTimer); s.idleTimer = null; }
    try { s.child.stdin.end(); } catch { /* already closed */ }
    this.#sessions.delete(oldestKey);
    return true;
  }

  /** Bounded dedup: returns true when key was newly remembered, false if duplicate. */
  _rememberDedup(key) {
    if (this.#dedupSet.has(key)) return false;
    this.#dedupSet.add(key);
    this.#dedupQueue.push(key);
    while (this.#dedupQueue.length > this.#DEDUP_MAX) {
      const old = this.#dedupQueue.shift();
      this.#dedupSet.delete(old);
    }
    return true;
  }

  /**
   * Drop a previously-remembered key so the same trigger/message can be
   * dispatched again. Subclasses call this from session exit handlers — once
   * the subagent for a key is gone, any unacknowledged triggers it carried
   * must become re-dispatchable, otherwise the poller's retry path is dead
   * for that trigger forever.
   */
  _forgetDedup(key) {
    if (!this.#dedupSet.delete(key)) return;
    const idx = this.#dedupQueue.indexOf(key);
    if (idx >= 0) this.#dedupQueue.splice(idx, 1);
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
    log(`${this.constructor.name} stopped (terminated ${sessions.length} sessions)`);
  }
}
