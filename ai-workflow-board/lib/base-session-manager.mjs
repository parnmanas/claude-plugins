// ─── Base Session Manager ─────────────────────────────────
// Shared lifecycle skeleton for persistent per-key CLI children.
// ChatSessionManager (key = roomId) and TicketSessionManager (key = ticketId)
// both extend this class.
//
// Phase 2: parameterized by a CliAdapter. The adapter contributes the bits
// that vary across CLIs (argv shape, stream-json formatting, line parsing).
// Sessions are only available when the adapter declares PERSISTENT_SESSION;
// _spawnSession() refuses to spawn for stateless adapters (gemini, …) so
// the manager can fail fast instead of leaving a half-broken child running.
//
// The base class owns:
//   - the #sessions Map, cap enforcement + LRU eviction
//   - spawn (mcp-config creation, CLI args via adapter, child.spawn, wireStdio/wireExit)
//   - writeTurn / resetIdleTimer / maxTurns respawn trigger
//   - bounded dedup set+queue helpers
//   - graceful stop() (SIGTERM → grace → SIGKILL)
//
// Subclasses customise per-kind behaviour via:
//   - constructor options { keyField, logTag, cfgPrefix, kindLabel }
//   - their own dispatch*() public methods that call this._spawnSession()
//     and this._sendFollowUp() after composing the turn text.

import { promises as fsp } from 'fs';
import { join, dirname } from 'path';
import { createInterface } from 'readline';
import { spawn } from 'child_process';
import { SUBAGENTS_BASE_DIR, STOP_GRACE_MS } from './constants.mjs';
import { log } from './logging.mjs';
import { ClaudeCliAdapter } from './cli-adapters/claude.mjs';
import { ADAPTER_CAPABILITIES, PARSE_STAGE } from './cli-adapters/base.mjs';

const { PERSISTENT_SESSION } = ADAPTER_CAPABILITIES;

// Health watchdog. A session is "responding" when the adapter reports a
// `result` line for each turn we wrote. If the LLM goes silent — Claude
// account hit a rate-limit ceiling, network deadlock, child wedged on a
// blocking syscall, etc. — turns stack on stdin without acks and the
// AWB server keeps re-firing the same trigger forever.
//
// Two thresholds, OR'd together:
//   - 5 turns dispatched without seeing a single `result` line back
//   - 30 minutes elapsed since the first unresponded turn was written
const UNHEALTHY_TURN_THRESHOLD = 5;
const UNHEALTHY_DURATION_MS = 30 * 60 * 1000;
const HEALTH_SWEEP_INTERVAL_MS = 60 * 1000;

export class BaseSessionManager {
  #config;
  #adapter;
  #sessions = new Map();
  #dedupSet = new Set();
  #dedupQueue = [];
  #DEDUP_MAX = 200;
  #healthTimer = null;

  // Subclass-injected descriptors, set in constructor.
  #keyField;
  #logTag;
  #cfgPrefix;
  #kindLabel;

  #monitor = null;

  /**
   * @param {object} config delegation config (config.delegation.*)
   * @param {object} options subclass descriptors
   * @param {string} options.keyField   session record field naming the key ('roomId' | 'ticketId')
   * @param {string} options.logTag     log-line prefix ('[chat-session]' | '[ticket-session]')
   * @param {string} options.cfgPrefix  mcp-config tempfile prefix ('cfg-chat-' | 'cfg-ticket-')
   * @param {string} options.kindLabel  spawn-log kind value ('chat_session' | 'ticket_session')
   * @param {import('./cli-adapters/base.mjs').CliAdapter} [adapter] CLI adapter; default = claude
   */
  constructor(config, options, adapter) {
    this.#config = config;
    this.#keyField = options.keyField;
    this.#logTag = options.logTag;
    this.#cfgPrefix = options.cfgPrefix;
    this.#kindLabel = options.kindLabel;
    this.#adapter = adapter || new ClaudeCliAdapter();
  }

  setMonitor(monitor) { this.#monitor = monitor; }

  // ─── Read-only accessors for subclasses ────────────────────────
  get _config() { return this.#config; }
  get _sessions() { return this.#sessions; }
  get _adapter() { return this.#adapter; }

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
   * Spawn a new persistent CLI child bound to `sessionKey`. Seeds it with
   * `firstTurnText`. Registers the session on success and returns the record.
   * Returns null on spawn failure or when the adapter doesn't support
   * persistent sessions (caller decides what to do — typically fall through
   * to a one-shot SubagentManager.spawn).
   */
  async _spawnSession(sessionKey, rolePrompt, firstTurnText, { onProgress, monitorMeta } = {}) {
    if (!this.#adapter.has(PERSISTENT_SESSION)) {
      log(`${this.#logTag} adapter cli=${this.#adapter.cliType} does not support persistent sessions; refusing to spawn`);
      return null;
    }

    let configPath = null;
    let pidPath = null;
    try {
      // Build the spawn descriptor first so we know whether mcp-config is
      // needed before writing tempfiles. (For claude this is always true;
      // future adapters may opt out.)
      let descriptor = this.#adapter.buildSessionSpawn({
        rolePrompt: rolePrompt || '',
        mcpConfigPath: null,
      });

      if (descriptor.needsMcpConfig) {
        configPath = join(
          SUBAGENTS_BASE_DIR,
          `${this.#cfgPrefix}${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
        );
        await fsp.mkdir(dirname(configPath), { recursive: true, mode: 0o700 });
        // X-AWB-Client-Type: subagent bypasses the server's proxy
        // schemaVersion gate. X-AWB-Subagent-Role / X-AWB-Subagent-Ticket-Id
        // pin the role context for ticket-session subagents.
        const headers = {
          Authorization: `Bearer ${this.#config.apiKey}`,
          'X-AWB-Client-Type': 'subagent',
        };
        if (monitorMeta?.ticket_id) headers['X-AWB-Subagent-Ticket-Id'] = monitorMeta.ticket_id;
        if (monitorMeta?.role) headers['X-AWB-Subagent-Role'] = monitorMeta.role;
        const mcpConfig = {
          mcpServers: {
            awb: {
              type: 'http',
              url: `${this.#config.url.replace(/\/$/, '')}/mcp`,
              headers,
            },
          },
        };
        await fsp.writeFile(configPath, JSON.stringify(mcpConfig), { mode: 0o600 });

        // Re-build with the real path. Adapter buildSessionSpawn is pure.
        descriptor = this.#adapter.buildSessionSpawn({
          rolePrompt: rolePrompt || '',
          mcpConfigPath: configPath,
        });
      }

      const resolvedBin = this.#adapter.resolveBin(this.#config.delegation.claudeBin);
      const child = spawn(resolvedBin, descriptor.args, {
        stdio: descriptor.stdio || ['pipe', 'pipe', 'pipe'],
        detached: true,
        windowsHide: true,
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
        shell: descriptor.shell ?? /\.(cmd|bat|ps1)$/i.test(resolvedBin),
      });
      child.once('error', (err) => {
        log(`${this.#logTag} spawn error: code=${err.code || ''} cli=${this.#adapter.cliType} bin=${resolvedBin} msg=${err.message}`);
      });
      child.unref();

      if (!child.pid) {
        if (configPath) await fsp.unlink(configPath).catch(() => {});
        return null;
      }
      // Pid sidecar — used by orphan-cleanup on restart to reap survivors
      // of a hard crash. Only meaningful when the adapter wrote an mcp-config
      // (the cleanup keys off cfg path); otherwise skip.
      if (configPath) {
        pidPath = configPath.replace(/\.json$/, '.pid');
        await fsp.writeFile(pidPath, String(child.pid), { mode: 0o600 }).catch(() => {});
      }

      const sess = {
        [this.#keyField]: sessionKey,
        pid: child.pid,
        cli_type: this.#adapter.cliType,
        child,
        configPath,
        pidPath,
        turnCount: 0,
        startedAt: Date.now(),
        lastTouchedAt: Date.now(),
        idleTimer: null,
        unrespondedTurnCount: 0,
        unrespondedSince: null,
        unhealthyKilled: false,
      };
      sess.tap = this.#monitor?.register({
        kind: this.#kindLabel === 'chat_session' ? 'chat' : (this.#kindLabel === 'ticket_session' ? 'ticket' : 'oneshot'),
        sessionKey,
        pid: child.pid,
        ticketId: monitorMeta?.ticket_id,
        ticketTitle: monitorMeta?.ticket_title,
        role: monitorMeta?.role,
      }) || null;
      this.#wireStdio(sess);
      this.#wireExit(sess);

      log(`Subagent spawned: pid=${sess.pid} cli=${this.#adapter.cliType} kind=${this.#kindLabel} ${this.#keyField}=${sessionKey}`);

      this.#startTurn(sess, onProgress);
      this._writeTurn(sess, firstTurnText);
      sess.turnCount = 1;
      this._resetIdleTimer(sess);
      this.#sessions.set(sessionKey, sess);
      this.#ensureHealthSweep();
      return sess;
    } catch (err) {
      log(`${this.#logTag} spawn error ${this.#keyField}=${sessionKey}: ${err.message}`);
      if (configPath) await fsp.unlink(configPath).catch(() => {});
      return null;
    }
  }

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

  #advanceTurn(sess, parsed) {
    const turn = sess._currentTurn;
    if (!turn) return;
    if (!turn.fired.thinking && parsed.stage) {
      turn.fired.thinking = true;
      turn.stage = PARSE_STAGE.THINKING;
      try { turn.onProgress(PARSE_STAGE.THINKING); } catch (err) {
        log(`${this.#logTag} onProgress(thinking) error: ${err.message}`);
      }
    }
    if (!turn.fired.composing && parsed.stage === PARSE_STAGE.COMPOSING) {
      turn.fired.composing = true;
      turn.stage = PARSE_STAGE.COMPOSING;
      try { turn.onProgress(PARSE_STAGE.COMPOSING); } catch (err) {
        log(`${this.#logTag} onProgress(composing) error: ${err.message}`);
      }
    }
    if (parsed.isResult) {
      // Health watchdog: a result line means the LLM pipeline answered the
      // turn. Whether the result is success or error doesn't matter here —
      // the child is alive and round-tripping.
      sess.unrespondedTurnCount = 0;
      sess.unrespondedSince = null;
      try { sess.onResult?.(parsed.raw); } catch (err) {
        log(`${this.#logTag} onResult error: ${err.message}`);
      }
      this.#endTurn(sess);
    }
  }

  _writeTurn(sess, text) {
    const wire = this.#adapter.formatTurn(String(text));
    try {
      sess.child.stdin.write(wire + '\n');
      sess.tap?.inLine(wire);
      sess.unrespondedTurnCount = (sess.unrespondedTurnCount || 0) + 1;
      if (!sess.unrespondedSince) sess.unrespondedSince = Date.now();
      log(`${this.#logTag} dispatched turn ${this.#keyField}=${sess[this.#keyField]} pid=${sess.pid} turn=${sess.turnCount + 1} bytes=${Buffer.byteLength(text)}`);
    } catch (err) {
      log(`${this.#logTag} stdin write failed pid=${sess.pid}: ${err.message}`);
      return;
    }
    if (sess.unrespondedTurnCount >= UNHEALTHY_TURN_THRESHOLD) {
      this.#killUnhealthy(
        sess,
        `${sess.unrespondedTurnCount} consecutive turns without an LLM response`,
      );
    }
  }

  #wireStdio(sess) {
    if (sess.child.stdout) {
      const rlOut = createInterface({ input: sess.child.stdout });
      const tag = this.#logTag.replace(/^\[|\]$/g, '');
      rlOut.on('line', (line) => {
        sess.tap?.outLine(line);
        const parsed = this.#adapter.parseStdoutLine(line);
        this.#advanceTurn(sess, parsed);
        if (parsed.isResult) {
          const subtype = parsed.raw?.subtype || '-';
          const isError = parsed.isError === true ? 'true' : (parsed.raw?.is_error ?? '-');
          log(`[${tag}:${sess.pid}] result subtype=${subtype} is_error=${isError}`);
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

  #ensureHealthSweep() {
    if (this.#healthTimer) return;
    this.#healthTimer = setInterval(() => this.#healthSweep(), HEALTH_SWEEP_INTERVAL_MS);
    if (typeof this.#healthTimer.unref === 'function') this.#healthTimer.unref();
  }

  #healthSweep() {
    const now = Date.now();
    for (const sess of this.#sessions.values()) {
      if (sess.unhealthyKilled) continue;
      if (!sess.unrespondedSince) continue;
      const elapsed = now - sess.unrespondedSince;
      if (elapsed >= UNHEALTHY_DURATION_MS) {
        this.#killUnhealthy(
          sess,
          `${Math.round(elapsed / 60_000)}m elapsed without an LLM response`,
        );
      }
    }
  }

  #killUnhealthy(sess, reason) {
    if (sess.unhealthyKilled) return;
    sess.unhealthyKilled = true;
    const key = sess[this.#keyField];
    log(`${this.#logTag} UNHEALTHY ${this.#keyField}=${key} pid=${sess.pid} — ${reason}; killing for respawn`);
    if (this.#sessions.get(key) === sess) this.#sessions.delete(key);
    if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
    try { sess.child.stdin.end(); } catch { /* already closed */ }
    try { process.kill(sess.pid, 'SIGTERM'); } catch { /* already dead */ }
    setTimeout(() => {
      try { process.kill(sess.pid, 'SIGKILL'); } catch { /* gone */ }
    }, STOP_GRACE_MS);
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

  _forgetDedup(key) {
    if (!this.#dedupSet.delete(key)) return;
    const idx = this.#dedupQueue.indexOf(key);
    if (idx >= 0) this.#dedupQueue.splice(idx, 1);
  }

  async stop() {
    if (this.#healthTimer) { clearInterval(this.#healthTimer); this.#healthTimer = null; }
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
