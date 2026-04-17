// ─── Ticket Session Manager (v0.8.0 persistent per-ticket subagents) ────

import { promises as fsp } from 'fs';
import { join, dirname } from 'path';
import { createInterface } from 'readline';
import { spawn } from 'child_process';
import { SUBAGENTS_BASE_DIR, STOP_GRACE_MS } from './constants.mjs';
import { log } from './logging.mjs';
import { composeTriggerPrompt } from './prompts.mjs';

/**
 * Keeps one Claude CLI child alive per ticket so that successive events
 * (trigger, board_update, comments) reuse the same KV cache and context.
 *
 * Mirrors ChatSessionManager's lifecycle model:
 *   - IDLE_TTL:   no events → stdin.end() → child exits
 *   - MAX_TURNS:  respawn on next event (fresh context)
 *   - CAP:        LRU-evict oldest-idle before spawn
 *
 * Trigger events spawn a new session with full ticket context.
 * Board_update events are forwarded to existing sessions as follow-up turns.
 */
export class TicketSessionManager {
  #config;
  #sessions = new Map();         // ticketId → session
  #dedupSet = new Set();
  #dedupQueue = [];
  #DEDUP_MAX = 200;

  constructor(config) {
    this.#config = config;
  }

  /**
   * Dispatch a trigger into the ticket's live session, spawning one if needed.
   * spec = { ticketId, triggerId, agentId, rolePrompt, ticketPrompt, ticket }
   * Returns { dispatched: boolean, pid?: number, reason?: string, firstTurn?: boolean }
   */
  async dispatchTrigger(spec) {
    if (!spec.ticketId) return { dispatched: false, reason: 'no_ticket' };

    // Dedup by triggerId
    if (spec.triggerId) {
      const dedupKey = `trigger:${spec.triggerId}`;
      if (this.#dedupSet.has(dedupKey)) {
        return { dispatched: false, reason: 'duplicate_trigger' };
      }
      this.#rememberDedup(dedupKey);
    }

    let sess = this.#sessions.get(spec.ticketId);

    if (sess) {
      // Existing live session — send the trigger as a follow-up turn
      const turnText = this.#composeTriggerTurn(spec);
      this.#writeTurn(sess, turnText);
      sess.turnCount++;
      sess.lastTouchedAt = Date.now();
      this.#resetIdleTimer(sess);
      const maxTurns = this.#config.delegation.maxTurnsPerSession ?? 30;
      if (sess.turnCount >= maxTurns) {
        log(`[ticket-session] ticket=${spec.ticketId} hit maxTurns=${maxTurns}, closing for respawn`);
        try { sess.child.stdin.end(); } catch { /* already closed */ }
      }
      return { dispatched: true, pid: sess.pid };
    }

    // No live session — spawn. Check cap; LRU-evict on overflow.
    const cap = this.#config.delegation.maxConcurrent ?? 5;
    if (this.#sessions.size >= cap) {
      const evicted = this.#evictLru();
      if (!evicted) return { dispatched: false, reason: 'cap_busy' };
    }

    const firstTurnText = composeTriggerPrompt(
      spec.ticket, spec.rolePrompt || '', spec.ticketPrompt || '', spec.ticketId,
    );
    const spawned = await this.#spawn(spec.ticketId, spec.rolePrompt || '', firstTurnText);
    if (!spawned) return { dispatched: false, reason: 'spawn_failed' };
    this.#sessions.set(spec.ticketId, spawned);
    return { dispatched: true, pid: spawned.pid, firstTurn: true };
  }

  /**
   * Forward a board_update event to an existing ticket session.
   * Returns true if delivered, false if no live session exists for this ticket.
   */
  forwardBoardUpdate(ticketId, ev) {
    const sess = this.#sessions.get(ticketId);
    if (!sess) return false;

    const lines = [];
    lines.push(`[Board Update] The ticket you are working on was updated:`);
    lines.push(`  Event: ${ev.entity_type || 'unknown'}.${ev.action || 'unknown'}`);
    if (ev.field_changed) lines.push(`  Field changed: ${ev.field_changed}`);
    if (ev.actor_name) lines.push(`  By: ${ev.actor_name}`);
    lines.push('');
    lines.push('Review the change and adjust your work if needed. Use mcp__awb__get_ticket to fetch the latest ticket state.');

    this.#writeTurn(sess, lines.join('\n'));
    sess.turnCount++;
    sess.lastTouchedAt = Date.now();
    this.#resetIdleTimer(sess);
    return true;
  }

  #composeTriggerTurn(spec) {
    const lines = [];
    lines.push(`[New Trigger] A new trigger arrived for the ticket you are already working on.`);
    if (spec.ticketPrompt) {
      lines.push('');
      lines.push('Updated instructions:');
      lines.push(spec.ticketPrompt);
    }
    lines.push('');
    lines.push('Use mcp__awb__get_ticket to fetch the latest ticket state and continue your work.');
    return lines.join('\n');
  }

  async #spawn(ticketId, rolePrompt, firstTurnText) {
    let configPath = null;
    try {
      configPath = join(
        SUBAGENTS_BASE_DIR,
        `cfg-ticket-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
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
        ticketId,
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

      log(`Subagent spawned: pid=${sess.pid} kind=ticket_session ticket=${ticketId}`);

      this.#writeTurn(sess, firstTurnText);
      sess.turnCount = 1;
      this.#resetIdleTimer(sess);
      return sess;
    } catch (err) {
      log(`[ticket-session] spawn error ticket=${ticketId}: ${err.message}`);
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
      log(`[ticket-session] dispatched turn ticket=${sess.ticketId} pid=${sess.pid} turn=${sess.turnCount + 1} bytes=${Buffer.byteLength(text)}`);
    } catch (err) {
      log(`[ticket-session] stdin write failed pid=${sess.pid}: ${err.message}`);
    }
  }

  #wireStdio(sess) {
    if (sess.child.stdout) {
      const rlOut = createInterface({ input: sess.child.stdout });
      rlOut.on('line', (line) => {
        try {
          const obj = JSON.parse(line);
          if (obj?.type === 'result') {
            log(`[ticket-session:${sess.pid}] result subtype=${obj.subtype || '-'} is_error=${obj.is_error ?? '-'}`);
          }
        } catch { /* non-JSON; ignore */ }
      });
    }
    if (sess.child.stderr) {
      const rlErr = createInterface({ input: sess.child.stderr });
      rlErr.on('line', (line) => log(`[ticket-session:${sess.pid}:err] ${line}`));
    }
  }

  #wireExit(sess) {
    sess.child.once('exit', async (code, signal) => {
      if (sess.idleTimer) { clearTimeout(sess.idleTimer); sess.idleTimer = null; }
      const durationSec = Math.round((Date.now() - sess.startedAt) / 1000);
      log(`[ticket-session] exit pid=${sess.pid} ticket=${sess.ticketId} code=${code} signal=${signal || '-'} turns=${sess.turnCount} duration=${durationSec}s`);
      if (this.#sessions.get(sess.ticketId) === sess) this.#sessions.delete(sess.ticketId);
      if (sess.configPath) {
        try { await fsp.unlink(sess.configPath); } catch { /* best-effort */ }
      }
    });
    sess.child.once('error', (err) => log(`[ticket-session] child error pid=${sess.pid}: ${err.message}`));
  }

  #resetIdleTimer(sess) {
    if (sess.idleTimer) clearTimeout(sess.idleTimer);
    const mins = this.#config.delegation.idleMinutes ?? 10;
    sess.idleTimer = setTimeout(() => {
      log(`[ticket-session] idle, closing stdin ticket=${sess.ticketId} pid=${sess.pid}`);
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
    log(`[ticket-session] evicting lru ticket=${oldestKey} pid=${s.pid}`);
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
    log(`TicketSessionManager stopped (terminated ${sessions.length} sessions)`);
  }

  /** Test-only accessor: snapshot of active sessions. */
  _snapshot() {
    return Array.from(this.#sessions.values()).map((s) => ({
      ticketId: s.ticketId,
      pid: s.pid,
      turnCount: s.turnCount,
      startedAt: s.startedAt,
      lastTouchedAt: s.lastTouchedAt,
    }));
  }
}
