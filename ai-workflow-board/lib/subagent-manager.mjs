// ─── Subagent Manager (Phase 4 D-55..D-75) ────────────────

import { promises as fsp } from 'fs';
import { join, dirname } from 'path';
import { createInterface } from 'readline';
import { spawn } from 'child_process';
import {
  SUBAGENTS_BASE_DIR,
  SUBAGENTS_PERSIST_PATH,
  TTL_SWEEP_INTERVAL_MS,
  SIGTERM_GRACE_MS,
  STOP_GRACE_MS,
} from './constants.mjs';
import { log } from './logging.mjs';
import { resolveClaudeBin } from './claude-bin-resolver.mjs';

/**
 * Owns the lifecycle of Claude CLI subagent child processes.
 *
 * Consumers (Plan 04-03 #handleTrigger and #handleChatRequest) call spawn(spec)
 * with a composed task prompt; the manager handles MCP config file writing, process
 * spawning, PID tracking, stdout/stderr capture, TTL enforcement, persistence, and
 * exit-driven cleanup + completion notification.
 *
 * This class is behaviorally inert until Plan 04-03 wires consumers — Plan 04-02
 * ships it with no caller, only a node --test suite that injects a fake claudeBin.
 *
 * Pitfalls guarded:
 *  - Pitfall 1: MCP config file uses {"mcpServers": {...}} wrapper shape
 *  - Pitfall 3: SIGTERM/SIGINT/exit handlers clean up children on proxy shutdown
 *  - Pitfall 4: Concurrency cap reserves slot synchronously before async spawn
 *  - Pitfall 6: stdio: ['ignore', 'pipe', 'pipe'] closes child stdin
 *  - Pitfall 7: #persist() strips process_handle before JSON.stringify
 */
export class SubagentManager {
  #map = new Map();              // pid → SubagentRecord, AND reservationId → {kind: 'reservation', ...}
  #config;
  #sweepTimer = null;
  #reservationCounter = 0;
  #persistPath;
  #pidDir;
  #initialized = false;
  #monitor = null;               // v0.32: injected by proxy.mjs after agent_id resolution

  constructor(config) {
    this.#config = config;
    this.#persistPath = SUBAGENTS_PERSIST_PATH;
    this.#pidDir = SUBAGENTS_BASE_DIR;
  }

  setMonitor(monitor) { this.#monitor = monitor; }

  /** Idempotent init: create dirs, reconcile persisted records, start TTL sweep. */
  async init() {
    if (this.#initialized) return;
    this.#initialized = true;
    try {
      // mode 0700 so only the user can read config files containing the Bearer token
      await fsp.mkdir(this.#pidDir, { recursive: true, mode: 0o700 });
    } catch (err) {
      log(`SubagentManager: mkdir failed: ${err.message}`);
    }
    await this.#reconcileOnStart();
    await this.#sweepOrphanCfgs();
    this.#sweepTimer = setInterval(() => this.#sweep(), TTL_SWEEP_INTERVAL_MS);
    // Prevent the sweep timer from keeping the event loop alive past proxy shutdown
    if (typeof this.#sweepTimer.unref === 'function') this.#sweepTimer.unref();
    log(`SubagentManager initialized (pidDir=${this.#pidDir}, cap=${this.#config.delegation.maxConcurrent}, ttl=${this.#config.delegation.ttlMinutes}min)`);
  }

  /**
   * Delete cfg-*.json files left behind by dead proxies. v0.24.2: before this
   * sweep the orphan-subagent design (v0.6.7–v0.24.1) guaranteed that every
   * proxy that died while children ran left their MCP config files around,
   * and they accumulated without bound (1000+ files per day observed).
   *
   * A cfg file is safe to delete when either:
   *   - the pid whose spawn created it is no longer alive, OR
   *   - /proc scan finds no running process with that cfg path on argv
   *
   * We intentionally sweep both SubagentManager-owned (cfg-…) and
   * BaseSessionManager-owned (cfg-ticket-… / cfg-chat-…) files since they
   * share the same directory and the session managers keep no persistent
   * state across proxy restarts.
   */
  async #sweepOrphanCfgs() {
    let files;
    try {
      files = await fsp.readdir(this.#pidDir);
    } catch (err) {
      log(`Orphan cfg sweep: readdir failed: ${err.message}`);
      return;
    }

    // Build set of cfg paths still in use by live processes on this host.
    const liveCfgs = new Set();
    // (1) Records we just reconciled — their pids are alive, keep their cfgs.
    for (const rec of this.#map.values()) {
      if (rec.kind !== 'reservation' && rec.config_path) liveCfgs.add(rec.config_path);
    }
    // (2) /proc scan — covers orphans from other proxy generations whose
    // persist entry was overwritten but whose child is still alive.
    try {
      const procEntries = await fsp.readdir('/proc');
      for (const entry of procEntries) {
        if (!/^\d+$/.test(entry)) continue;
        try {
          const cmdline = await fsp.readFile(`/proc/${entry}/cmdline`, 'utf8');
          const parts = cmdline.split('\0');
          const idx = parts.indexOf('--mcp-config');
          if (idx >= 0 && parts[idx + 1]) liveCfgs.add(parts[idx + 1]);
        } catch { /* process vanished mid-scan; ignore */ }
      }
    } catch { /* /proc missing (non-Linux) — rely on persist-reconciliation only */ }

    let purged = 0;
    for (const f of files) {
      if (!f.startsWith('cfg-') || !f.endsWith('.json')) continue;
      const path = join(this.#pidDir, f);
      if (liveCfgs.has(path)) continue;
      try {
        await fsp.unlink(path);
        purged++;
      } catch { /* vanished; ignore */ }
    }
    if (purged > 0) log(`Orphan cfg sweep: purged ${purged} stale config file(s)`);
  }

  /** Count non-reservation records. True if room exists under maxConcurrent. */
  canSpawn() {
    const active = this.#activeCount();
    return active < this.#config.delegation.maxConcurrent;
  }

  #activeCount() {
    let n = 0;
    for (const rec of this.#map.values()) {
      if (rec.kind !== 'reservation') n++;
      else n++; // reservations also consume a slot to close the check-then-act race
    }
    return n;
  }

  /**
   * Spawn a subagent. spec = {
   *   kind: 'trigger' | 'chat',
   *   taskText: string,              // positional prompt passed as the last argv
   *   rolePrompt: string,            // injected via --append-system-prompt
   *   triggerId?: string,            // dedup key for trigger kind
   *   chatRequestId?: string,        // dedup key for chat kind
   *   ticketId?: string,
   *   agentId?: string,
   * }
   * Returns { spawned: boolean, pid?: number, reason?: string }
   */
  async spawn(spec) {
    // Dedup by trigger_id (Pitfall 5: SSE reconnect duplicate trigger)
    if (spec.triggerId) {
      for (const rec of this.#map.values()) {
        if (rec.kind !== 'reservation' && rec.trigger_id === spec.triggerId) {
          return { spawned: false, reason: 'duplicate_trigger' };
        }
      }
    }

    // Strategy B fix: dedup by chatRequestId — prevents double-spawn when chat_request and
    // chat_room_message both arrive for the same user message and both attempt delegation.
    // Uses a separate check from triggerId to avoid namespace collisions between the two dedup keys.
    if (spec.chatRequestId) {
      for (const rec of this.#map.values()) {
        if (rec.kind !== 'reservation' && rec.chat_request_id === spec.chatRequestId) {
          return { spawned: false, reason: 'duplicate_chat' };
        }
      }
    }

    // Concurrency cap check — synchronous reservation closes Pitfall 4 race
    if (!this.canSpawn()) {
      return { spawned: false, reason: 'cap_reached' };
    }
    const reservationId = -(++this.#reservationCounter);
    this.#map.set(reservationId, { kind: 'reservation', started_at: Date.now() });

    let configPath = null;
    try {
      // Write per-subagent MCP config file. Pitfall 7 (v0.6.11): the child Claude CLI
      // reads --mcp-config lazily after spawn returns. We must NOT rename the file
      // after spawn — the child will fail with "MCP config file not found". Keep the
      // file at its original path for the child's entire lifetime; cleanup on exit.
      configPath = join(
        this.#pidDir,
        `cfg-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
      );
      await fsp.mkdir(dirname(configPath), { recursive: true, mode: 0o700 });

      // CRITICAL (Pitfall 1): wrapper shape. Bare {serverName: {...}} is REJECTED.
      // X-AWB-Client-Type: subagent signals to the server that this session is a
      // subagent spawning directly via Claude CLI (not through proxy.mjs), so the
      // server's proxy schemaVersion gate should be skipped. Without this header,
      // the server rejects initialize with -32000 "proxy schemaVersion mismatch"
      // and no mcp__awb__* tools register.
      const mcpConfig = {
        mcpServers: {
          awb: {
            type: 'http',
            url: `${this.#config.url.replace(/\/$/, '')}/mcp`,
            headers: {
              Authorization: `Bearer ${this.#config.apiKey}`,
              'X-AWB-Client-Type': 'subagent',
            },
          },
        },
      };
      await fsp.writeFile(configPath, JSON.stringify(mcpConfig), { mode: 0o600 });

      // Spawn. Pitfall 6: stdio[0]='ignore' closes child stdin.
      // All argv values are separate array elements — never shell-interpolated.
      const args = [
        '--print',
        '--output-format', 'json',
        '--mcp-config', configPath,
        '--strict-mcp-config',
        '--allowedTools', 'mcp__awb__*',
        '--append-system-prompt', spec.rolePrompt || '',
        '--dangerously-skip-permissions',
        spec.taskText,
      ];
      // detached:true puts the child in its own process group so signals aimed
      // at the proxy's pgrp (SIGHUP from a closing terminal, SIGINT from Ctrl+C
      // in the parent Claude CLI) don't cascade and kill in-flight work.
      // TTL sweep and explicit SIGTERM/SIGKILL by pid still work.
      const resolvedBin = resolveClaudeBin(this.#config.delegation.claudeBin);
      const child = spawn(resolvedBin, args, {
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: true,
        // v0.32: hide console windows on Windows; without this every
        // claude.exe + its Bash subprocesses flash a console.
        windowsHide: true,
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
        // Only .cmd/.bat/.ps1 wrappers need cmd.exe (shell:true). Native .exe
        // files spawn directly, avoiding Windows arg-escaping surprises from
        // shell-parsed quotes/ampersands in spec.taskText.
        shell: /\.(cmd|bat|ps1)$/i.test(resolvedBin),
      });
      // CRITICAL: attach the 'error' listener synchronously BEFORE any early
      // return. spawn() emits 'error' async on ENOENT etc.; without a
      // listener the event becomes an uncaughtException that corrupts proxy
      // state. Attaching here (not inside #wireExitHandler, which is skipped
      // when pid is undefined) guarantees coverage for failed spawns.
      child.once('error', (err) => {
        log(`Subagent spawn error: code=${err.code || ''} bin=${resolvedBin} msg=${err.message}`);
      });
      child.unref();

      const pid = child.pid;
      if (!pid) {
        await fsp.unlink(configPath).catch(() => {});
        this.#map.delete(reservationId);
        return { spawned: false, reason: 'spawn_failed' };
      }

      const record = {
        pid,
        kind: spec.kind,
        trigger_id: spec.triggerId || null,
        chat_request_id: spec.chatRequestId || null,
        ticket_id: spec.ticketId || null,
        agent_id: spec.agentId || null,
        started_at: Date.now(),
        expected_completion_at: Date.now() + this.#config.delegation.ttlMinutes * 60_000,
        config_path: configPath,
        process_handle: child,
      };
      // v0.32 monitor tap (no-op when monitor unset). One-shots use 'oneshot'
      // kind; session_key encodes the dispatch identity for the UI.
      record.tap = this.#monitor?.register({
        kind: 'oneshot',
        sessionKey: spec.triggerId
          ? `oneshot:trigger:${spec.triggerId}`
          : (spec.chatRequestId ? `oneshot:chat:${spec.chatRequestId}` : `oneshot:${pid}`),
        pid,
      }) || null;
      this.#map.delete(reservationId);
      this.#map.set(pid, record);
      this.#persist();

      this.#wireExitHandler(child, pid);
      this.#wireStdioCapture(child, pid);

      log(`Subagent spawned: pid=${pid} kind=${spec.kind} ticket=${spec.ticketId || '-'}`);
      return { spawned: true, pid };
    } catch (err) {
      this.#map.delete(reservationId);
      if (configPath) {
        await fsp.unlink(configPath).catch(() => {});
      }
      log(`Subagent spawn error: ${err.message}`);
      return { spawned: false, reason: 'exception' };
    }
  }

  #wireExitHandler(child, pid) {
    child.once('exit', async (code, signal) => {
      const record = this.#map.get(pid);
      if (!record) return; // already cleaned by sweep
      const durationSec = Math.round((Date.now() - record.started_at) / 1000);
      this.#map.delete(pid);
      this.#persist();
      try {
        await fsp.unlink(record.config_path);
      } catch { /* best-effort */ }
      record.tap?.end({ exit_code: code, signal });
      log(`Subagent exit: pid=${pid} kind=${record.kind} code=${code} signal=${signal || '-'} duration=${durationSec}s`);
      // Expose hook for Plan 04-03 test spy — stored on the instance for test visibility
      if (typeof this.onExit === 'function') {
        try { this.onExit({ pid, record, code, signal, durationSec }); } catch { /* ignore */ }
      }
    });
    child.once('error', (err) => {
      log(`Subagent spawn error pid=${pid}: ${err.message}`);
    });
  }

  #wireStdioCapture(child, pid) {
    if (child.stdout) {
      const rlOut = createInterface({ input: child.stdout });
      rlOut.on('line', (line) => {
        const record = this.#map.get(pid);
        record?.tap?.outLine(line);
        log(`[subagent:${pid}] ${line}`);
      });
    }
    if (child.stderr) {
      const rlErr = createInterface({ input: child.stderr });
      rlErr.on('line', (line) => log(`[subagent:${pid}:err] ${line}`));
    }
  }

  #sweep() {
    const now = Date.now();
    for (const [pid, record] of this.#map.entries()) {
      if (record.kind === 'reservation') continue;
      // Existence check — process may have died without our exit handler firing
      try {
        process.kill(pid, 0);
      } catch (err) {
        if (err.code === 'ESRCH' || err.code === 'EPERM') {
          log(`Sweep: pid=${pid} no longer alive, removing record`);
          this.#map.delete(pid);
          fsp.rm(dirname(record.config_path), { recursive: true, force: true }).catch(() => {});
          continue;
        }
      }
      // TTL enforcement
      if (now >= record.expected_completion_at) {
        log(`Sweep: pid=${pid} exceeded TTL, sending SIGTERM`);
        try { process.kill(pid, 'SIGTERM'); } catch { /* already dead */ }
        setTimeout(() => {
          try {
            process.kill(pid, 0);
            log(`Sweep: pid=${pid} still alive after SIGTERM grace, sending SIGKILL`);
            try { process.kill(pid, 'SIGKILL'); } catch { /* ignore */ }
          } catch { /* already exited */ }
        }, SIGTERM_GRACE_MS);
      }
    }
    this.#persist();
  }

  async #reconcileOnStart() {
    let raw;
    try {
      raw = await fsp.readFile(this.#persistPath, 'utf8');
    } catch { return; }
    let persisted;
    try {
      persisted = JSON.parse(raw).pids || [];
    } catch { return; }

    let revived = 0, dropped = 0;
    for (const rec of persisted) {
      if (!rec || !rec.pid) continue;
      try {
        process.kill(rec.pid, 0);
        // Alive — revive record. process_handle is null because we didn't spawn it this session.
        this.#map.set(rec.pid, { ...rec, process_handle: null });
        revived++;
      } catch (err) {
        if (err.code === 'ESRCH' || err.code === 'EPERM') dropped++;
      }
    }
    if (revived || dropped) {
      log(`SubagentManager reconciled: revived=${revived} dropped=${dropped}`);
    }
    this.#persist();
  }

  #persist() {
    // Pitfall 7: strip process_handle (circular ChildProcess refs throw on JSON.stringify)
    const pids = [];
    for (const rec of this.#map.values()) {
      if (rec.kind === 'reservation') continue;
      const { process_handle, ...serializable } = rec;
      pids.push(serializable);
    }
    // Fire-and-forget; log on failure
    fsp.writeFile(this.#persistPath, JSON.stringify({ pids }, null, 2))
      .catch((err) => log(`SubagentManager persist failed: ${err.message}`));
  }

  /** Graceful shutdown: SIGTERM all children, give STOP_GRACE_MS, then SIGKILL survivors. */
  async stop() {
    if (this.#sweepTimer) {
      clearInterval(this.#sweepTimer);
      this.#sweepTimer = null;
    }
    const pids = [];
    for (const [pid, rec] of this.#map.entries()) {
      if (rec.kind === 'reservation') continue;
      pids.push(pid);
      try { process.kill(pid, 'SIGTERM'); } catch { /* dead */ }
    }
    if (pids.length === 0) {
      this.#map.clear();
      return;
    }
    await new Promise((r) => setTimeout(r, STOP_GRACE_MS));
    for (const pid of pids) {
      try { process.kill(pid, 'SIGKILL'); } catch { /* gone */ }
    }
    this.#map.clear();
    try {
      await fsp.writeFile(this.#persistPath, JSON.stringify({ pids: [] }, null, 2));
    } catch { /* best-effort */ }
    log(`SubagentManager stopped (terminated ${pids.length} children)`);
  }

  /** Test-only accessor: snapshot of active records (stripped of process_handle). */
  _snapshot() {
    const out = [];
    for (const rec of this.#map.values()) {
      if (rec.kind === 'reservation') continue;
      const { process_handle, ...serializable } = rec;
      out.push(serializable);
    }
    return out;
  }
}
