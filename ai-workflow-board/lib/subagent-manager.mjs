// ─── Subagent Manager (Phase 4 D-55..D-75 / Phase 2 adapter refactor) ────

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
import { ClaudeCliAdapter } from './cli-adapters/claude.mjs';
import { ADAPTER_CAPABILITIES } from './cli-adapters/base.mjs';
import { fireAndForgetTool } from './mcp-client.mjs';

const { NATIVE_MCP } = ADAPTER_CAPABILITIES;

/**
 * Owns the lifecycle of CLI subagent child processes (one-shot trigger / chat).
 *
 * Phase 2: parameterized by a CliAdapter. The adapter contributes the bits
 * that vary across CLIs (argv shape, mcp-config requirement, stream parsing,
 * one-shot result aggregation). Default = ClaudeCliAdapter so existing
 * callers and tests that don't pass an adapter continue to work unchanged.
 *
 * For non-MCP adapters (gemini, …) the manager:
 *   - Skips the per-spawn mcp-config tempfile (adapter.needsMcpConfig=false)
 *   - Captures stdout lines into the record so the adapter's
 *     collectOneshotResult() can produce a final answer at exit time
 *   - Posts that answer back to AWB via the MCP `add_comment` tool when the
 *     spawn carried a ticketId (the only context where a comment makes sense)
 *
 * Pitfalls guarded:
 *  - Pitfall 1: MCP config file uses {"mcpServers": {...}} wrapper shape
 *  - Pitfall 3: SIGTERM/SIGINT/exit handlers clean up children on shutdown
 *  - Pitfall 4: Concurrency cap reserves slot synchronously before async spawn
 *  - Pitfall 6: stdio: ['ignore', 'pipe', 'pipe'] closes child stdin (claude)
 *  - Pitfall 7: #persist() strips process_handle before JSON.stringify
 */
export class SubagentManager {
  #map = new Map();              // pid → SubagentRecord, AND reservationId → {kind: 'reservation', ...}
  #config;
  #adapter;
  #sweepTimer = null;
  #reservationCounter = 0;
  #persistPath;
  #pidDir;
  #initialized = false;
  #monitor = null;               // v0.32: injected by proxy.mjs after agent_id resolution

  constructor(config, adapter) {
    this.#config = config;
    this.#adapter = adapter || new ClaudeCliAdapter();
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
    if (typeof this.#sweepTimer.unref === 'function') this.#sweepTimer.unref();
    log(`SubagentManager initialized (cli=${this.#adapter.cliType}, pidDir=${this.#pidDir}, cap=${this.#config.delegation.maxConcurrent}, ttl=${this.#config.delegation.ttlMinutes}min)`);
  }

  /** Adapter accessor for tests + external diagnostics. */
  get adapter() { return this.#adapter; }

  /**
   * Delete cfg-*.json files left behind by dead proxies. Same logic as before
   * Phase 2 — only claude-style adapters write cfg files, but the sweep is
   * keyed off filename prefix so it stays correct in mixed-adapter setups.
   */
  async #sweepOrphanCfgs() {
    let files;
    try {
      files = await fsp.readdir(this.#pidDir);
    } catch (err) {
      log(`Orphan cfg sweep: readdir failed: ${err.message}`);
      return;
    }

    const liveCfgs = new Set();
    for (const rec of this.#map.values()) {
      if (rec.kind !== 'reservation' && rec.config_path) liveCfgs.add(rec.config_path);
    }
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
   *   taskText: string,              // positional prompt passed as the last argv (claude)
   *   rolePrompt: string,            // injected via --append-system-prompt (claude)
   *   triggerId?: string,
   *   chatRequestId?: string,
   *   ticketId?: string,
   *   agentId?: string,
   * }
   * Returns { spawned: boolean, pid?: number, reason?: string }
   */
  async spawn(spec) {
    if (spec.triggerId) {
      for (const rec of this.#map.values()) {
        if (rec.kind !== 'reservation' && rec.trigger_id === spec.triggerId) {
          return { spawned: false, reason: 'duplicate_trigger' };
        }
      }
    }

    if (spec.chatRequestId) {
      for (const rec of this.#map.values()) {
        if (rec.kind !== 'reservation' && rec.chat_request_id === spec.chatRequestId) {
          return { spawned: false, reason: 'duplicate_chat' };
        }
      }
    }

    if (!this.canSpawn()) {
      return { spawned: false, reason: 'cap_reached' };
    }
    const reservationId = -(++this.#reservationCounter);
    this.#map.set(reservationId, { kind: 'reservation', started_at: Date.now() });

    let configPath = null;
    try {
      // Build the spawn descriptor first so we know whether mcp-config is
      // needed before writing anything to disk.
      const descriptor = this.#adapter.buildOneshotSpawn({
        rolePrompt: spec.rolePrompt || '',
        taskText: spec.taskText,
        // mcpConfigPath filled in below if needed
        mcpConfigPath: null,
      });

      if (descriptor.needsMcpConfig) {
        // Pitfall 7 (v0.6.11): the child reads --mcp-config lazily after
        // spawn returns. We must NOT rename or delete this file until exit.
        configPath = join(
          this.#pidDir,
          `cfg-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
        );
        await fsp.mkdir(dirname(configPath), { recursive: true, mode: 0o700 });

        // Pitfall 1: wrapper shape. X-AWB-Client-Type: subagent bypasses the
        // server's proxy schemaVersion gate (claude subagents don't go
        // through proxy.mjs).
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

        // Re-build with the real path. The adapter's buildOneshotSpawn is
        // pure (no side effects), so re-invoking it is safe and cheap.
        Object.assign(descriptor, this.#adapter.buildOneshotSpawn({
          rolePrompt: spec.rolePrompt || '',
          taskText: spec.taskText,
          mcpConfigPath: configPath,
        }));
      }

      const resolvedBin = this.#adapter.resolveBin(this.#config.delegation.claudeBin);
      const child = spawn(resolvedBin, descriptor.args, {
        stdio: descriptor.stdio || ['ignore', 'pipe', 'pipe'],
        detached: true,
        windowsHide: true,
        env: { ...process.env, AWB_API_KEY: this.#config.apiKey },
        // Only .cmd/.bat/.ps1 wrappers need cmd.exe shell. Native .exe
        // spawns directly. The adapter normally won't surface a wrapper
        // path (cli-resolver gates Windows to .exe-only), but the user can
        // override via config; respect that choice.
        shell: descriptor.shell ?? /\.(cmd|bat|ps1)$/i.test(resolvedBin),
      });
      // Attach 'error' synchronously BEFORE any early return — spawn() emits
      // 'error' async on ENOENT; without a listener it becomes an
      // uncaughtException.
      child.once('error', (err) => {
        log(`Subagent spawn error: code=${err.code || ''} cli=${this.#adapter.cliType} bin=${resolvedBin} msg=${err.message}`);
      });
      child.unref();

      const pid = child.pid;
      if (!pid) {
        if (configPath) await fsp.unlink(configPath).catch(() => {});
        this.#map.delete(reservationId);
        return { spawned: false, reason: 'spawn_failed' };
      }

      // Push the prompt over stdin if the adapter wants stdin-prompted
      // execution (gemini one-shot). Done after spawn() returns so we have
      // a real child handle.
      if (typeof descriptor.writePrompt === 'function') {
        try { descriptor.writePrompt(child); } catch (err) {
          log(`Subagent writePrompt failed: ${err.message}`);
        }
      }

      const record = {
        pid,
        kind: spec.kind,
        cli_type: this.#adapter.cliType,
        trigger_id: spec.triggerId || null,
        chat_request_id: spec.chatRequestId || null,
        ticket_id: spec.ticketId || null,
        agent_id: spec.agentId || null,
        started_at: Date.now(),
        expected_completion_at: Date.now() + this.#config.delegation.ttlMinutes * 60_000,
        config_path: configPath,
        process_handle: child,
        // For non-MCP adapters: capture stdout so collectOneshotResult can
        // assemble a final answer at exit time.
        captureOutput: !this.#adapter.has(NATIVE_MCP),
        outLines: [],
      };
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

      log(`Subagent spawned: pid=${pid} cli=${this.#adapter.cliType} kind=${spec.kind} ticket=${spec.ticketId || '-'}`);
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
      if (record.config_path) {
        try { await fsp.unlink(record.config_path); } catch { /* best-effort */ }
      }
      record.tap?.end({ exit_code: code, signal });

      // Non-MCP adapters: post the captured output as a ticket comment so
      // the user sees the gemini-style CLI's answer in the AWB UI. Only
      // when the spawn was scoped to a ticket and the run wasn't killed.
      if (record.captureOutput && record.ticket_id && code === 0 && !signal) {
        try {
          const answer = this.#adapter.collectOneshotResult(record.outLines);
          if (answer) await this.#postOneshotAnswer(record, answer);
        } catch (err) {
          log(`Subagent post-answer failed pid=${pid}: ${err.message}`);
        }
      }

      log(`Subagent exit: pid=${pid} cli=${record.cli_type || '-'} kind=${record.kind} code=${code} signal=${signal || '-'} duration=${durationSec}s`);
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
        if (record?.captureOutput) {
          // Bound the buffer so a runaway CLI doesn't OOM the daemon.
          if (record.outLines.length < 10000) record.outLines.push(line);
        }
        log(`[subagent:${pid}] ${line}`);
      });
    }
    if (child.stderr) {
      const rlErr = createInterface({ input: child.stderr });
      rlErr.on('line', (line) => log(`[subagent:${pid}:err] ${line}`));
    }
  }

  async #postOneshotAnswer(record, answer) {
    // Use the same MCP add_comment path persistent ticket sessions use for
    // set/clear_current_task — fire-and-forget, log on failure. Cap content
    // length so an unexpectedly verbose CLI doesn't blow the comment
    // endpoint's payload limit.
    const MAX = 60_000;
    const trimmed = answer.length > MAX ? answer.slice(0, MAX) + '\n\n…[truncated]' : answer;
    await fireAndForgetTool(this.#config, 'add_comment', {
      ticket_id: record.ticket_id,
      content: trimmed,
      type: 'note',
    });
    log(`Subagent posted answer to ticket=${record.ticket_id} (cli=${record.cli_type}, ${trimmed.length} chars)`);
  }

  #sweep() {
    const now = Date.now();
    for (const [pid, record] of this.#map.entries()) {
      if (record.kind === 'reservation') continue;
      try {
        process.kill(pid, 0);
      } catch (err) {
        if (err.code === 'ESRCH' || err.code === 'EPERM') {
          log(`Sweep: pid=${pid} no longer alive, removing record`);
          this.#map.delete(pid);
          if (record.config_path) {
            fsp.rm(dirname(record.config_path), { recursive: true, force: true }).catch(() => {});
          }
          continue;
        }
      }
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
    const pids = [];
    for (const rec of this.#map.values()) {
      if (rec.kind === 'reservation') continue;
      // outLines can grow large for long gemini runs — strip from persist
      // along with the (always non-serializable) process_handle.
      const { process_handle, outLines, ...serializable } = rec;
      pids.push(serializable);
    }
    fsp.writeFile(this.#persistPath, JSON.stringify({ pids }, null, 2))
      .catch((err) => log(`SubagentManager persist failed: ${err.message}`));
  }

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

  /** Test-only accessor: snapshot of active records. */
  _snapshot() {
    const out = [];
    for (const rec of this.#map.values()) {
      if (rec.kind === 'reservation') continue;
      const { process_handle, outLines, ...serializable } = rec;
      out.push(serializable);
    }
    return out;
  }
}
