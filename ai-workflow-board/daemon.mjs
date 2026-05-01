#!/usr/bin/env node

/**
 * AWB Agent Manager — daemon entrypoint (Phase 1)
 *
 * Standalone process that owns the SSE channel + subagent lifecycle that
 * `proxy.mjs` runs as a side-effect of being launched by Claude CLI. Where
 * proxy.mjs requires an active Claude CLI parent (it bridges that CLI's
 * stdio MCP traffic to the AWB HTTP server), the daemon has no MCP-stdio
 * responsibility — it just connects to the AWB SSE stream and spawns
 * subagents (`claude --print …`) for each trigger / chat / mention event.
 *
 * Phase 1 scope is parity with proxy.mjs's non-MCP pipeline:
 *   - Reads the same ~/.claude/channels/awb/{config,agent}.json files.
 *   - Reuses the same lib/* (SubagentManager, Chat/TicketSessionManager,
 *     EventStream, PresenceHeartbeat, SubagentMonitor, FsBrowser, …).
 *   - Spawns claude.exe subagents the same way.
 *   - Persistence + orphan cleanup share the same on-disk state, so a
 *     daemon restart correctly inherits in-flight subagents and a daemon
 *     running alongside a sibling proxy won't trample each other's
 *     children (orphan-cleanup reads each running process's cmdline for the
 *     `--mcp-config` flag; live cfg paths are protected from reaping).
 *
 * Concurrent run with proxy.mjs: prevented by Phase 4's lockfile
 * (lib/agent-lockfile.mjs). Whichever process boots first holds
 * `~/.claude/channels/awb/agent.lock`; the other aborts at startup unless
 * launched with --force (daemon CLI flag) or AWB_FORCE_LOCK=1 (proxy env).
 * This eliminates the broadcast-event double-processing risk that the
 * orphan-cleanup + main-session-pinning soft mutex left open.
 *
 * Phase 4 signal contract:
 *   - SIGTERM/SIGINT  → graceful drain + lockfile release + exit
 *   - SIGUSR1         → git pull on plugin repo, then drain + re-exec self
 *   - SIGHUP          → re-read config.json + apply non-disruptive changes
 *
 * Out of scope for Phase 1 (deferred to later phases):
 *   - non-claude CLI adapters (gemini, codex, …) — Phase 2
 *   - AWB web UI / control surface for the daemon — Phase 3
 */

import { loadConfig, resolveAgentId } from './lib/config.mjs';
import { log } from './lib/logging.mjs';
import { acquireAgentLock } from './lib/agent-lockfile.mjs';
import { runSelfUpdate } from './lib/self-update.mjs';
import { PresenceHeartbeat } from './lib/presence-heartbeat.mjs';
import { EventStream } from './lib/event-stream.mjs';
import { SubagentManager } from './lib/subagent-manager.mjs';
import { ChatSessionManager } from './lib/chat-session-manager.mjs';
import { TicketSessionManager } from './lib/ticket-session-manager.mjs';
import { uploadIfNewErrors } from './lib/error-log-uploader.mjs';
import { onFlushThreshold } from './lib/event-log-recorder.mjs';
import { cleanupOrphanSubagents } from './lib/orphan-cleanup.mjs';
import { FsBrowser } from './lib/fs-browser.mjs';
import { SubagentMonitor } from './lib/subagent-monitor.mjs';
import { createAdapter, ADAPTER_CAPABILITIES } from './lib/cli-adapters/index.mjs';

// Plugin version is read at runtime from plugin.json so the daemon and the
// proxy never drift out of sync with the actual installed version. We do
// this lazily inside main() so a missing/malformed plugin.json doesn't
// stop the module from loading (matters for `node --test` import).
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { spawn } from 'child_process';

function readPluginVersion() {
  try {
    const here = dirname(fileURLToPath(import.meta.url));
    const raw = readFileSync(join(here, '.claude-plugin', 'plugin.json'), 'utf8');
    return String(JSON.parse(raw).version || '').trim() || 'unknown';
  } catch {
    return 'unknown';
  }
}

function parseDaemonFlags(argv) {
  return {
    force: argv.includes('--force') || argv.includes('-f'),
  };
}

async function runDaemon(argv = process.argv.slice(2)) {
  let config = loadConfig();
  if (!config?.url || !config?.apiKey) {
    log('Daemon: not configured. Run /awb:setup <server-url> <api-key> to connect.');
    process.exit(1);
  }
  const version = readPluginVersion();
  const flags = parseDaemonFlags(argv);

  // Phase 4: hard mutual exclusion. If a sibling daemon or legacy proxy is
  // already on this channel, abort startup unless --force was passed. We
  // acquire BEFORE any heavy state (subagent managers, SSE, presence) so a
  // refused startup doesn't briefly double-spawn anything.
  let lock;
  try {
    lock = acquireAgentLock({ role: 'daemon', version, force: flags.force });
  } catch (err) {
    if (err.code === 'EAGENTLOCKED') {
      log(`Daemon: ${err.message}`);
      process.exit(2);
    }
    throw err;
  }

  // Phase 2: pick the CLI adapter once at startup. Same adapter instance is
  // shared across SubagentManager + Chat/TicketSessionManager so all spawn
  // sites agree on argv shape and stream parsing.
  const adapter = createAdapter(config.cli);
  const persistent = adapter.has(ADAPTER_CAPABILITIES.PERSISTENT_SESSION);
  log(`AWB Agent Manager starting (server=${config.url} version=${version} cli=${adapter.cliType} persistent_sessions=${persistent})`);
  log(`Delegation: maxConcurrent=${config.delegation.maxConcurrent} ttl=${config.delegation.ttlMinutes}min idle=${config.delegation.idleMinutes}min cliBin=${config.delegation.claudeBin}`);

  // Resolve agent_id from agent.json (or via MCP whoami once). Same path
  // proxy.mjs uses; Presence + monitor wait on this Promise.
  const agentIdReady = resolveAgentId(config).then((id) => {
    if (id) log(`Agent identity: ${id.slice(0, 8)}…`);
    else log('Agent identity: not resolved — presence + error-log upload disabled until /awb:setup writes agent.json');
    return id;
  });

  // Mirror proxy.mjs's kickPresencePing pattern so SSE reconnect / forward-session
  // re-init can fast-path an out-of-band ping. The daemon has no forward-session
  // (no MCP stdio), so only the SSE reconnect path uses this.
  const presenceHeartbeat = { _real: null };
  const kickPresencePing = () => {
    presenceHeartbeat._real?.pingNow().catch(() => { /* logged inside pingNow */ });
  };

  // Reap orphan subagents from previous daemon / proxy generations on this host.
  // Sibling-proxy protection (orphan-cleanup.mjs) reads /proc/*/cmdline so live
  // children of any other plugin instance are NOT killed — only genuine orphans.
  cleanupOrphanSubagents()
    .then((r) => {
      if (r.scanned > 0) log(`Orphan subagent cleanup: scanned=${r.scanned} reaped=${r.reaped} skipped=${r.skipped ?? 0}`);
    })
    .catch((err) => log(`Orphan subagent cleanup failed: ${err.message}`));

  const subagentManager = new SubagentManager(config, adapter);
  subagentManager.init().catch((err) => log(`SubagentManager init failed: ${err.message}`));

  // Persistent session managers only run when the adapter supports them.
  // For stateless adapters (gemini) we still construct them so the
  // EventStream interface stays uniform — they'll just decline every spawn
  // and fall through to one-shot SubagentManager.spawn for triggers.
  const chatSessionManager = new ChatSessionManager(config, adapter);
  const ticketSessionManager = new TicketSessionManager(config, adapter);
  const fsBrowser = new FsBrowser(config, config.fs_browser || {});

  const subagentMonitor = new SubagentMonitor(config, null);
  subagentManager.setMonitor(subagentMonitor);
  chatSessionManager.setMonitor(subagentMonitor);
  ticketSessionManager.setMonitor(subagentMonitor);

  // proxy.mjs sends a notifications/claude/channel "completed" message to the
  // parent Claude CLI on every subagent exit. The daemon has no parent CLI,
  // so we only log. The web UI already renders subagent state via the
  // SubagentMonitor / Subagent entity — no functional regression.
  subagentManager.onExit = ({ pid, record, code, signal, durationSec }) => {
    const label = record.kind === 'chat' ? 'Chat Subagent' : 'Subagent';
    let msg;
    if (signal === 'SIGTERM' || signal === 'SIGKILL') {
      msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} TIMED OUT after ${durationSec}s`;
    } else if (code === 0) {
      msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} completed (duration=${durationSec}s)`;
    } else {
      msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} FAILED (exit=${code}, duration=${durationSec}s, see daemon logs)`;
    }
    log(msg);
  };

  let eventStream = null;
  let uploadTimer = null;
  // Phase 4: shutdown can be steered by SIGUSR1 (self-update) — drain
  // subagents, then re-exec the daemon with the updated plugin code instead
  // of just exit(0). Set before shutdown() is called.
  let postShutdownAction = 'exit';

  const shutdown = async (signal) => {
    log(`Daemon received ${signal} — terminating subagents`);
    presenceHeartbeat._real?.stop();
    if (uploadTimer) { clearInterval(uploadTimer); uploadTimer = null; }
    eventStream?.stop();
    try { await subagentManager.stop(); } catch (err) { log(`shutdown: ${err.message}`); }
    try { await chatSessionManager.stop(); } catch (err) { log(`shutdown (chat): ${err.message}`); }
    try { await ticketSessionManager.stop(); } catch (err) { log(`shutdown (ticket): ${err.message}`); }
    try { subagentMonitor.stop(); } catch (err) { log(`shutdown (monitor): ${err.message}`); }
    // Release lock LAST — before this line another daemon spinning up would
    // be told "owner alive" and abort, which is exactly what we want until
    // our subagents are gone.
    try { lock.release(); } catch (err) { log(`shutdown (lockfile): ${err.message}`); }
    if (postShutdownAction === 'reexec') {
      reExecSelf(argv);
      // reExecSelf exits via process.exit(0) once the child is detached.
      return;
    }
    process.exit(0);
  };
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  process.once('SIGINT', () => shutdown('SIGINT'));

  // SIGUSR1 — self-update trigger. POSIX-standard "reload binary" signal.
  // Used by the (Phase 3) admin endpoint `/admin/agent-manager/instances/:id/restart`
  // and by an operator running `kill -USR1 <pid>` directly. The handler is
  // async-safe via a one-shot guard: the second SIGUSR1 during an in-flight
  // update is ignored (logged) so a flapping endpoint can't queue restarts.
  let selfUpdateInFlight = false;
  process.on('SIGUSR1', async () => {
    if (selfUpdateInFlight) { log('SIGUSR1: self-update already in flight, ignoring'); return; }
    selfUpdateInFlight = true;
    try {
      const result = await runSelfUpdate({ log });
      log(`Self-update: ${result.summary}`);
      postShutdownAction = 'reexec';
      await shutdown('SIGUSR1/self-update');
    } catch (err) {
      log(`Self-update failed: ${err?.stack || err?.message || err}`);
      selfUpdateInFlight = false;
    }
  });

  // SIGHUP — config reload. Re-reads ~/.claude/channels/awb/config.json and
  // applies non-disruptive changes in place. Disruptive changes (server URL
  // / apiKey / cli type) require a full restart; we log a notice but don't
  // auto-restart — the operator can chase it with SIGUSR1 if they want.
  process.on('SIGHUP', () => {
    const next = loadConfig();
    if (!next?.url || !next?.apiKey) {
      log('SIGHUP: config.json missing or unparseable — keeping previous config');
      return;
    }
    const disruptive = (
      next.url !== config.url ||
      next.apiKey !== config.apiKey ||
      String(next.cli || '') !== String(config.cli || '')
    );
    // Hot-reloadable: delegation tunables. SubagentManager / session managers
    // read from the live config object on each spawn; mutating the object in
    // place propagates without restart.
    Object.assign(config, next);
    log(
      `SIGHUP: config reloaded (delegation.maxConcurrent=${config.delegation.maxConcurrent} ` +
      `ttl=${config.delegation.ttlMinutes}min idle=${config.delegation.idleMinutes}min)` +
      (disruptive ? ' — server/apiKey/cli changes need SIGUSR1 to take effect' : ''),
    );
  });

  // Start SSE immediately — no MCP handshake gate (the daemon owns its own
  // lifecycle; the only reason proxy.mjs waits for `notifications/initialized`
  // is to avoid pushing channel events to a Claude CLI that isn't ready yet).
  eventStream = new EventStream(
    config,
    subagentManager,
    chatSessionManager,
    ticketSessionManager,
    fsBrowser,
    kickPresencePing,
  );
  eventStream.start();
  log('SSE event stream started');

  // Kick presence + error-log upload after agent_id resolves. Heartbeat fires
  // immediately on start() so the dashboard flips to ONLINE within the first
  // second; uploadIfNewErrors also fires once immediately so a recent crash
  // is surfaced in admin UI without waiting 30s for the first interval tick.
  agentIdReady.then((agentId) => {
    if (!agentId) return;
    presenceHeartbeat._real = new PresenceHeartbeat(config, agentId);
    presenceHeartbeat._real.start();
    const fireUpload = () => uploadIfNewErrors(config, agentId, version).catch(() => {});
    fireUpload();
    uploadTimer = setInterval(fireUpload, 30 * 1000);
    if (typeof uploadTimer.unref === 'function') uploadTimer.unref();
    onFlushThreshold(fireUpload);
  });

  log('AWB Agent Manager ready');
}

function reExecSelf(argv) {
  // Self-restart after self-update. Spawn a fresh `node daemon.mjs ...`
  // detached so the child outlives our process.exit; inherit stdio so logs
  // continue going to the same place (systemd journal, terminal, etc.).
  // We strip any prior --force / -f from argv (they're no longer needed —
  // the lock has been released) and re-add --force as a belt-and-braces
  // measure for the rare case where our sync exit listener couldn't unlink.
  const passthrough = argv.filter((a) => a !== '--force' && a !== '-f');
  const child = spawn(process.execPath, [process.argv[1], ...passthrough, '--force'], {
    detached: true,
    stdio: 'inherit',
    env: process.env,
  });
  child.unref();
  log(`Self-update: re-exec spawned pid=${child.pid}`);
  process.exit(0);
}

// Only run when executed directly. Test harnesses can `import { runDaemon }`
// without the side effect of actually starting it.
const isDirectExecution =
  import.meta.url === `file://${process.argv[1]}` ||
  (typeof process.argv[1] === 'string' && process.argv[1].endsWith('daemon.mjs'));

if (isDirectExecution) {
  runDaemon().catch((err) => {
    log(`Daemon fatal: ${err?.stack || err?.message || err}`);
    process.exit(1);
  });
}

export { runDaemon };
