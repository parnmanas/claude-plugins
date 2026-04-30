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
 * Concurrent run with proxy.mjs: technically allowed (orphan-cleanup is
 * sibling-aware) but DISCOURAGED — both will receive the same SSE events
 * and try to spawn for them. The AWB server's per-agent main-session
 * routing (events.controller.ts AGENT_ROUTED_EVENTS) will pin trigger /
 * chat / mention events to ONE of them, so most events go to one path —
 * but the broadcast events and any unpinned routing fall to whichever
 * sees them first. Phase 4 will add a lockfile to make the daemon and
 * the legacy proxy mutually exclusive.
 *
 * Out of scope for Phase 1 (deferred to later phases):
 *   - non-claude CLI adapters (gemini, codex, …) — Phase 2
 *   - AWB web UI / control surface for the daemon — Phase 3
 *   - daemon self-update + per-agent config UI + lockfile — Phase 4
 */

import { loadConfig, resolveAgentId } from './lib/config.mjs';
import { log } from './lib/logging.mjs';
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

// Plugin version is read at runtime from plugin.json so the daemon and the
// proxy never drift out of sync with the actual installed version. We do
// this lazily inside main() so a missing/malformed plugin.json doesn't
// stop the module from loading (matters for `node --test` import).
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

function readPluginVersion() {
  try {
    const here = dirname(fileURLToPath(import.meta.url));
    const raw = readFileSync(join(here, '.claude-plugin', 'plugin.json'), 'utf8');
    return String(JSON.parse(raw).version || '').trim() || 'unknown';
  } catch {
    return 'unknown';
  }
}

async function runDaemon() {
  const config = loadConfig();
  if (!config?.url || !config?.apiKey) {
    log('Daemon: not configured. Run /awb:setup <server-url> <api-key> to connect.');
    process.exit(1);
  }
  const version = readPluginVersion();
  log(`AWB Agent Manager starting (server=${config.url} version=${version})`);
  log(`Delegation: maxConcurrent=${config.delegation.maxConcurrent} ttl=${config.delegation.ttlMinutes}min idle=${config.delegation.idleMinutes}min claudeBin=${config.delegation.claudeBin}`);

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

  const subagentManager = new SubagentManager(config);
  subagentManager.init().catch((err) => log(`SubagentManager init failed: ${err.message}`));

  const chatSessionManager = new ChatSessionManager(config);
  const ticketSessionManager = new TicketSessionManager(config);
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

  const shutdown = async (signal) => {
    log(`Daemon received ${signal} — terminating subagents`);
    presenceHeartbeat._real?.stop();
    if (uploadTimer) { clearInterval(uploadTimer); uploadTimer = null; }
    eventStream?.stop();
    try { await subagentManager.stop(); } catch (err) { log(`shutdown: ${err.message}`); }
    try { await chatSessionManager.stop(); } catch (err) { log(`shutdown (chat): ${err.message}`); }
    try { await ticketSessionManager.stop(); } catch (err) { log(`shutdown (ticket): ${err.message}`); }
    try { subagentMonitor.stop(); } catch (err) { log(`shutdown (monitor): ${err.message}`); }
    process.exit(0);
  };
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  process.once('SIGINT', () => shutdown('SIGINT'));

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
