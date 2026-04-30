// ─── Orphan subagent cleanup ─────────────────────────────
// When the proxy exits cleanly, each subagent's exit hook unlinks its
// mcp-config tempfile + pid sidecar. When it dies hard (SIGKILL, crash,
// host reboot) those hooks never run. The children we spawn are
// `detached: true` + `.unref()`-ed, so THEY survive — and their config
// files + (now) pid sidecars stay on disk.
//
// This module scans SUBAGENTS_BASE_DIR on proxy startup, reads each
// `.pid` sidecar, and reaps anything genuinely orphaned:
//   1. Build a set of cfg paths that appear in the argv of ANY live
//      process on the host (`/proc/*/cmdline`, looking for the
//      `--mcp-config <path>` flag the children are spawned with).
//   2. For each `.pid` sidecar:
//        - if the cfg path is in the live-argv set → a sibling proxy
//          still owns this subagent. Leave the files alone.
//        - else → genuine orphan. SIGTERM the pid (+ delayed SIGKILL),
//          unlink the .pid + .json files.
//
// The /proc protection is the same trick subagent-manager's
// #sweepOrphanCfgs uses; without it, every fresh proxy startup
// SIGTERMs the still-alive subagents owned by an older sibling proxy
// (e.g. the user's long-running interactive Claude Code session being
// trampled by every short-lived `claude` invocation that follows).
// On non-Linux hosts /proc isn't available — we fall back to the
// pre-fix behavior (kill any sidecar pid we can prove is alive),
// which is no worse than before.
//
// Cross-platform detail: `process.kill(pid, 0)` is the standard
// existence check — it throws on ESRCH (no such process) or EPERM
// (exists but no permission). We treat EPERM as "alive, not ours to
// kill" and move on (still clean up stray files); all other errors =
// dead, so just unlink.

import { promises as fsp } from 'fs';
import { join } from 'path';
import { SUBAGENTS_BASE_DIR } from './constants.mjs';
import { log } from './logging.mjs';

const KILL_BACKUP_DELAY_MS = 2000;

async function readPid(pidPath) {
  try {
    const raw = await fsp.readFile(pidPath, 'utf8');
    const pid = parseInt(raw.trim(), 10);
    return Number.isFinite(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function isPidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // EPERM = exists but not ours (treat as alive — best effort kill below
    // will still try). ESRCH = truly gone.
    return err.code === 'EPERM';
  }
}

/**
 * Scan /proc for the set of `--mcp-config <path>` argv values across all
 * live processes. The set is the source of truth for "this cfg path is
 * still backing a live subagent — don't reap it." Returns null on
 * non-Linux / unreadable /proc so callers know to fall back.
 */
async function readLiveCfgPathsFromProc() {
  let procEntries;
  try {
    procEntries = await fsp.readdir('/proc');
  } catch {
    return null;
  }
  const live = new Set();
  for (const entry of procEntries) {
    if (!/^\d+$/.test(entry)) continue;
    try {
      const cmdline = await fsp.readFile(`/proc/${entry}/cmdline`, 'utf8');
      const parts = cmdline.split('\0');
      const idx = parts.indexOf('--mcp-config');
      if (idx >= 0 && parts[idx + 1]) live.add(parts[idx + 1]);
    } catch {
      /* process vanished mid-scan, or perms error — ignore */
    }
  }
  return live;
}

async function reapOne(dir, entry, liveCfgPaths) {
  const pidPath = join(dir, entry);
  const cfgPath = pidPath.replace(/\.pid$/, '.json');

  // Sibling-proxy protection: if any live process on this host has this
  // cfg path on its argv, the cfg is in active use. Skip — leave files
  // and the child alone.
  if (liveCfgPaths && liveCfgPaths.has(cfgPath)) {
    return { skipped: true };
  }

  const pid = await readPid(pidPath);
  if (pid != null && isPidAlive(pid)) {
    log(`[orphan-cleanup] killing stale subagent pid=${pid} (${entry})`);
    try { process.kill(pid, 'SIGTERM'); } catch { /* already gone */ }
    // Backup SIGKILL — unref so the proxy can still exit if the child
    // takes its sweet time dying.
    const t = setTimeout(() => {
      try { process.kill(pid, 'SIGKILL'); } catch { /* gone */ }
    }, KILL_BACKUP_DELAY_MS);
    if (typeof t.unref === 'function') t.unref();
  }
  // Unlink regardless of liveness — stale files are the symptom we care
  // about, the pid kill is the cherry on top.
  await fsp.unlink(pidPath).catch(() => {});
  await fsp.unlink(cfgPath).catch(() => {});
  return { skipped: false };
}

/**
 * Scan SUBAGENTS_BASE_DIR for leftover .pid sidecars and reap each one.
 * Idempotent and safe to call on every proxy startup. Never throws —
 * failures are logged and swallowed because a cleanup blowup must not
 * take down the proxy on boot.
 */
export async function cleanupOrphanSubagents() {
  let entries;
  try {
    entries = await fsp.readdir(SUBAGENTS_BASE_DIR);
  } catch {
    // Dir doesn't exist yet (first run). Nothing to clean.
    return { scanned: 0, reaped: 0 };
  }
  const pidFiles = entries.filter((e) => e.endsWith('.pid'));
  if (pidFiles.length === 0) {
    return { scanned: 0, reaped: 0 };
  }
  // Build the live-cfg set ONCE up front so the per-entry sibling-proxy
  // check is just a Set membership lookup. Null on non-Linux — reapOne
  // will fall back to the kill-everything-alive behavior.
  const liveCfgPaths = await readLiveCfgPathsFromProc();
  log(`[orphan-cleanup] scanning ${pidFiles.length} pid sidecar(s) in ${SUBAGENTS_BASE_DIR} (live cfg paths in /proc: ${liveCfgPaths ? liveCfgPaths.size : 'unavailable'})`);
  let reaped = 0;
  let skipped = 0;
  for (const entry of pidFiles) {
    try {
      const r = await reapOne(SUBAGENTS_BASE_DIR, entry, liveCfgPaths);
      if (r.skipped) skipped++;
      else reaped++;
    } catch (err) {
      log(`[orphan-cleanup] skipping ${entry}: ${err.message}`);
    }
  }
  log(`[orphan-cleanup] reaped ${reaped}/${pidFiles.length} orphan subagents (${skipped} protected as live-sibling)`);
  return { scanned: pidFiles.length, reaped, skipped };
}
