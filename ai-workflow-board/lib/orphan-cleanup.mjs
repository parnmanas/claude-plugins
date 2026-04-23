// ─── Orphan subagent cleanup ─────────────────────────────
// When the proxy exits cleanly, each subagent's exit hook unlinks its
// mcp-config tempfile + pid sidecar. When it dies hard (SIGKILL, crash,
// host reboot) those hooks never run. The children we spawn are
// `detached: true` + `.unref()`-ed, so THEY survive — and their config
// files + (now) pid sidecars stay on disk.
//
// This module scans SUBAGENTS_BASE_DIR on proxy startup, reads each
// `.pid` sidecar, and reaps anything it finds:
//   1. If the pid is still alive, SIGTERM (+ delayed SIGKILL backup).
//   2. Delete both the .pid file and its matching .json cfg.
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

async function reapOne(dir, entry) {
  const pidPath = join(dir, entry);
  const cfgPath = pidPath.replace(/\.pid$/, '.json');

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
  log(`[orphan-cleanup] scanning ${pidFiles.length} pid sidecar(s) in ${SUBAGENTS_BASE_DIR}`);
  let reaped = 0;
  for (const entry of pidFiles) {
    try {
      await reapOne(SUBAGENTS_BASE_DIR, entry);
      reaped++;
    } catch (err) {
      log(`[orphan-cleanup] skipping ${entry}: ${err.message}`);
    }
  }
  log(`[orphan-cleanup] reaped ${reaped}/${pidFiles.length} orphan subagents`);
  return { scanned: pidFiles.length, reaped };
}
