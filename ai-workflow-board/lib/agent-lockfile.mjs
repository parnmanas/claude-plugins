// ─── Agent lockfile (Phase 4) ─────────────────────────────
// Hard mutual exclusion between daemon.mjs and proxy.mjs on the same host.
// Until Phase 4, concurrent daemon+proxy was only soft-protected by
// sibling-aware orphan-cleanup + the server's main-session pinning.
// Broadcast SSE events (e.g. agent_status) still hit BOTH processes and got
// processed twice.
//
// This lockfile is a single PID-owned file at ~/.claude/channels/awb/agent.lock
// containing JSON `{ pid, role, started_at, version }`. Only one of
// daemon/proxy may hold it at a time; the second one's startup aborts unless
// it was launched with --force.
//
// Acquisition rules:
//   1. Try atomic create (O_EXCL via writeFile flag 'wx').
//   2. On EEXIST: read pid from the existing lock and `process.kill(pid, 0)`.
//      - alive   → owner is real. Abort unless force=true. With force=true,
//                  SIGTERM the owner, wait briefly, overwrite the lock.
//      - dead    → stale (last owner crashed). Remove and retry create.
//   3. Garbage on disk (unparseable JSON / pid=0): treat as stale, remove.
//
// Release rules:
//   - On clean shutdown (entrypoint's async shutdown handler), call release().
//   - release() only unlinks if the lockfile's pid still matches ours — so a
//     race where another instance forced its way in won't have us delete
//     ITS lock.
//   - We also wire a `process.on('exit')` synchronous safety net so a crash
//     that bypasses async shutdown still drops the lock; this uses unlinkSync
//     and the same pid-match guard.

import { writeFileSync, readFileSync, unlinkSync, mkdirSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';
import { setTimeout as delay } from 'timers/promises';
import { log } from './logging.mjs';

export const LOCK_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'agent.lock',
);

const FORCE_KILL_GRACE_MS = 1500;

function readLock() {
  try {
    const raw = readFileSync(LOCK_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    const pid = Number.isFinite(parsed?.pid) ? parsed.pid : 0;
    return pid > 0 ? { pid, role: parsed.role, started_at: parsed.started_at, version: parsed.version } : null;
  } catch {
    return null;
  }
}

function isPidAlive(pid) {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err.code === 'EPERM';
  }
}

function writeLockAtomic(payload) {
  // Ensure the directory exists. Cheap; no-op if already there.
  try { mkdirSync(join(LOCK_PATH, '..'), { recursive: true }); } catch { /* ignore */ }
  // 'wx' = O_CREAT | O_EXCL. Throws EEXIST if anyone beat us.
  writeFileSync(LOCK_PATH, JSON.stringify(payload, null, 2) + '\n', { flag: 'wx' });
}

function writeLockOverwrite(payload) {
  try { mkdirSync(join(LOCK_PATH, '..'), { recursive: true }); } catch { /* ignore */ }
  writeFileSync(LOCK_PATH, JSON.stringify(payload, null, 2) + '\n');
}

/**
 * Acquire the agent lockfile. Returns a release-handle on success, throws on
 * conflict. The caller (daemon.mjs / proxy.mjs) should let the throw bubble
 * up and exit the process — that is the "abort startup" behavior the ticket
 * specifies.
 *
 * @param {{ role: 'daemon'|'proxy', version: string, force?: boolean }} opts
 * @returns {{ release: () => void }} call release() during async shutdown
 */
export function acquireAgentLock(opts) {
  const role = opts?.role;
  const version = opts?.version || 'unknown';
  const force = opts?.force === true;
  if (role !== 'daemon' && role !== 'proxy') {
    throw new Error(`acquireAgentLock: invalid role ${JSON.stringify(role)}`);
  }
  const payload = {
    pid: process.pid,
    role,
    version,
    started_at: new Date().toISOString(),
  };

  // First attempt — pure happy path.
  try {
    writeLockAtomic(payload);
    log(`[lockfile] acquired ${LOCK_PATH} (role=${role} pid=${process.pid})`);
    return makeReleaseHandle(payload);
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }

  // Conflict — examine the existing lock.
  const existing = readLock();
  if (!existing) {
    // Garbage on disk. Remove and retry.
    log(`[lockfile] removing unparseable lockfile at ${LOCK_PATH}`);
    try { unlinkSync(LOCK_PATH); } catch { /* race; fine */ }
    writeLockAtomic(payload);
    log(`[lockfile] acquired after stale-cleanup (role=${role} pid=${process.pid})`);
    return makeReleaseHandle(payload);
  }

  if (!isPidAlive(existing.pid)) {
    log(`[lockfile] reusing stale lock (previous owner pid=${existing.pid} role=${existing.role || '?'} dead)`);
    try { unlinkSync(LOCK_PATH); } catch { /* race; fine */ }
    writeLockAtomic(payload);
    log(`[lockfile] acquired after stale-cleanup (role=${role} pid=${process.pid})`);
    return makeReleaseHandle(payload);
  }

  // Owner is alive.
  if (!force) {
    const e = new Error(
      `AWB agent lockfile held by pid=${existing.pid} role=${existing.role || '?'} ` +
      `version=${existing.version || '?'} since ${existing.started_at || '?'}. ` +
      `Stop it first, or pass --force to take over.`,
    );
    e.code = 'EAGENTLOCKED';
    throw e;
  }

  // --force path: knock the live owner over and take the lock.
  log(`[lockfile] --force: SIGTERM previous owner pid=${existing.pid} role=${existing.role || '?'}`);
  try { process.kill(existing.pid, 'SIGTERM'); } catch { /* already gone */ }
  // Best-effort wait — async release is the previous owner's job. We don't
  // block forever; if it doesn't drop the file in 1.5s we just overwrite.
  return forceTakeover(payload, existing.pid);
}

async function forceTakeoverAsync(payload, prevPid) {
  // Poll briefly for the previous owner to release. If it does, atomic
  // create works. If it doesn't, overwrite.
  const start = Date.now();
  while (Date.now() - start < FORCE_KILL_GRACE_MS) {
    if (!isPidAlive(prevPid)) break;
    await delay(100);
  }
  try {
    writeLockAtomic(payload);
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
    writeLockOverwrite(payload);
  }
  log(`[lockfile] --force: acquired by overwrite (role=${payload.role} pid=${process.pid})`);
}

function forceTakeover(payload, prevPid) {
  // Synchronous fallback so acquireAgentLock can stay sync — start the async
  // poll fire-and-forget, but immediately try one overwrite so any caller
  // racing right after the SIGTERM has a fresh file. Pid-match release still
  // guards us from clobbering some THIRD instance's lock.
  try {
    writeLockOverwrite(payload);
  } catch (err) {
    log(`[lockfile] --force overwrite failed: ${err.message}`);
    throw err;
  }
  forceTakeoverAsync(payload, prevPid).catch((err) => log(`[lockfile] takeover poll: ${err.message}`));
  return makeReleaseHandle(payload);
}

function makeReleaseHandle(payload) {
  let released = false;
  // Crash safety net — if the entrypoint never calls release() (uncaught
  // exception, fatal signal mid-shutdown, etc.), drop the lockfile sync
  // before the process actually exits. Pid-match guard prevents wiping a
  // takeover lock written by a --force caller.
  process.on('exit', () => {
    if (released) return;
    safeUnlinkOwn(payload.pid);
  });
  return {
    release() {
      if (released) return;
      released = true;
      safeUnlinkOwn(payload.pid);
    },
    path: LOCK_PATH,
    payload,
  };
}

function safeUnlinkOwn(myPid) {
  // Re-read so we never delete a lockfile that another instance has taken
  // over. If it's still ours, drop it.
  try {
    const raw = readFileSync(LOCK_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    if (parsed?.pid !== myPid) return;
  } catch {
    return;
  }
  try {
    unlinkSync(LOCK_PATH);
    log(`[lockfile] released ${LOCK_PATH}`);
  } catch { /* race; fine */ }
}

/** Pure inspector — does not touch the lockfile. Useful for tests + admin endpoints. */
export function inspectAgentLock() {
  return readLock();
}
