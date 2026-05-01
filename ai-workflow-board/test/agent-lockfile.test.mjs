// Unit test — Phase 4 lockfile (acquire / stale reuse / live conflict / force / release)
//
// Each scenario isolates state via a per-test CLAUDE_CONFIG_DIR so the tests
// can run in parallel and never touch the user's real ~/.claude tree.

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

function freshConfigDir() {
  const dir = mkdtempSync(join(tmpdir(), 'awb-lockfile-test-'));
  // Match agent-lockfile.mjs's expected layout — channels/awb/agent.lock
  mkdirSync(join(dir, 'channels', 'awb'), { recursive: true });
  return dir;
}

// We import the module fresh for each test by pointing CLAUDE_CONFIG_DIR
// before the import. agent-lockfile.mjs reads the env at module-load via
// constants in its top-level statement, so we use a dynamic import with a
// cache-buster query string to force re-evaluation in each test.
async function loadModule() {
  const url = new URL('../lib/agent-lockfile.mjs', import.meta.url);
  url.searchParams.set('cacheBust', Math.random().toString(36).slice(2));
  return import(url.href);
}

test('acquire on empty dir creates atomic lockfile', async () => {
  const dir = freshConfigDir();
  process.env.CLAUDE_CONFIG_DIR = dir;
  const { acquireAgentLock, LOCK_PATH } = await loadModule();
  const lock = acquireAgentLock({ role: 'daemon', version: 'test-1.0.0' });
  assert.equal(typeof lock.release, 'function');
  assert.ok(existsSync(LOCK_PATH));
  const parsed = JSON.parse(readFileSync(LOCK_PATH, 'utf8'));
  assert.equal(parsed.pid, process.pid);
  assert.equal(parsed.role, 'daemon');
  lock.release();
  assert.equal(existsSync(LOCK_PATH), false);
  rmSync(dir, { recursive: true, force: true });
});

test('acquire conflicts when live owner exists; rejects with EAGENTLOCKED', async () => {
  const dir = freshConfigDir();
  process.env.CLAUDE_CONFIG_DIR = dir;
  const { acquireAgentLock, LOCK_PATH } = await loadModule();
  // Plant a lockfile pointing at our own pid (definitely alive).
  writeFileSync(
    LOCK_PATH,
    JSON.stringify({ pid: process.pid, role: 'daemon', started_at: new Date().toISOString(), version: 'planted' }),
  );
  assert.throws(
    () => acquireAgentLock({ role: 'proxy', version: 'test-1.0.0' }),
    (err) => err.code === 'EAGENTLOCKED',
  );
  rmSync(dir, { recursive: true, force: true });
});

test('acquire treats stale lockfile (dead pid) as garbage and replaces it', async () => {
  const dir = freshConfigDir();
  process.env.CLAUDE_CONFIG_DIR = dir;
  const { acquireAgentLock, LOCK_PATH } = await loadModule();
  // PID 999999999 will not be running.
  writeFileSync(
    LOCK_PATH,
    JSON.stringify({ pid: 999999999, role: 'daemon', started_at: '2000-01-01T00:00:00.000Z', version: 'stale' }),
  );
  const lock = acquireAgentLock({ role: 'daemon', version: 'test-1.0.0' });
  const parsed = JSON.parse(readFileSync(LOCK_PATH, 'utf8'));
  assert.equal(parsed.pid, process.pid);
  assert.equal(parsed.role, 'daemon');
  lock.release();
  rmSync(dir, { recursive: true, force: true });
});

test('acquire treats unparseable lockfile as garbage', async () => {
  const dir = freshConfigDir();
  process.env.CLAUDE_CONFIG_DIR = dir;
  const { acquireAgentLock, LOCK_PATH } = await loadModule();
  writeFileSync(LOCK_PATH, 'this is not json');
  const lock = acquireAgentLock({ role: 'proxy', version: 'test-1.0.0' });
  const parsed = JSON.parse(readFileSync(LOCK_PATH, 'utf8'));
  assert.equal(parsed.pid, process.pid);
  lock.release();
  rmSync(dir, { recursive: true, force: true });
});

test('--force overwrites a live owner', async () => {
  const dir = freshConfigDir();
  process.env.CLAUDE_CONFIG_DIR = dir;
  const { acquireAgentLock, LOCK_PATH } = await loadModule();
  // Spawn a long-lived sleep; we'll use its pid as the "live owner". We
  // intentionally avoid signaling it (it's a sleep — SIGTERM lands fine but
  // we don't want the test to depend on that timing).
  const sleeper = spawnSync('sh', ['-c', 'sleep 30 & echo $! && wait'], { encoding: 'utf8', timeout: 1000 });
  // The above won't actually return until sleep finishes; use a different
  // approach — fork a detached sleep via child_process.spawn.
  const { spawn } = await import('node:child_process');
  const child = spawn('sleep', ['30'], { detached: true, stdio: 'ignore' });
  child.unref();
  try {
    writeFileSync(
      LOCK_PATH,
      JSON.stringify({ pid: child.pid, role: 'daemon', started_at: new Date().toISOString(), version: 'live' }),
    );
    const lock = acquireAgentLock({ role: 'daemon', version: 'test-2.0.0', force: true });
    const parsed = JSON.parse(readFileSync(LOCK_PATH, 'utf8'));
    assert.equal(parsed.pid, process.pid);
    lock.release();
  } finally {
    try { process.kill(child.pid, 'SIGKILL'); } catch { /* gone */ }
  }
  rmSync(dir, { recursive: true, force: true });
});

test('release does not unlink a lockfile owned by a different pid', async () => {
  const dir = freshConfigDir();
  process.env.CLAUDE_CONFIG_DIR = dir;
  const { acquireAgentLock, LOCK_PATH } = await loadModule();
  const lock = acquireAgentLock({ role: 'daemon', version: 'test-1.0.0' });
  // Simulate another daemon force-overwrote our lock with its own pid.
  writeFileSync(
    LOCK_PATH,
    JSON.stringify({ pid: process.pid + 1000, role: 'daemon', started_at: new Date().toISOString(), version: 'takeover' }),
  );
  lock.release();
  // Release MUST have left the takeover lock alone.
  assert.ok(existsSync(LOCK_PATH));
  const parsed = JSON.parse(readFileSync(LOCK_PATH, 'utf8'));
  assert.equal(parsed.pid, process.pid + 1000);
  rmSync(dir, { recursive: true, force: true });
});

test('inspectAgentLock returns null when no lockfile, parsed object otherwise', async () => {
  const dir = freshConfigDir();
  process.env.CLAUDE_CONFIG_DIR = dir;
  const { acquireAgentLock, inspectAgentLock } = await loadModule();
  assert.equal(inspectAgentLock(), null);
  const lock = acquireAgentLock({ role: 'daemon', version: 'test-3.0.0' });
  const inspected = inspectAgentLock();
  assert.equal(inspected.pid, process.pid);
  assert.equal(inspected.role, 'daemon');
  assert.equal(inspected.version, 'test-3.0.0');
  lock.release();
  rmSync(dir, { recursive: true, force: true });
});
