// Integration test — Phase 4 Plan 04-02 — SubagentManager lifecycle
//
// Tests every lifecycle state transition in the SubagentManager class:
//  1. spawn -> exit normal lifecycle (fast exit)
//  2. concurrency cap (maxConcurrent)
//  3. trigger_id dedup
//  4. TTL sweep kill path (simulated via direct SIGTERM of tracked PID)
//  5. Persistence file write on spawn + delete on exit
//  6. Startup reconciliation drops dead PIDs
//  7. stop() SIGTERMs all children and clears state
//  8. MCP config file uses wrapped shape
//
// Design:
//  - Boots SubagentManager with claudeBin pointed at test/fake-claude.sh
//  - Overrides CLAUDE_CONFIG_DIR to an isolated tmp dir so real ~/.claude state is untouched
//  - Uses AWB_FAKE_SLEEP + AWB_FAKE_EXIT_CODE env vars to control child behavior
//  - Uses node:test + node:assert/strict (Node 22+ built-in)

import test from 'node:test';
import assert from 'node:assert/strict';
import {
  mkdtempSync, mkdirSync, readFileSync, writeFileSync, existsSync, rmSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FAKE_CLAUDE = join(__dirname, 'fake-claude.sh');

// Isolate state to a temp dir before importing proxy.mjs
const TMP_BASE = mkdtempSync(join(tmpdir(), 'awb-subagent-test-'));
process.env.CLAUDE_CONFIG_DIR = TMP_BASE;
// Make sure the base "channels/awb" dir exists (proxy.mjs loadConfig() reads from there)
mkdirSync(join(TMP_BASE, 'channels', 'awb'), { recursive: true });
// Seed a minimal config.json so the exported loadConfig() can return a usable object
writeFileSync(
  join(TMP_BASE, 'channels', 'awb', 'config.json'),
  JSON.stringify({ url: 'http://localhost:7701', apiKey: 'test-key-123' }),
);

const { SubagentManager, DELEGATION_DEFAULTS, loadConfig } = await import('../proxy.mjs');

function makeConfig(overrides = {}) {
  return {
    url: 'http://localhost:7701',
    apiKey: 'test-key-123',
    delegation: {
      ...DELEGATION_DEFAULTS,
      claudeBin: FAKE_CLAUDE,
      ...overrides,
    },
  };
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

test.after(() => {
  // Clean up tmp dir
  try { rmSync(TMP_BASE, { recursive: true, force: true }); } catch {}
});

test('loadConfig normalizes delegation section with defaults', () => {
  const cfg = loadConfig();
  assert.ok(cfg);
  assert.equal(cfg.url, 'http://localhost:7701');
  assert.equal(cfg.delegation.enabled, true);
  assert.equal(cfg.delegation.maxConcurrent, 5);
  assert.equal(cfg.delegation.ttlMinutes, 15);
  assert.equal(cfg.delegation.claudeBin, 'claude');
  assert.equal(cfg.delegation.appendSystemPromptMode, 'role_only');
});

test('spawn -> exit lifecycle (fast exit)', async () => {
  const mgr = new SubagentManager(makeConfig());
  await mgr.init();

  // Fast exit
  process.env.AWB_FAKE_SLEEP = '0';
  process.env.AWB_FAKE_EXIT_CODE = '0';
  const result = await mgr.spawn({
    kind: 'trigger',
    taskText: 'test task',
    rolePrompt: 'you are a test agent',
    triggerId: 'trg-1',
    ticketId: 'tkt-1',
    agentId: 'agt-1',
  });

  assert.equal(result.spawned, true);
  assert.ok(result.pid);

  // Give the child time to exit and exit handler to fire
  await sleep(1500);

  const snap = mgr._snapshot();
  assert.equal(snap.length, 0, 'expected no active records after exit');

  await mgr.stop();
});

test('concurrency cap enforced', async () => {
  const mgr = new SubagentManager(makeConfig({ maxConcurrent: 2 }));
  await mgr.init();

  process.env.AWB_FAKE_SLEEP = '5';
  process.env.AWB_FAKE_EXIT_CODE = '0';

  const a = await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'cap-1' });
  const b = await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'cap-2' });
  const c = await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'cap-3' });

  assert.equal(a.spawned, true);
  assert.equal(b.spawned, true);
  assert.equal(c.spawned, false);
  assert.equal(c.reason, 'cap_reached');

  await mgr.stop();
});

test('trigger_id dedup prevents duplicate spawn', async () => {
  const mgr = new SubagentManager(makeConfig());
  await mgr.init();

  process.env.AWB_FAKE_SLEEP = '5';

  const a = await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'dup-1' });
  const b = await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'dup-1' });

  assert.equal(a.spawned, true);
  assert.equal(b.spawned, false);
  assert.equal(b.reason, 'duplicate_trigger');

  await mgr.stop();
});

test('mcp-config file uses wrapped shape {"mcpServers": {...}}', async () => {
  const mgr = new SubagentManager(makeConfig());
  await mgr.init();

  process.env.AWB_FAKE_SLEEP = '3';
  const result = await mgr.spawn({
    kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'shape-1', ticketId: 'x',
  });
  assert.equal(result.spawned, true);

  // Read the per-subagent config file while the child is still running
  await sleep(300);
  const cfgPath = join(TMP_BASE, 'channels', 'awb', 'subagents', String(result.pid), 'mcp-config.json');
  assert.ok(existsSync(cfgPath), `expected config at ${cfgPath}`);
  const parsed = JSON.parse(readFileSync(cfgPath, 'utf8'));
  assert.ok(parsed.mcpServers, 'config missing mcpServers wrapper');
  assert.ok(parsed.mcpServers.awb, 'config missing awb server');
  assert.equal(parsed.mcpServers.awb.type, 'http');
  assert.ok(parsed.mcpServers.awb.headers.Authorization.startsWith('Bearer '));

  await mgr.stop();
});

test('TTL sweep / exit handler clears record after SIGTERM', async () => {
  const mgr = new SubagentManager(makeConfig({ ttlMinutes: 15 }));
  await mgr.init();

  process.env.AWB_FAKE_SLEEP = '30';
  const result = await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'ttl-1' });
  assert.equal(result.spawned, true);

  // Verify the record is tracked, then simulate what the TTL sweep would do
  // by sending SIGTERM directly to the PID. The exit handler should then clean up.
  const snap = mgr._snapshot();
  assert.equal(snap.length, 1);
  const pid = snap[0].pid;

  try { process.kill(pid, 'SIGTERM'); } catch {}

  // Give exit handler time to fire
  await sleep(2000);
  const snap2 = mgr._snapshot();
  assert.equal(snap2.length, 0, 'expected record cleared after SIGTERM + exit handler');

  await mgr.stop();
});

test('persistence file tracks PID across spawn + exit', async () => {
  const mgr = new SubagentManager(makeConfig());
  await mgr.init();

  process.env.AWB_FAKE_SLEEP = '3';
  const result = await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'persist-1' });
  assert.equal(result.spawned, true);

  // Let #persist() flush the initial write
  await sleep(200);
  const persistPath = join(TMP_BASE, 'channels', 'awb', 'subagents.json');
  assert.ok(existsSync(persistPath), 'persistence file not written');
  const persisted = JSON.parse(readFileSync(persistPath, 'utf8'));
  assert.ok(persisted.pids.find((p) => p.pid === result.pid), 'pid not in persistence file');

  // Wait for child exit and persistence to be updated
  await sleep(4000);
  const persisted2 = JSON.parse(readFileSync(persistPath, 'utf8'));
  assert.ok(!persisted2.pids.find((p) => p.pid === result.pid), 'pid should be removed after exit');

  await mgr.stop();
});

test('startup reconciliation drops dead PIDs', async () => {
  const persistPath = join(TMP_BASE, 'channels', 'awb', 'subagents.json');
  writeFileSync(persistPath, JSON.stringify({
    pids: [
      {
        pid: 999999,
        kind: 'trigger',
        trigger_id: 'dead-1',
        started_at: Date.now() - 60_000,
        expected_completion_at: Date.now() + 60_000,
        config_path: '/tmp/none/mcp-config.json',
        ticket_id: null,
        agent_id: null,
        chat_request_id: null,
      },
    ],
  }, null, 2));

  const mgr = new SubagentManager(makeConfig());
  await mgr.init();

  // After init, the dead PID should have been reconciled out
  const snap = mgr._snapshot();
  assert.equal(snap.length, 0, 'expected dead PID to be dropped');
  // Let the reconcile-driven persist flush
  await sleep(100);
  const persisted = JSON.parse(readFileSync(persistPath, 'utf8'));
  assert.equal(persisted.pids.length, 0);

  await mgr.stop();
});

test('stop() terminates all children within grace window', async () => {
  const mgr = new SubagentManager(makeConfig({ maxConcurrent: 5 }));
  await mgr.init();

  process.env.AWB_FAKE_SLEEP = '30';
  await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'stop-1' });
  await mgr.spawn({ kind: 'trigger', taskText: 't', rolePrompt: '', triggerId: 'stop-2' });

  assert.equal(mgr._snapshot().length, 2);

  const start = Date.now();
  await mgr.stop();
  const elapsed = Date.now() - start;

  // stop() waits STOP_GRACE_MS (2s) then SIGKILLs survivors — should complete under 4.5s
  assert.ok(elapsed < 4500, `stop took too long: ${elapsed}ms`);
  assert.equal(mgr._snapshot().length, 0);
});
