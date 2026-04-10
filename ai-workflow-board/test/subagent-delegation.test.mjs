// Integration test — Phase 4 Plan 04-04 — subagent delegation end-to-end
//
// Exercises the full delegation path: EventStream.#handleTrigger / #handleChatRequest →
// SubagentManager.spawn (with fake-claude.sh stub) → child exit → onExit hook.
// Does NOT boot an AWB server — feeds the EventStream pre-composed event data directly
// via the _testDispatchTrigger / _testDispatchChatRequest test shims on the class.
//
// Six scenarios:
//  1. Trigger delegation happy path
//  2. Chat delegation happy path (envelope-native payload)
//  3. Legacy fallback when delegation disabled
//  4. Legacy fallback when maxConcurrent cap reached
//  5. Trigger dedup (same trigger_id twice)
//  6. TTL timeout → TIMED OUT completion notification
//
// Uses:
//  - fake-claude.sh as claudeBin (no real Claude CLI dependency)
//  - CLAUDE_CONFIG_DIR tmp dir isolation
//  - process.stdout.write monkey-patch to capture sendChannelEvent notifications

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FAKE_CLAUDE = join(__dirname, 'fake-claude.sh');

// Isolate state before importing proxy.mjs
const TMP_BASE = mkdtempSync(join(tmpdir(), 'awb-deleg-test-'));
process.env.CLAUDE_CONFIG_DIR = TMP_BASE;
mkdirSync(join(TMP_BASE, 'channels', 'awb'), { recursive: true });
writeFileSync(
  join(TMP_BASE, 'channels', 'awb', 'config.json'),
  JSON.stringify({ url: 'http://localhost:7793', apiKey: 'test-key' }),
);

const { SubagentManager, EventStream, DELEGATION_DEFAULTS } = await import('../proxy.mjs');

function makeConfig(overrides = {}) {
  return {
    url: 'http://localhost:7793',
    apiKey: 'test-key',
    delegation: {
      ...DELEGATION_DEFAULTS,
      claudeBin: FAKE_CLAUDE,
      ...overrides,
    },
  };
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

/**
 * Captures sendChannelEvent output by monkey-patching process.stdout.write.
 * Returns a handle {notifications, restore()}.
 *
 * NOTE: sendChannelEvent writes JSON-RPC envelopes with method
 * 'notifications/claude/channel' and params {content, meta}. The capture parses
 * each line written to stdout, filters for those envelopes, and pushes
 * params into the notifications array.
 */
function captureChannelNotifications() {
  const notifications = [];
  const origWrite = process.stdout.write.bind(process.stdout);
  process.stdout.write = function (chunk, encoding, cb) {
    const str = typeof chunk === 'string' ? chunk : chunk?.toString?.() ?? '';
    // Scan the chunk for JSON-RPC channel notification lines. If found, capture
    // them AND let the full chunk pass through to stdout unchanged — the node:test
    // reporter also writes to stdout and must not be swallowed, otherwise the
    // test results (and subtest pass/fail lines) disappear from the output.
    // The channel JSON lines are tiny and mixing them into the test log is fine.
    for (const line of str.split('\n')) {
      if (!line.trim()) continue;
      if (!line.startsWith('{')) continue; // fast filter
      try {
        const parsed = JSON.parse(line);
        if (parsed && parsed.method === 'notifications/claude/channel') {
          notifications.push(parsed.params);
        }
      } catch { /* not JSON — ignore */ }
    }
    return origWrite(chunk, encoding, cb);
  };
  return {
    notifications,
    restore: () => { process.stdout.write = origWrite; },
  };
}

// ─── Minimal fetch stub for fetchTicketContext ─────────────
// The test doesn't boot an AWB server, so a real fetch() would fail with
// ECONNREFUSED. Stub global fetch so fetchTicketContext returns a minimal
// ticket object for the trigger tests. Returning null from fetchTicketContext
// is also valid — composeTriggerPrompt handles a null ticket gracefully —
// but we provide a real response so the test can assert on taskText content.
const origFetch = globalThis.fetch;
function installFetchStub(response) {
  globalThis.fetch = async (url, opts) => {
    if (String(url).includes('/api/tickets/')) {
      return {
        ok: true,
        status: 200,
        async json() { return response; },
      };
    }
    return { ok: false, status: 404, statusText: 'Not Found', async json() { return {}; } };
  };
}
function restoreFetch() { globalThis.fetch = origFetch; }

test.after(() => {
  restoreFetch();
  try { rmSync(TMP_BASE, { recursive: true, force: true }); } catch {}
});

// ─── Scenario 1: Trigger delegation happy path ─────────────
test('trigger delegation happy path: spawn + dispatch notification + completion', async () => {
  const cap = captureChannelNotifications();
  try {
    installFetchStub({
      id: 'tkt-happy',
      title: 'Fix the bug',
      description: 'Something is broken',
      prompt_text: 'Do the thing carefully',
      comments: [{ body: 'First comment', author_name: 'user', created_at: '2026-04-10T00:00:00Z' }],
    });

    const mgr = new SubagentManager(makeConfig({ maxConcurrent: 2 }));
    await mgr.init();

    const spawnCalls = [];
    const origSpawn = mgr.spawn.bind(mgr);
    mgr.spawn = async (spec) => { spawnCalls.push(spec); return origSpawn(spec); };

    // Install completion-notification handler (mirrors the Plan 04-03 runProxy wiring)
    mgr.onExit = ({ pid, record, code, signal, durationSec }) => {
      const label = record.kind === 'chat' ? 'Chat Subagent' : 'Subagent';
      let msg;
      if (signal === 'SIGTERM' || signal === 'SIGKILL') {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} TIMED OUT after ${durationSec}s`;
      } else if (code === 0) {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} completed (duration=${durationSec}s)`;
      } else {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} FAILED (exit=${code}, duration=${durationSec}s)`;
      }
      process.stdout.write(JSON.stringify({
        jsonrpc: '2.0',
        method: 'notifications/claude/channel',
        params: {
          content: msg,
          meta: {
            type: 'subagent_complete', subagent_kind: record.kind, ticket_id: record.ticket_id,
            trigger_id: record.trigger_id, pid, exit_code: code, signal, duration_sec: durationSec,
          },
        },
      }) + '\n');
    };

    const stream = new EventStream(makeConfig({ maxConcurrent: 2 }), mgr);

    process.env.AWB_FAKE_SLEEP = '0';
    process.env.AWB_FAKE_EXIT_CODE = '0';

    const trigger = JSON.stringify({
      board_id: '__trigger__',
      event_type: 'agent_trigger',
      ticket_id: 'tkt-happy',
      entity_type: 'trigger',
      action: 'assignee',
      field_changed: 'trg-happy-1',
      actor_name: 'agent-42',
      role_prompt: 'You are a helpful coder agent',
      ticket_prompt: 'Do the thing carefully',
      trigger_source: 'test',
      timestamp: '2026-04-10T00:00:00Z',
    });

    await stream._testDispatchTrigger(trigger);

    assert.equal(spawnCalls.length, 1, 'expected exactly 1 spawn call');
    assert.equal(spawnCalls[0].kind, 'trigger');
    assert.equal(spawnCalls[0].rolePrompt, 'You are a helpful coder agent');
    assert.equal(spawnCalls[0].triggerId, 'trg-happy-1');
    assert.equal(spawnCalls[0].ticketId, 'tkt-happy');
    assert.ok(spawnCalls[0].taskText.includes('Fix the bug'), 'taskText should include ticket title');
    assert.ok(
      spawnCalls[0].taskText.includes('Do the thing carefully'),
      'taskText should include ticket_prompt',
    );

    // Wait for child exit + onExit to fire (fake-claude.sh with SLEEP=0 exits fast)
    await sleep(1500);

    // Assert dispatch notification captured
    const dispatches = cap.notifications.filter((n) => n.meta?.type === 'subagent_dispatched');
    assert.equal(dispatches.length, 1, 'expected 1 dispatch notification');
    assert.equal(dispatches[0].meta.subagent_kind, 'trigger');
    assert.equal(dispatches[0].meta.ticket_id, 'tkt-happy');

    // Assert completion notification captured
    const completes = cap.notifications.filter((n) => n.meta?.type === 'subagent_complete');
    assert.equal(completes.length, 1, 'expected 1 completion notification');
    assert.equal(completes[0].meta.exit_code, 0);
    assert.equal(completes[0].meta.subagent_kind, 'trigger');

    await mgr.stop();
  } finally {
    cap.restore();
    restoreFetch();
  }
});

// ─── Scenario 2: Chat delegation happy path ────────────────
test('chat delegation happy path: envelope-native payload → spawn kind=chat', async () => {
  const cap = captureChannelNotifications();
  try {
    const mgr = new SubagentManager(makeConfig({ maxConcurrent: 2 }));
    await mgr.init();

    const spawnCalls = [];
    const origSpawn = mgr.spawn.bind(mgr);
    mgr.spawn = async (spec) => { spawnCalls.push(spec); return origSpawn(spec); };

    // Install completion-notification handler (mirrors runProxy wiring)
    mgr.onExit = ({ pid, record, code, signal, durationSec }) => {
      const label = record.kind === 'chat' ? 'Chat Subagent' : 'Subagent';
      let msg;
      if (signal === 'SIGTERM' || signal === 'SIGKILL') {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} TIMED OUT after ${durationSec}s`;
      } else if (code === 0) {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} completed (duration=${durationSec}s)`;
      } else {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} FAILED (exit=${code}, duration=${durationSec}s)`;
      }
      process.stdout.write(JSON.stringify({
        jsonrpc: '2.0',
        method: 'notifications/claude/channel',
        params: {
          content: msg,
          meta: {
            type: 'subagent_complete', subagent_kind: record.kind, ticket_id: record.ticket_id,
            trigger_id: record.trigger_id, pid, exit_code: code, signal, duration_sec: durationSec,
          },
        },
      }) + '\n');
    };

    const stream = new EventStream(makeConfig({ maxConcurrent: 2 }), mgr);

    process.env.AWB_FAKE_SLEEP = '0';
    process.env.AWB_FAKE_EXIT_CODE = '0';

    // Envelope-native chat_request shape (Plan 04-01 server produces this)
    const chatEvent = JSON.stringify({
      event_type: 'chat_request',
      scope: { agent_id: 'agent-42' },
      payload: {
        agent_id: 'agent-42',
        user_id: 'user-1',
        ticket_id: null,
        role_prompt: 'You are a chatty helper',
        new_message: 'Hello agent, how are you?',
        history: [
          { message_id: 'm1', sender_type: 'user', content: 'hi', created_at: '2026-04-10T00:00:00Z' },
          { message_id: 'm2', sender_type: 'agent', content: 'hello', created_at: '2026-04-10T00:00:01Z' },
        ],
      },
      timestamp: '2026-04-10T00:00:02Z',
    });

    await stream._testDispatchChatRequest(chatEvent);

    assert.equal(spawnCalls.length, 1);
    assert.equal(spawnCalls[0].kind, 'chat');
    assert.equal(spawnCalls[0].rolePrompt, 'You are a chatty helper');
    assert.equal(spawnCalls[0].agentId, 'agent-42');
    assert.ok(
      spawnCalls[0].taskText.includes('Hello agent, how are you?'),
      'taskText should include new_message',
    );
    assert.ok(
      spawnCalls[0].taskText.includes('send_chat_message'),
      'taskText should instruct reply via send_chat_message',
    );

    await sleep(1500);

    const dispatches = cap.notifications.filter(
      (n) => n.meta?.type === 'subagent_dispatched' && n.meta?.subagent_kind === 'chat',
    );
    assert.equal(dispatches.length, 1);

    const completes = cap.notifications.filter(
      (n) => n.meta?.type === 'subagent_complete' && n.meta?.subagent_kind === 'chat',
    );
    assert.equal(completes.length, 1);

    await mgr.stop();
  } finally {
    cap.restore();
  }
});

// ─── Scenario 3: Legacy fallback when delegation disabled ──
test('legacy fallback when delegation.enabled=false', async () => {
  const cap = captureChannelNotifications();
  try {
    const mgr = new SubagentManager(makeConfig({ enabled: false }));
    await mgr.init();

    const spawnCalls = [];
    const origSpawn = mgr.spawn.bind(mgr);
    mgr.spawn = async (spec) => { spawnCalls.push(spec); return origSpawn(spec); };

    const stream = new EventStream(makeConfig({ enabled: false }), mgr);

    const trigger = JSON.stringify({
      board_id: '__trigger__',
      event_type: 'agent_trigger',
      ticket_id: 'tkt-legacy',
      action: 'assignee',
      field_changed: 'trg-legacy-1',
      actor_name: 'agent-42',
      role_prompt: 'role',
      ticket_prompt: 'tp',
      timestamp: '2026-04-10T00:00:00Z',
    });
    await stream._testDispatchTrigger(trigger);

    assert.equal(spawnCalls.length, 0, 'spawn should NOT be called when delegation disabled');

    // Legacy fallback MUST have fired
    const legacy = cap.notifications.filter((n) => n.meta?.type === 'agent_trigger');
    assert.equal(legacy.length, 1, 'expected 1 legacy agent_trigger notification');
    assert.equal(legacy[0].meta.ticket_id, 'tkt-legacy');

    const dispatches = cap.notifications.filter((n) => n.meta?.type === 'subagent_dispatched');
    assert.equal(dispatches.length, 0, 'no subagent_dispatched should fire when delegation disabled');

    await mgr.stop();
  } finally {
    cap.restore();
  }
});

// ─── Scenario 4: Legacy fallback when cap reached ──────────
test('legacy fallback when concurrency cap reached', async () => {
  const cap = captureChannelNotifications();
  try {
    installFetchStub({ id: 'tkt-cap', title: 't', description: 'd', comments: [] });

    const mgr = new SubagentManager(makeConfig({ maxConcurrent: 1 }));
    await mgr.init();

    const stream = new EventStream(makeConfig({ maxConcurrent: 1 }), mgr);

    // First trigger: keep it running to occupy the only slot
    process.env.AWB_FAKE_SLEEP = '10';
    process.env.AWB_FAKE_EXIT_CODE = '0';

    const trigger1 = JSON.stringify({
      event_type: 'agent_trigger',
      ticket_id: 'tkt-cap',
      action: 'a',
      field_changed: 'trg-cap-1',
      actor_name: 'agent-42',
      role_prompt: 'r',
      ticket_prompt: 't',
      timestamp: '2026-04-10T00:00:00Z',
    });
    await stream._testDispatchTrigger(trigger1);

    // Now cap is full. Second trigger should fall back to legacy.
    const trigger2 = JSON.stringify({
      event_type: 'agent_trigger',
      ticket_id: 'tkt-cap-2',
      action: 'a',
      field_changed: 'trg-cap-2',
      actor_name: 'agent-42',
      role_prompt: 'r',
      ticket_prompt: 't',
      timestamp: '2026-04-10T00:00:01Z',
    });
    await stream._testDispatchTrigger(trigger2);

    const legacy = cap.notifications.filter((n) => n.meta?.type === 'agent_trigger');
    assert.ok(legacy.length >= 1, 'expected at least 1 legacy fallback notification');
    assert.ok(
      legacy.some((n) => n.meta.ticket_id === 'tkt-cap-2'),
      'cap-2 should have gone through legacy path',
    );

    // Exactly one tracked subagent (the long-running one)
    const snap = mgr._snapshot();
    assert.equal(snap.length, 1, 'expected 1 tracked subagent (second trigger fell back)');

    await mgr.stop();
    restoreFetch();
  } finally {
    cap.restore();
  }
});

// ─── Scenario 5: Trigger dedup ─────────────────────────────
test('trigger dedup: same trigger_id twice → one spawn, one legacy fallback', async () => {
  const cap = captureChannelNotifications();
  try {
    installFetchStub({ id: 'tkt-dup', title: 't', description: 'd', comments: [] });

    const mgr = new SubagentManager(makeConfig({ maxConcurrent: 5 }));
    await mgr.init();

    const stream = new EventStream(makeConfig({ maxConcurrent: 5 }), mgr);

    // Keep the first one running so the second dispatch sees the dedup map
    process.env.AWB_FAKE_SLEEP = '10';
    process.env.AWB_FAKE_EXIT_CODE = '0';

    const trigger = JSON.stringify({
      event_type: 'agent_trigger',
      ticket_id: 'tkt-dup',
      action: 'a',
      field_changed: 'trg-dup-same',
      actor_name: 'agent-42',
      role_prompt: 'r',
      ticket_prompt: 't',
      timestamp: '2026-04-10T00:00:00Z',
    });

    await stream._testDispatchTrigger(trigger);
    await stream._testDispatchTrigger(trigger);

    // Exactly one subagent running in the manager (dedup kicked in)
    const snap = mgr._snapshot();
    assert.equal(snap.length, 1, 'expected exactly 1 tracked subagent after dedup');

    // Second dispatch should have fallen through to legacy
    const legacy = cap.notifications.filter(
      (n) => n.meta?.type === 'agent_trigger' && n.meta.trigger_id === 'trg-dup-same',
    );
    assert.equal(legacy.length, 1, 'expected 1 legacy fallback for the deduped trigger');

    // And only one subagent_dispatched (for the first successful spawn)
    const dispatches = cap.notifications.filter(
      (n) => n.meta?.type === 'subagent_dispatched' && n.meta.trigger_id === 'trg-dup-same',
    );
    assert.equal(dispatches.length, 1, 'expected 1 subagent_dispatched for the first call');

    await mgr.stop();
    restoreFetch();
  } finally {
    cap.restore();
  }
});

// ─── Scenario 6: TTL timeout → TIMED OUT completion ────────
test('TTL timeout → SIGTERM → TIMED OUT completion notification', async () => {
  const cap = captureChannelNotifications();
  try {
    installFetchStub({ id: 'tkt-ttl', title: 't', description: 'd', comments: [] });

    const mgr = new SubagentManager(makeConfig({ maxConcurrent: 5 }));
    await mgr.init();

    // Install the Plan 04-03 onExit completion notification handler directly for this
    // test (normally runProxy() wires it; here we replicate the logic inline so the
    // test is hermetic).
    mgr.onExit = ({ pid, record, code, signal, durationSec }) => {
      const label = record.kind === 'chat' ? 'Chat Subagent' : 'Subagent';
      let msg;
      if (signal === 'SIGTERM' || signal === 'SIGKILL') {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} TIMED OUT after ${durationSec}s`;
      } else if (code === 0) {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} completed (duration=${durationSec}s)`;
      } else {
        msg = `[AWB ${label}] ticket=${record.ticket_id || '-'} FAILED (exit=${code}, duration=${durationSec}s)`;
      }
      process.stdout.write(JSON.stringify({
        jsonrpc: '2.0',
        method: 'notifications/claude/channel',
        params: {
          content: msg,
          meta: {
            type: 'subagent_complete', subagent_kind: record.kind, ticket_id: record.ticket_id,
            trigger_id: record.trigger_id, pid, exit_code: code, signal, duration_sec: durationSec,
          },
        },
      }) + '\n');
    };

    const stream = new EventStream(makeConfig({ maxConcurrent: 5 }), mgr);

    // Long-running child so we can simulate the TTL kill path below
    process.env.AWB_FAKE_SLEEP = '30';
    process.env.AWB_FAKE_EXIT_CODE = '0';

    const trigger = JSON.stringify({
      event_type: 'agent_trigger',
      ticket_id: 'tkt-ttl',
      action: 'a',
      field_changed: 'trg-ttl-1',
      actor_name: 'agent-42',
      role_prompt: 'r',
      ticket_prompt: 't',
      timestamp: '2026-04-10T00:00:00Z',
    });
    await stream._testDispatchTrigger(trigger);

    const snap = mgr._snapshot();
    assert.equal(snap.length, 1);
    const pid = snap[0].pid;

    // Simulate TTL kill: the real setInterval sweep would SIGTERM overdue PIDs after
    // ttlMinutes. Sending SIGTERM directly exercises the exit-handler path end-to-end,
    // which is what the sweep ultimately triggers, without waiting 60s for the timer.
    process.kill(pid, 'SIGTERM');
    await sleep(2000);

    const timeouts = cap.notifications.filter(
      (n) => n.meta?.type === 'subagent_complete' && String(n.content).includes('TIMED OUT'),
    );
    assert.equal(timeouts.length, 1, 'expected 1 TIMED OUT completion notification');
    assert.equal(timeouts[0].meta.ticket_id, 'tkt-ttl');

    await mgr.stop();
    restoreFetch();
  } finally {
    cap.restore();
  }
});
