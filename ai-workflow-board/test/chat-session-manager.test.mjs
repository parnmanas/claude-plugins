// Tests for ChatSessionManager — v0.7.0 persistent per-room chat subagents.
// Uses fake-claude.sh as claudeBin so we don't need a real Claude CLI.

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const FAKE_CLAUDE = join(__dirname, 'fake-claude.sh');

const TMP_BASE = mkdtempSync(join(tmpdir(), 'awb-chat-sess-test-'));
process.env.CLAUDE_CONFIG_DIR = TMP_BASE;
mkdirSync(join(TMP_BASE, 'channels', 'awb'), { recursive: true });
writeFileSync(
  join(TMP_BASE, 'channels', 'awb', 'config.json'),
  JSON.stringify({ url: 'http://localhost:7793', apiKey: 'test-key' }),
);

const { ChatSessionManager, EventStream, DELEGATION_DEFAULTS } = await import('../proxy.mjs');

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

// Stub fetch so fetchChatRoomHistory doesn't hit a real server.
const origFetch = globalThis.fetch;
globalThis.fetch = async () => ({ ok: true, status: 200, async json() { return []; } });

test.after(() => {
  globalThis.fetch = origFetch;
  try { rmSync(TMP_BASE, { recursive: true, force: true }); } catch {}
});

test('two messages to the same room reuse one session, not two', async () => {
  const mgr = new ChatSessionManager(makeConfig({ maxConcurrent: 3 }));
  process.env.AWB_FAKE_SLEEP = '0';

  const r1 = await mgr.dispatch({
    roomId: 'room-A', senderId: 'u1', senderName: 'Alice',
    createdAt: '2026-04-16T00:00:00Z', content: 'hi', rolePrompt: 'be nice',
  });
  assert.equal(r1.dispatched, true);
  assert.equal(r1.firstTurn, true);

  const r2 = await mgr.dispatch({
    roomId: 'room-A', senderId: 'u1', senderName: 'Alice',
    createdAt: '2026-04-16T00:00:05Z', content: 'still there?', rolePrompt: 'be nice',
  });
  assert.equal(r2.dispatched, true);
  assert.equal(r2.firstTurn, undefined, 'second message should reuse existing session');
  assert.equal(r2.pid, r1.pid, 'same pid for same room');

  const snap = mgr._snapshot();
  assert.equal(snap.length, 1);
  assert.equal(snap[0].turnCount, 2);

  await mgr.stop();
  await sleep(200);
});

test('dedup: chat_request + chat_room_message for same (sender_id, created_at) → one dispatch', async () => {
  const mgr = new ChatSessionManager(makeConfig({ maxConcurrent: 3 }));
  process.env.AWB_FAKE_SLEEP = '0';

  const spec = {
    roomId: 'room-B', senderId: 'u7', senderName: 'Bob',
    createdAt: '2026-04-16T00:01:00Z', content: 'ping', rolePrompt: '',
  };
  const r1 = await mgr.dispatch(spec);
  const r2 = await mgr.dispatch(spec);
  assert.equal(r1.dispatched, true);
  assert.equal(r2.dispatched, false);
  assert.equal(r2.reason, 'duplicate_chat');

  const snap = mgr._snapshot();
  assert.equal(snap.length, 1);
  assert.equal(snap[0].turnCount, 1, 'only the first turn was written');

  await mgr.stop();
  await sleep(200);
});

test('idle TTL closes the session stdin and clears the map entry', async () => {
  // idleMinutes is minutes — coerce via float to get sub-second TTL for the test.
  const mgr = new ChatSessionManager(makeConfig({ maxConcurrent: 3, idleMinutes: 0.02 }));
  process.env.AWB_FAKE_SLEEP = '0';

  const r = await mgr.dispatch({
    roomId: 'room-C', senderId: 'u1', senderName: 'Cat',
    createdAt: '2026-04-16T00:02:00Z', content: 'hello', rolePrompt: '',
  });
  assert.equal(r.dispatched, true);
  assert.equal(mgr._snapshot().length, 1);

  // Wait past idle TTL (0.02 min = 1.2s). fake-claude exits on stdin close.
  await sleep(2500);
  assert.equal(mgr._snapshot().length, 0, 'session should have exited and been cleared');
});

test('maxTurnsPerSession closes stdin for respawn on next message', async () => {
  const mgr = new ChatSessionManager(makeConfig({ maxConcurrent: 3, maxTurnsPerSession: 2 }));
  process.env.AWB_FAKE_SLEEP = '0';

  const r1 = await mgr.dispatch({
    roomId: 'room-D', senderId: 'u1', senderName: 'Dan',
    createdAt: '2026-04-16T00:03:00Z', content: 'a', rolePrompt: '',
  });
  const pid1 = r1.pid;
  // Second turn hits maxTurns and closes stdin.
  await mgr.dispatch({
    roomId: 'room-D', senderId: 'u1', senderName: 'Dan',
    createdAt: '2026-04-16T00:03:01Z', content: 'b', rolePrompt: '',
  });
  // Wait for the child to exit after stdin close.
  await sleep(1000);
  assert.equal(mgr._snapshot().length, 0, 'session should have exited after maxTurns');

  // Third message should respawn with a new pid.
  const r3 = await mgr.dispatch({
    roomId: 'room-D', senderId: 'u1', senderName: 'Dan',
    createdAt: '2026-04-16T00:03:02Z', content: 'c', rolePrompt: '',
  });
  assert.equal(r3.dispatched, true);
  assert.equal(r3.firstTurn, true);
  assert.notEqual(r3.pid, pid1, 'respawn should produce a fresh pid');

  await mgr.stop();
  await sleep(200);
});

test('cap eviction: third room LRU-evicts the oldest-idle session', async () => {
  const mgr = new ChatSessionManager(makeConfig({ maxConcurrent: 2 }));
  process.env.AWB_FAKE_SLEEP = '0';

  await mgr.dispatch({
    roomId: 'room-E1', senderId: 'u1', senderName: 'E',
    createdAt: '2026-04-16T00:04:00Z', content: '1', rolePrompt: '',
  });
  await sleep(20); // ensure distinct lastTouchedAt ordering
  await mgr.dispatch({
    roomId: 'room-E2', senderId: 'u2', senderName: 'E',
    createdAt: '2026-04-16T00:04:01Z', content: '2', rolePrompt: '',
  });
  assert.equal(mgr._snapshot().length, 2);

  // Third room — must LRU-evict room-E1 (oldest-idle).
  const r3 = await mgr.dispatch({
    roomId: 'room-E3', senderId: 'u3', senderName: 'E',
    createdAt: '2026-04-16T00:04:02Z', content: '3', rolePrompt: '',
  });
  assert.equal(r3.dispatched, true);
  assert.equal(r3.firstTurn, true);

  // Give the evicted child time to exit so the map lands at 2.
  await sleep(800);
  const snap = mgr._snapshot();
  const rooms = snap.map((s) => s.roomId).sort();
  assert.equal(snap.length, 2);
  assert.ok(!rooms.includes('room-E1'), 'room-E1 should have been evicted');
  assert.ok(rooms.includes('room-E3'));

  await mgr.stop();
  await sleep(200);
});

test('EventStream wires chat_room_message into ChatSessionManager (one spawn, two turns)', async () => {
  const mgr = new ChatSessionManager(makeConfig({ maxConcurrent: 3 }));
  const stream = new EventStream(makeConfig({ maxConcurrent: 3 }), null, mgr);
  process.env.AWB_FAKE_SLEEP = '0';

  const mk = (content, ts) => JSON.stringify({
    event_type: 'chat_room_message',
    scope: { room_id: 'room-F' },
    payload: {
      room_id: 'room-F', message_id: `m-${ts}`,
      sender_type: 'user', sender_id: 'u9', sender_name: 'Fin',
      content, created_at: ts, role_prompt: '',
    },
    timestamp: ts,
  });

  await stream._testDispatchChatRoomMessage(mk('hello', '2026-04-16T00:05:00Z'));
  await stream._testDispatchChatRoomMessage(mk('are you there', '2026-04-16T00:05:01Z'));

  const snap = mgr._snapshot();
  assert.equal(snap.length, 1);
  assert.equal(snap[0].turnCount, 2, 'second message reused the live session');

  await mgr.stop();
  await sleep(200);
});
