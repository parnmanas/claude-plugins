// Tests for McpForwardSession — forward session lifecycle, stale-session
// recovery, network retries, keepalive. Spins up a lightweight HTTP server
// that mimics AWB's MCP Streamable HTTP behavior around session TTL.

import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtempSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

const TMP_BASE = mkdtempSync(join(tmpdir(), 'awb-forward-sess-test-'));
process.env.CLAUDE_CONFIG_DIR = TMP_BASE;
mkdirSync(join(TMP_BASE, 'channels', 'awb'), { recursive: true });
writeFileSync(
  join(TMP_BASE, 'channels', 'awb', 'config.json'),
  JSON.stringify({ url: 'http://localhost:0', apiKey: 'test-key' }),
);

const { McpForwardSession } = await import('../proxy.mjs');

/**
 * Fake AWB MCP server. Mirrors the parts of the real server the forward
 * session talks to: issues a session id on initialize, 404s stale session
 * requests with the marker text, supports tools/list and tools/call.
 */
function startFakeServer({ initialSessionId = null } = {}) {
  const state = {
    validSessions: new Set(),
    requestLog: [],
    // Hooks so individual tests can inject behavior without forking the server.
    failNextN: 0,           // # of requests to 500 before succeeding
    invalidateAfterInit: false,
    currentSessionId: initialSessionId,
  };

  const server = createServer((req, res) => {
    let chunks = '';
    req.on('data', (c) => { chunks += c; });
    req.on('end', () => {
      let msg = {};
      try { msg = JSON.parse(chunks); } catch { /* tolerate */ }
      const sid = req.headers['mcp-session-id'];
      state.requestLog.push({ method: msg.method, sid, id: msg.id });

      // Transient 500 injection (for retry tests)
      if (state.failNextN > 0) {
        state.failNextN -= 1;
        res.statusCode = 502;
        res.end('upstream boom');
        return;
      }

      if (msg.method === 'initialize') {
        const newSid = randomUUID();
        state.validSessions.add(newSid);
        state.currentSessionId = newSid;
        if (state.invalidateAfterInit) {
          // Simulate a server that accepts initialize but then drops the session
          // before the next request — exercises the "second-pass stale" guard.
          state.invalidateAfterInit = false;
          state.validSessions.delete(newSid);
        }
        res.setHeader('mcp-session-id', newSid);
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({
          jsonrpc: '2.0', id: msg.id,
          result: {
            protocolVersion: '2025-03-26',
            capabilities: { tools: {} },
            serverInfo: { name: 'fake-awb', version: '1.0.0' },
          },
        }));
        return;
      }

      if (msg.method === 'notifications/initialized') {
        res.statusCode = 204;
        res.end();
        return;
      }

      // Stale session: matches real server's 404 + "Session not found. Please re-initialize."
      if (!sid || !state.validSessions.has(sid)) {
        res.statusCode = 404;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({
          jsonrpc: '2.0',
          error: { code: -32000, message: 'Session not found. Please re-initialize.' },
          id: null,
        }));
        return;
      }

      // Happy path tool call
      res.setHeader('mcp-session-id', sid);
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({
        jsonrpc: '2.0', id: msg.id,
        result: { ok: true, method: msg.method },
      }));
    });
  });

  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        url: `http://127.0.0.1:${port}/mcp`,
        close: () => new Promise((r) => server.close(r)),
        state,
        invalidateAllSessions: () => state.validSessions.clear(),
      });
    });
  });
}

test('initialize caches session + subsequent forward uses it', async () => {
  const srv = await startFakeServer();
  const session = new McpForwardSession(srv.url, 'test-key');
  try {
    const init = await session.handleClaudeInitialize({
      jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-03-26' },
    });
    assert.ok(init.body?.result, 'initialize body returned');
    const sid = session.sessionId;
    assert.ok(sid, 'session id captured');

    const res = await session.forward({ jsonrpc: '2.0', id: 2, method: 'tools/list' });
    assert.equal(res.body?.result?.ok, true);
    assert.equal(session.sessionId, sid, 'session id preserved after normal call');
  } finally {
    session.stop();
    await srv.close();
  }
});

test('stale session triggers silent re-init and transparent retry', async () => {
  const srv = await startFakeServer();
  const session = new McpForwardSession(srv.url, 'test-key');
  try {
    await session.handleClaudeInitialize({
      jsonrpc: '2.0', id: 1, method: 'initialize', params: {},
    });
    const originalSid = session.sessionId;

    // Simulate AWB's 10-min idle sweep: server evicts our session.
    srv.invalidateAllSessions();

    // Claude CLI's periodic tools/list — this is the request that used to kill
    // the proxy. It should now succeed transparently.
    const res = await session.forward({ jsonrpc: '2.0', id: 42, method: 'tools/list' });
    assert.equal(res.body?.result?.ok, true, 'forward succeeded after stale-session recovery');
    assert.notEqual(session.sessionId, originalSid, 'session id rotated to fresh one');

    // Verify the log shows: stale attempt -> re-init -> successful retry
    const methods = srv.state.requestLog.map((r) => r.method);
    assert.deepEqual(
      methods.slice(-3),
      ['tools/list', 'initialize', 'tools/list'],
      'expected stale -> re-init -> retry sequence',
    );
  } finally {
    session.stop();
    await srv.close();
  }
});

test('network 5xx retries with backoff then succeeds', async () => {
  const srv = await startFakeServer();
  const session = new McpForwardSession(srv.url, 'test-key');
  try {
    await session.handleClaudeInitialize({
      jsonrpc: '2.0', id: 1, method: 'initialize', params: {},
    });

    srv.state.failNextN = 2; // first 2 forwards return 502, 3rd succeeds
    const t0 = Date.now();
    const res = await session.forward({ jsonrpc: '2.0', id: 99, method: 'tools/list' });
    const elapsed = Date.now() - t0;
    assert.equal(res.body?.result?.ok, true, 'succeeded after transient failures');
    // First backoff 1000ms, second 2000ms. Expect >= ~3000ms but keep slack.
    assert.ok(elapsed >= 2800, `expected retries to add ~3s backoff, got ${elapsed}ms`);
  } finally {
    session.stop();
    await srv.close();
  }
});

test('repeated stale after re-init bails instead of looping forever', async () => {
  const srv = await startFakeServer();
  const session = new McpForwardSession(srv.url, 'test-key');
  try {
    await session.handleClaudeInitialize({
      jsonrpc: '2.0', id: 1, method: 'initialize', params: {},
    });

    // Make every subsequent request stale — even post-reinit. We must not loop.
    srv.state.invalidateAfterInit = true;
    srv.invalidateAllSessions();

    await assert.rejects(
      session.forward({ jsonrpc: '2.0', id: 7, method: 'tools/list' }),
      /repeatedly stale|Forward error/i,
    );
  } finally {
    session.stop();
    await srv.close();
  }
});

test('concurrent forwards share one re-init handshake', async () => {
  const srv = await startFakeServer();
  const session = new McpForwardSession(srv.url, 'test-key');
  try {
    await session.handleClaudeInitialize({
      jsonrpc: '2.0', id: 1, method: 'initialize', params: {},
    });
    srv.invalidateAllSessions();

    // Fire three parallel forwards — they all hit stale and should dedupe on
    // one re-init call (not three).
    const [a, b, c] = await Promise.all([
      session.forward({ jsonrpc: '2.0', id: 10, method: 'tools/list' }),
      session.forward({ jsonrpc: '2.0', id: 11, method: 'tools/list' }),
      session.forward({ jsonrpc: '2.0', id: 12, method: 'tools/list' }),
    ]);
    assert.equal(a.body?.result?.ok, true);
    assert.equal(b.body?.result?.ok, true);
    assert.equal(c.body?.result?.ok, true);

    const initCount = srv.state.requestLog.filter((r) => r.method === 'initialize').length;
    // 1 from handleClaudeInitialize + exactly 1 from concurrent re-init dedup.
    // (sid-at-request snapshot guards against races where a concurrent call
    // rotates the sid between our doFetch and our stale-handler branch.)
    assert.equal(initCount, 2, `expected dedup to a single re-init, observed ${initCount}`);
  } finally {
    session.stop();
    await srv.close();
  }
});
