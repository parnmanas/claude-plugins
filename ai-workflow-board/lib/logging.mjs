// ─── File logger + crash instrumentation ──────────────────
// Persists proxy logs to disk and wires global crash/exit listeners so that
// anything pushing the process toward termination leaves a trace. Keep this
// module's import surface tiny (no class imports) so it can be imported from
// everywhere without circular-import risk.

import { appendFileSync, mkdirSync, renameSync, statSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';

// Private — intentionally NOT exported. Callers log through log()/send().
const LOG_DIR = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb',
);
const LOG_PATH = join(LOG_DIR, 'proxy.log');
const LOG_MAX_BYTES = 5 * 1024 * 1024;

try { mkdirSync(LOG_DIR, { recursive: true }); } catch { /* ignore */ }

export function writeLogLine(line) {
  try {
    const st = statSync(LOG_PATH);
    if (st.size > LOG_MAX_BYTES) {
      try { renameSync(LOG_PATH, LOG_PATH + '.1'); } catch { /* ignore */ }
    }
  } catch { /* file may not exist yet */ }
  try { appendFileSync(LOG_PATH, line); } catch { /* disk full, readonly fs, etc. */ }
}

export function log(msg) {
  const line = `[${new Date().toISOString()}] [pid=${process.pid}] ${msg}\n`;
  try { process.stderr.write(`[AWB] ${msg}\n`); } catch { /* ignore */ }
  writeLogLine(line);
}

// ─── Crash / exit instrumentation ─────────────────────────
// Claude CLI keeps MCP servers on stdio pipes; if anything pushes this process
// toward exit we want the cause recorded. `exit` is sync-only, so the final
// line is written via appendFileSync. SIGPIPE on stdout is handled explicitly
// because an unhandled EPIPE kills Node by default.

process.on('uncaughtException', (err) => {
  log(`Uncaught error: ${err?.stack || err?.message || err}`);
});
process.on('unhandledRejection', (err) => {
  log(`Unhandled rejection: ${err?.stack || err?.message || err}`);
});
process.on('exit', (code) => {
  writeLogLine(`[${new Date().toISOString()}] [pid=${process.pid}] EXIT code=${code}\n`);
});
for (const sig of ['SIGTERM', 'SIGINT', 'SIGHUP', 'SIGPIPE']) {
  process.on(sig, () => {
    log(`Received ${sig}`);
    if (sig !== 'SIGPIPE') process.exit(0);
  });
}
process.stdout.on('error', (err) => {
  log(`stdout error: code=${err?.code} msg=${err?.message} stack=${err?.stack?.split('\n')[1] || ''}`);
  // EPIPE usually means Claude CLI closed its read end — no point staying up.
  if (err?.code === 'EPIPE') process.exit(0);
});
process.stderr.on('error', () => { /* swallow; stderr loss is non-fatal */ });

// DIAG v0.6.9: trace stdin lifecycle — fires BEFORE rl.on('close') so we can see
// whether Claude CLI sent any final line or just dropped the pipe silently.
process.stdin.on('end', () => log('[DIAG] stdin end event (EOF from Claude CLI)'));
process.stdin.on('error', (err) => log(`[DIAG] stdin error: code=${err?.code} msg=${err?.message}`));
process.stdin.on('close', () => log('[DIAG] stdin close event'));

export function send(obj) {
  const payload = JSON.stringify(obj) + '\n';
  // DIAG v0.6.9: record every outbound write so we can correlate with stdin close.
  // method + id/params-type + byte length; truncate content to keep log small.
  try {
    const method = obj?.method || (obj?.error ? 'error' : obj?.result ? 'result' : '?');
    const metaType = obj?.params?.meta?.type ?? '';
    log(`[DIAG] stdout.write method=${method} metaType=${metaType} bytes=${Buffer.byteLength(payload)}`);
  } catch { /* ignore diag failure */ }
  const ok = process.stdout.write(payload);
  if (!ok) log('[DIAG] stdout.write returned false (backpressure)');
}

export function sendError(id, code, message) {
  send({ jsonrpc: '2.0', id: id ?? null, error: { code, message } });
}

/** Send a channel notification to Claude — the core delivery mechanism */
export function sendChannelEvent(content, meta = {}) {
  send({
    jsonrpc: '2.0',
    method: 'notifications/claude/channel',
    params: { content, meta },
  });
}
