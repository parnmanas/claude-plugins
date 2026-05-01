// ─── File logger + minimal crash instrumentation ──────────
// Persists proxy logs to disk and wires a few global handlers so anything
// pushing the process toward exit leaves a trace. Kept import-light so it
// can be pulled in everywhere without circular-import risk.

import { appendFileSync, mkdirSync, renameSync, statSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';

const LOG_DIR = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb',
);
const LOG_PATH = join(LOG_DIR, 'proxy.log');
const LOG_MAX_BYTES = 5 * 1024 * 1024;

try { mkdirSync(LOG_DIR, { recursive: true }); } catch { /* ignore */ }

function writeLogLine(line) {
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

process.on('uncaughtException', (err) => {
  log(`Uncaught error: ${err?.stack || err?.message || err}`);
});
process.on('unhandledRejection', (err) => {
  log(`Unhandled rejection: ${err?.stack || err?.message || err}`);
});
process.on('exit', (code) => {
  writeLogLine(`[${new Date().toISOString()}] [pid=${process.pid}] EXIT code=${code}\n`);
});

// EPIPE on stdout means Claude CLI closed its read end — no point staying up.
process.stdout.on('error', (err) => {
  if (err?.code === 'EPIPE') process.exit(0);
  log(`stdout error: code=${err?.code} msg=${err?.message}`);
});
process.stderr.on('error', () => { /* swallow; stderr loss is non-fatal */ });

export function send(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}

export function sendError(id, code, message) {
  send({ jsonrpc: '2.0', id: id ?? null, error: { code, message } });
}
