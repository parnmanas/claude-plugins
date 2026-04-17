// ─── Error Log Uploader ───────────────────────────────────
// Scans the local proxy.log for error/warn/fatal lines since the last upload
// marker and POSTs them to AWB's /api/agent/error-logs so they show up in the
// admin Agent Logs viewer. Dormant for success-only sessions.

import { readFile, writeFile } from 'fs/promises';
import { join } from 'path';
import { homedir } from 'os';
import { log } from './logging.mjs';

const CONFIG_DIR = process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude');
const LOG_PATH = join(CONFIG_DIR, 'channels', 'awb', 'proxy.log');
const MARKER_PATH = join(CONFIG_DIR, 'channels', 'awb', 'error-upload.json');

const LINE_RE = /^\[([^\]]+)\] \[pid=([^\]]+)\] (.+)$/;
const MAX_ENTRIES = 500;
const DEFAULT_LOOKBACK_MS = 24 * 60 * 60 * 1000;

/** Classify a raw log message. Returns null to skip (success/noise). */
function classify(msg) {
  if (/^Uncaught error:|^Unhandled rejection:/.test(msg)) return { level: 'fatal', category: 'crash' };
  if (/^EXIT code=[1-9]/.test(msg)) return { level: 'fatal', category: 'crash' };
  if (/^SSE error:/.test(msg)) return { level: 'error', category: 'sse' };
  if (/^Presence ping failed:/.test(msg)) return { level: 'error', category: 'presence' };
  if (/stdout error:|EPIPE/.test(msg)) return { level: 'error', category: 'ipc' };
  if (/result subtype=error|is_error=true/.test(msg)) return { level: 'error', category: 'subagent' };
  if (/error|failed/i.test(msg)) return { level: 'warn', category: 'misc' };
  return null;
}

export async function scanErrorsSince(logPath, sinceMs) {
  let text;
  try { text = await readFile(logPath, 'utf8'); }
  catch { return []; }
  const out = [];
  for (const line of text.split('\n')) {
    const m = LINE_RE.exec(line);
    if (!m) continue;
    const [_, isoTs, pid, msg] = m;
    const ms = Date.parse(isoTs);
    if (!Number.isFinite(ms) || ms <= sinceMs) continue;
    const klass = classify(msg);
    if (!klass) continue;
    out.push({
      occurred_at: new Date(ms).toISOString(),
      level: klass.level,
      category: klass.category,
      message: msg.slice(0, 2000),
      raw_line: line.slice(0, 4000),
      pid,
    });
  }
  return out.slice(-MAX_ENTRIES); // keep last N if oversized
}

async function readMarker() {
  try {
    const raw = await readFile(MARKER_PATH, 'utf8');
    const j = JSON.parse(raw);
    return typeof j.last_occurred_at === 'string' ? Date.parse(j.last_occurred_at) : null;
  } catch { return null; }
}

async function writeMarker(agentId, lastOccurredAt, uploadedAt) {
  try {
    await writeFile(MARKER_PATH, JSON.stringify({
      agent_id: agentId,
      last_occurred_at: lastOccurredAt,
      last_uploaded_at: uploadedAt,
    }, null, 2));
  } catch (err) {
    log(`[uploader] marker write failed: ${err.message}`);
  }
}

export async function uploadIfNewErrors(config, agentId, pluginVersion) {
  if (!config?.url || !config?.apiKey || !agentId) return { uploaded: 0, reason: 'missing_config' };
  const markerMs = await readMarker();
  const sinceMs = markerMs ?? (Date.now() - DEFAULT_LOOKBACK_MS);
  const entries = await scanErrorsSince(LOG_PATH, sinceMs);
  if (entries.length === 0) return { uploaded: 0, reason: 'no_new_errors' };

  const url = `${config.url.replace(/\/$/, '')}/api/agent/error-logs`;
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Agent-Key': config.apiKey,
      },
      body: JSON.stringify({
        agent_id: agentId,
        workspace_id: config.workspace_id ?? null,
        plugin_version: pluginVersion,
        entries,
      }),
    });
    if (!resp.ok) {
      log(`[uploader] upload failed: HTTP ${resp.status}`);
      return { uploaded: 0, reason: `http_${resp.status}` };
    }
    const data = await resp.json().catch(() => ({}));
    const lastOccurredAt = data.last_occurred_at ?? entries[entries.length - 1].occurred_at;
    const uploadedAt = data.uploaded_at ?? new Date().toISOString();
    await writeMarker(agentId, lastOccurredAt, uploadedAt);
    log(`[uploader] uploaded ${data.accepted ?? entries.length} entries, marker=${lastOccurredAt}`);
    return { uploaded: entries.length, last_occurred_at: lastOccurredAt };
  } catch (err) {
    log(`[uploader] upload error: ${err.message}`);
    return { uploaded: 0, reason: 'network_error' };
  }
}
