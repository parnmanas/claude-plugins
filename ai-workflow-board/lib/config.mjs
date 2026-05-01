// ─── Config loader ────────────────────────────────────────
// Reads ~/.claude/channels/awb/config.json. Returns null if the file is
// missing or malformed so the proxy can fall back to its unconfigured loop.

import { readFileSync, existsSync } from 'fs';
import { CONFIG_PATH } from './constants.mjs';

export function loadConfig() {
  if (!existsSync(CONFIG_PATH)) return null;
  try {
    return JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
  } catch {
    return null;
  }
}
