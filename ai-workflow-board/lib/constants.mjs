// ─── AWB Proxy Constants ──────────────────────────────────
// Shared values used by proxy.mjs and lib/. Keep this file free of runtime
// side effects so it can be imported anywhere without circular-import risk.

import { join } from 'path';
import { homedir } from 'os';

export const CONFIG_PATH = join(
  process.env.CLAUDE_CONFIG_DIR ?? join(homedir(), '.claude'),
  'channels', 'awb', 'config.json',
);

export const REQUEST_TIMEOUT_MS = 30000;
