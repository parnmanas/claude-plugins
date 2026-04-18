// ─── Claude Binary Resolver ───────────────────────────────
// Finds the `claude` CLI binary so subagent spawning doesn't depend on the
// inherited PATH. Claude Code often launches the MCP proxy with a minimal
// PATH (systemd, docker, plugin marketplace loader), so a literal 'claude'
// argv[0] to spawn() frequently ENOENTs even when the binary is clearly
// installed for the user. We resolve once at startup, cache, and inject
// the absolute path at every spawn site.
//
// Resolution order (first hit wins):
//   1. Explicit absolute path in config.delegation.claudeBin (caller override)
//   2. `command -v claude` in the proxy's own shell — catches PATH that may
//      differ from ours if the user exports it via a login shell
//   3. Common install locations for Node/Bun/system package managers
//   4. Fallback: literal 'claude' (will ENOENT; caller's error listener
//      absorbs it without crashing)

import { execSync } from 'child_process';
import { accessSync, constants as fsConstants } from 'fs';
import { homedir } from 'os';
import { join } from 'path';
import { log } from './logging.mjs';

function canExec(p) {
  if (!p) return false;
  try { accessSync(p, fsConstants.X_OK); return true; } catch { return false; }
}

let cached = null;

/**
 * Resolve the `claude` CLI to an absolute executable path. Result is cached
 * for the lifetime of the process. Pass a pre-resolved absolute path via
 * `configured` to bypass auto-detection.
 */
export function resolveClaudeBin(configured = 'claude') {
  if (cached) return cached;

  // 1. Explicit override: any non-default value is treated as user intent
  //    and trusted even if it's not currently executable (installer race, etc.)
  if (configured && configured !== 'claude') {
    cached = configured;
    log(`[claude-bin] using configured path: ${configured}`);
    return cached;
  }

  // 2. Ask the shell. This picks up `claude` wherever the user's login shell
  //    finds it, which may differ from the MCP proxy's inherited PATH.
  try {
    const out = execSync('command -v claude 2>/dev/null || which claude 2>/dev/null', {
      encoding: 'utf8',
      timeout: 2000,
      shell: '/bin/sh',
    }).trim();
    const first = out.split('\n')[0] || '';
    if (canExec(first)) {
      cached = first;
      log(`[claude-bin] resolved via shell: ${first}`);
      return cached;
    }
  } catch { /* shell or spawn failed, keep trying */ }

  // 3. Common install locations
  const home = homedir();
  const candidates = [
    join(home, '.npm-global/bin/claude'),
    join(home, '.bun/bin/claude'),
    join(home, '.local/bin/claude'),
    join(home, '.volta/bin/claude'),
    join(home, '.npm-packages/bin/claude'),
    join(home, 'node_modules/.bin/claude'),
    '/usr/local/bin/claude',
    '/opt/homebrew/bin/claude',
    '/usr/bin/claude',
  ];
  for (const p of candidates) {
    if (canExec(p)) {
      cached = p;
      log(`[claude-bin] resolved via candidate: ${p}`);
      return cached;
    }
  }

  // 4. Give up — return literal. Caller's error listener will absorb ENOENT.
  cached = 'claude';
  log('[claude-bin] resolution failed; falling back to literal "claude" (expect ENOENT unless PATH is set)');
  return cached;
}
