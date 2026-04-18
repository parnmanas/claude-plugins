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
import { accessSync, constants as fsConstants, readlinkSync, readdirSync } from 'fs';
import { homedir } from 'os';
import { join, basename } from 'path';
import { log } from './logging.mjs';

function canExec(p) {
  if (!p) return false;
  try { accessSync(p, fsConstants.X_OK); return true; } catch { return false; }
}

/**
 * Resolve the Claude CLI binary by inspecting the parent process — we're a
 * child of `claude` (MCP server spawned by the CLI), so /proc/{ppid}/exe on
 * Linux points right at its executable even when it lives somewhere exotic
 * (VS Code extension bundle, non-PATH npm install, etc). Returns null on
 * macOS/Windows (no /proc) or if the symlink doesn't resolve to something
 * recognizable as claude.
 */
function parentClaudeBin() {
  try {
    const ppid = process.ppid;
    if (!ppid) return null;
    const exe = readlinkSync(`/proc/${ppid}/exe`);
    if (!exe || !canExec(exe)) return null;
    const name = basename(exe);
    // Accept `claude`, `claude-{suffix}`, or any path whose final segment
    // contains `claude` (covers VS Code extension native-binary/claude).
    if (/claude/i.test(name)) return exe;
    return null;
  } catch {
    return null;
  }
}

/**
 * Scan ~/.vscode/extensions for the bundled Claude Code native binary. The
 * VS Code extension ships its own claude executable that isn't on PATH but
 * is the thing that spawned us when the user runs from the IDE.
 */
function vscodeExtensionClaudeBin() {
  try {
    const extDir = join(homedir(), '.vscode/extensions');
    for (const name of readdirSync(extDir)) {
      if (!/^anthropic\.claude-code/i.test(name)) continue;
      const candidate = join(extDir, name, 'resources/native-binary/claude');
      if (canExec(candidate)) return candidate;
    }
  } catch { /* dir missing, permission denied, etc. */ }
  return null;
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

  // 2. Parent process exe (Linux /proc trick). We are a child of claude — if
  //    the parent's /proc/{ppid}/exe resolves to a binary whose name contains
  //    "claude", that's the canonical path. Works even when PATH is stripped
  //    (VS Code extension, systemd, docker) because it's direct filesystem
  //    inspection, not a PATH lookup.
  const viaParent = parentClaudeBin();
  if (viaParent) {
    cached = viaParent;
    log(`[claude-bin] resolved via parent /proc/${process.ppid}/exe: ${viaParent}`);
    return cached;
  }

  // 3. VS Code extension bundled binary — covers the common case where the
  //    user runs Claude Code from the IDE and the bundled claude isn't on PATH.
  const viaVSCode = vscodeExtensionClaudeBin();
  if (viaVSCode) {
    cached = viaVSCode;
    log(`[claude-bin] resolved via VS Code extension: ${viaVSCode}`);
    return cached;
  }

  // 4. Ask the shell. Picks up `claude` wherever the user's login shell
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

  // 5. Common install locations
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

  // 6. Give up — return literal. Caller's error listener will absorb ENOENT.
  cached = 'claude';
  log('[claude-bin] resolution failed; falling back to literal "claude" (expect ENOENT unless PATH is set)');
  return cached;
}
