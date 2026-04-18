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
import { accessSync, constants as fsConstants, readlinkSync } from 'fs';
import { homedir } from 'os';
import { join, basename } from 'path';
import { log } from './logging.mjs';

const isWindows = process.platform === 'win32';
// On Windows, accessSync(X_OK) returns true for any readable file — including
// extensionless bash-style wrappers that Windows cannot execute directly
// (spawn without shell:true ENOENTs). Gate acceptance on a Windows-executable
// extension so `where claude` doesn't hand us the MSYS/Bash wrapper.
const WIN_EXEC_EXT = /\.(exe|cmd|bat|ps1)$/i;

function canExec(p) {
  if (!p) return false;
  if (isWindows && !WIN_EXEC_EXT.test(p)) return false;
  try { accessSync(p, fsConstants.X_OK); return true; } catch { return false; }
}

/**
 * Resolve the Claude CLI binary by inspecting the parent process — we're a
 * child of `claude` (MCP server spawned by the CLI), so /proc/{ppid}/exe on
 * Linux points right at its executable even when it lives somewhere exotic
 * (non-PATH npm install, systemd, docker). Returns null on macOS/Windows
 * (no /proc) or if the symlink doesn't resolve to a claude binary.
 *
 * VS Code extension bundle paths are intentionally rejected — we don't want
 * the plugin to silently depend on the IDE extension being installed.
 */
function parentClaudeBin() {
  try {
    const ppid = process.ppid;
    if (!ppid) return null;
    const exe = readlinkSync(`/proc/${ppid}/exe`);
    if (!exe || !canExec(exe)) return null;
    // Hard-reject VS Code extension bundle regardless of basename match.
    if (/\.vscode\/extensions\//.test(exe)) return null;
    const name = basename(exe);
    // Accept `claude`, `claude-{suffix}`, or any basename containing `claude`.
    if (/claude/i.test(name)) return exe;
    return null;
  } catch {
    return null;
  }
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
  //    (systemd, docker, non-PATH npm install) because it's direct filesystem
  //    inspection, not a PATH lookup. No-op on Windows/macOS.
  const viaParent = parentClaudeBin();
  if (viaParent) {
    cached = viaParent;
    log(`[claude-bin] resolved via parent /proc/${process.ppid}/exe: ${viaParent}`);
    return cached;
  }

  // 3. Ask the shell. Picks up `claude` wherever the user's login shell
  //    finds it, which may differ from the MCP proxy's inherited PATH.
  //    Uses cmd.exe on Windows, /bin/sh elsewhere. On Windows `where` returns
  //    every matching name (bash wrapper + .cmd + .exe …); scan all lines and
  //    prefer .exe, because canExec rejects the extensionless bash wrapper.
  try {
    const cmd = isWindows ? 'where claude' : 'command -v claude 2>/dev/null || which claude 2>/dev/null';
    const out = execSync(cmd, {
      encoding: 'utf8',
      timeout: 2000,
      shell: isWindows ? undefined : '/bin/sh',
    }).trim();
    const lines = out.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    const execCandidates = isWindows
      ? [...lines.filter((l) => /\.exe$/i.test(l)), ...lines.filter((l) => /\.(cmd|bat|ps1)$/i.test(l))]
      : lines;
    for (const cand of execCandidates) {
      if (canExec(cand)) {
        cached = cand;
        log(`[claude-bin] resolved via shell: ${cand}`);
        return cached;
      }
    }
  } catch { /* shell or spawn failed, keep trying */ }

  // 4. Platform-specific common install locations.
  const home = homedir();
  const candidates = isWindows
    ? windowsCandidates(home)
    : unixCandidates(home);
  for (const p of candidates) {
    if (canExec(p)) {
      cached = p;
      log(`[claude-bin] resolved via candidate: ${p}`);
      return cached;
    }
  }

  // 5. Give up — return literal. Caller's error listener will absorb ENOENT.
  cached = 'claude';
  log('[claude-bin] resolution failed; falling back to literal "claude" (expect ENOENT unless PATH is set)');
  return cached;
}

function unixCandidates(home) {
  return [
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
}

function windowsCandidates(home) {
  // @anthropic-ai/claude-code on Windows installs an actual claude.exe inside
  // the package bin dir (that's the layout the user pointed at:
  // C:\Users\<user>\AppData\Roaming\npm\...\@anthropic-ai\claude-code\bin).
  // Prefer .exe everywhere — direct spawn, no shell, no arg-escaping risk.
  // Only fall back to .cmd/.js if no .exe exists on disk.
  const appdata = process.env.APPDATA || join(home, 'AppData', 'Roaming');
  const localAppData = process.env.LOCALAPPDATA || join(home, 'AppData', 'Local');
  const pkgBin = join(appdata, 'npm', 'node_modules', '@anthropic-ai', 'claude-code', 'bin');
  return [
    // .exe first — ordered by likelihood the user hits it
    join(pkgBin, 'claude.exe'),
    join(appdata, 'npm', 'claude.exe'),
    join(localAppData, 'Programs', 'anthropic', 'claude-code', 'claude.exe'),
    // .cmd/.ps1 shims — only if no .exe exists; caller spawns with shell:true
    join(appdata, 'npm', 'claude.cmd'),
    join(pkgBin, 'claude.cmd'),
    join(appdata, 'npm', 'claude.ps1'),
  ];
}
