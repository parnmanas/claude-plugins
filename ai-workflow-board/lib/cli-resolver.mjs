// ─── CLI Binary Resolver (Phase 2 — multi-CLI generalization) ─────────
//
// Resolves any agent CLI's binary to an absolute executable path so spawn()
// doesn't depend on the inherited PATH. Originally `claude-bin-resolver.mjs`
// (claude-only); generalized in Phase 2 so non-claude adapters (gemini, …)
// share the same resolution strategy.
//
// Resolution strategy (per CLI, first hit wins):
//   1. Explicit absolute path passed by adapter (`configured` arg)
//   2. Parent process exe (Linux /proc/{ppid}/exe — only useful for claude
//      because the proxy is spawned by claude.exe; other adapters skip step 2)
//   3. `command -v <name>` / `where <name>` in the proxy's own shell
//   4. Per-CLI well-known install paths (windows / unix candidates)
//   5. Fallback: literal CLI name (will ENOENT; adapter's spawn error
//      listener absorbs it without crashing)
//
// Memory pin (`feedback_windows_claude_exe_only`): Windows resolution must
// reject `.cmd`/`.ps1` shims and the MSYS bash wrapper that ship next to the
// .exe — those don't spawn reliably. The Windows gate (WIN_EXEC_EXT) is
// preserved verbatim from the original claude-only resolver.
//
// Cache: per-CLI to avoid first-claude-then-gemini collisions in a single
// daemon process (Phase 2 ticket calls out same-machine claude+gemini).

import { execSync } from 'child_process';
import { accessSync, constants as fsConstants, readlinkSync } from 'fs';
import { homedir } from 'os';
import { join, basename } from 'path';
import { log } from './logging.mjs';

const isWindows = process.platform === 'win32';
// Windows: every CLI we care about installs a real .exe — never a .cmd / .ps1
// shim or MSYS bash wrapper. accessSync(X_OK) accepts any readable file on
// Windows, so we have to reject non-.exe candidates explicitly.
const WIN_EXEC_EXT = /\.exe$/i;

function canExec(p) {
  if (!p) return false;
  if (isWindows && !WIN_EXEC_EXT.test(p)) return false;
  try { accessSync(p, fsConstants.X_OK); return true; } catch { return false; }
}

/**
 * Per-CLI candidate-path generator. Each entry returns absolute paths to try
 * (in order) when shell lookup fails. New CLIs only need to add a section
 * here — the rest of the resolver is CLI-agnostic.
 */
const CANDIDATE_PROVIDERS = {
  claude: { unix: claudeUnixCandidates, windows: claudeWindowsCandidates },
  gemini: { unix: geminiUnixCandidates, windows: geminiWindowsCandidates },
};

/**
 * Detect a parent-process exe whose basename matches the CLI. Only meaningful
 * for `claude` because proxy.mjs is launched as a child of `claude` itself —
 * other adapters can pass `lookParent: false`. Returns null on macOS/Windows
 * (no /proc) or when the symlink doesn't resolve to a CLI binary.
 *
 * VS Code extension bundle paths are intentionally rejected — the plugin must
 * not silently depend on the IDE extension being installed.
 */
function parentExeMatching(nameRegex) {
  try {
    const ppid = process.ppid;
    if (!ppid) return null;
    const exe = readlinkSync(`/proc/${ppid}/exe`);
    if (!exe || !canExec(exe)) return null;
    if (/\.vscode\/extensions\//.test(exe)) return null;
    if (!nameRegex.test(basename(exe))) return null;
    return exe;
  } catch {
    return null;
  }
}

const cache = new Map();   // cliType → resolved absolute path

/**
 * Resolve `<cliType>` to an absolute executable path. Cached per (cliType)
 * for the lifetime of the process. Pass `configured` to force a path.
 *
 * @param {string} cliType  'claude' | 'gemini' | …
 * @param {string|null|undefined} configured caller override; honored even
 *   when the path doesn't currently exist (covers installer races)
 * @returns {string} absolute path or fallback CLI name
 */
export function resolveCliBin(cliType, configured) {
  const ct = String(cliType || 'claude').toLowerCase();
  if (cache.has(ct)) return cache.get(ct);

  // 1. Explicit override — any non-default value is treated as user intent.
  // Default value per CLI is just the CLI name itself; anything else means
  // the caller wrote a path into config and we trust it.
  if (configured && configured !== ct) {
    cache.set(ct, configured);
    log(`[cli-resolver:${ct}] using configured path: ${configured}`);
    return configured;
  }

  // 2. Parent process exe — only useful for the claude path (proxy.mjs is
  // a child of claude). Other adapters skip this entirely.
  if (ct === 'claude') {
    const viaParent = parentExeMatching(/claude/i);
    if (viaParent) {
      cache.set(ct, viaParent);
      log(`[cli-resolver:claude] resolved via parent /proc/${process.ppid}/exe: ${viaParent}`);
      return viaParent;
    }
  }

  // 3. Shell lookup. command -v / where return every match for a name on
  // Windows; scan all and only accept canExec-passing entries.
  try {
    const cmd = isWindows
      ? `where ${ct}`
      : `command -v ${ct} 2>/dev/null || which ${ct} 2>/dev/null`;
    const out = execSync(cmd, {
      encoding: 'utf8',
      timeout: 2000,
      shell: isWindows ? undefined : '/bin/sh',
    }).trim();
    const lines = out.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
    for (const cand of lines) {
      if (canExec(cand)) {
        cache.set(ct, cand);
        log(`[cli-resolver:${ct}] resolved via shell: ${cand}`);
        return cand;
      }
    }
  } catch { /* shell or spawn failed — keep trying */ }

  // 4. Well-known install paths.
  const provider = CANDIDATE_PROVIDERS[ct];
  if (provider) {
    const home = homedir();
    const candidates = isWindows ? provider.windows(home) : provider.unix(home);
    for (const p of candidates) {
      if (canExec(p)) {
        cache.set(ct, p);
        log(`[cli-resolver:${ct}] resolved via candidate: ${p}`);
        return p;
      }
    }
  }

  // 5. Give up — return literal. Spawn error listener absorbs ENOENT.
  cache.set(ct, ct);
  log(`[cli-resolver:${ct}] resolution failed; falling back to literal "${ct}" (expect ENOENT unless PATH is set)`);
  return ct;
}

/** Test-only: clear the resolver cache. */
export function _resetResolverCache() {
  cache.clear();
}

// ─── Per-CLI candidate generators ─────────────────────────

function claudeUnixCandidates(home) {
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

function claudeWindowsCandidates(home) {
  // Windows = claude.exe only. @anthropic-ai/claude-code installs a real
  // claude.exe both at the npm bin root and inside the package bin dir.
  const appdata = process.env.APPDATA || join(home, 'AppData', 'Roaming');
  const localAppData = process.env.LOCALAPPDATA || join(home, 'AppData', 'Local');
  const pkgBin = join(appdata, 'npm', 'node_modules', '@anthropic-ai', 'claude-code', 'bin');
  return [
    join(pkgBin, 'claude.exe'),
    join(appdata, 'npm', 'claude.exe'),
    join(localAppData, 'Programs', 'anthropic', 'claude-code', 'claude.exe'),
  ];
}

function geminiUnixCandidates(home) {
  // Gemini CLI is distributed via npm (`@google/gemini-cli`). Common install
  // locations mirror the claude list since both ship as Node-based npm bins.
  return [
    join(home, '.npm-global/bin/gemini'),
    join(home, '.bun/bin/gemini'),
    join(home, '.local/bin/gemini'),
    join(home, '.volta/bin/gemini'),
    join(home, '.npm-packages/bin/gemini'),
    join(home, 'node_modules/.bin/gemini'),
    '/usr/local/bin/gemini',
    '/opt/homebrew/bin/gemini',
    '/usr/bin/gemini',
  ];
}

function geminiWindowsCandidates(home) {
  // gemini-cli ships a gemini.exe in the npm-global bin dir. Same .exe-only
  // gate as claude — never accept .cmd / .ps1 shims here.
  const appdata = process.env.APPDATA || join(home, 'AppData', 'Roaming');
  const localAppData = process.env.LOCALAPPDATA || join(home, 'AppData', 'Local');
  const pkgBin = join(appdata, 'npm', 'node_modules', '@google', 'gemini-cli', 'bin');
  return [
    join(pkgBin, 'gemini.exe'),
    join(appdata, 'npm', 'gemini.exe'),
    join(localAppData, 'Programs', 'google', 'gemini-cli', 'gemini.exe'),
  ];
}
