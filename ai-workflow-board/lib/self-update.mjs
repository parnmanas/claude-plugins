// ─── Daemon self-update (Phase 4) ─────────────────────────
// Drives the "git pull on the plugin repo + re-exec daemon" cycle. Triggered
// by SIGUSR1 from daemon.mjs (which the Phase 3 admin endpoint
// `/admin/agent-manager/instances/:id/restart` sends). Kept in its own
// module so the daemon entrypoint stays focused on lifecycle wiring and so
// the actual git operation is unit-testable in isolation.
//
// What this does NOT do:
//   - It does not `process.exit` or re-spawn the daemon. That's the
//     entrypoint's job — runSelfUpdate just ensures the working tree is
//     up to date and reports back. The entrypoint then runs its full async
//     shutdown (drain subagents, release lockfile) before re-execing.
//
// Safety:
//   - Pull is `--ff-only`. If the working tree has local commits / dirty
//     state that can't fast-forward, this throws so the caller can decide
//     (don't re-exec, log, leave the daemon as-is).
//   - We run from the plugin's own directory (resolved via import.meta.url),
//     not from some arbitrary CWD — same plugin tree the daemon is
//     executing.
//   - Two-minute timeout: a network hang must not wedge the daemon.

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';

const GIT_TIMEOUT_MS = 120_000;

function gitCommand(cwd, args) {
  return new Promise((resolveP, rejectP) => {
    const child = spawn('git', args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (b) => { stdout += b.toString('utf8'); });
    child.stderr.on('data', (b) => { stderr += b.toString('utf8'); });
    const timer = setTimeout(() => {
      try { child.kill('SIGTERM'); } catch { /* gone */ }
      rejectP(new Error(`git ${args.join(' ')}: timed out after ${GIT_TIMEOUT_MS / 1000}s`));
    }, GIT_TIMEOUT_MS);
    if (typeof timer.unref === 'function') timer.unref();
    child.on('exit', (code, signal) => {
      clearTimeout(timer);
      if (code === 0) resolveP({ stdout: stdout.trim(), stderr: stderr.trim() });
      else rejectP(new Error(`git ${args.join(' ')} failed (exit=${code} signal=${signal || ''}): ${stderr.trim() || stdout.trim()}`));
    });
    child.on('error', (err) => {
      clearTimeout(timer);
      rejectP(err);
    });
  });
}

/**
 * Locate the git working tree that owns the plugin code. The plugin lives at
 * `<plugin-repo>/ai-workflow-board/` so we walk one level up from the
 * daemon's import URL. If we're not actually inside a git checkout (rare —
 * someone unzipped a release tarball), `git -C` will fail with a clear
 * error and the caller logs it.
 */
function pluginRepoRoot(daemonImportUrl) {
  const here = dirname(fileURLToPath(daemonImportUrl));
  // here = .../ai-workflow-board ; one up = plugin repo root.
  return resolve(here, '..');
}

/**
 * Pull the plugin repo to the latest origin/<current branch>.
 * Returns { previousSha, currentSha, summary } so callers can decide whether
 * a re-exec is worth it (no-op pulls don't need a daemon restart).
 *
 * @param {{ log: (msg: string) => void, importUrl?: string, repoRoot?: string }} opts
 */
export async function runSelfUpdate(opts) {
  const log = opts?.log || (() => {});
  const cwd = opts?.repoRoot || pluginRepoRoot(opts?.importUrl || import.meta.url);

  // Snapshot HEAD so we can report a delta even if the pull is a no-op.
  const before = await gitCommand(cwd, ['rev-parse', 'HEAD']);
  const branch = await gitCommand(cwd, ['rev-parse', '--abbrev-ref', 'HEAD']);
  log(`Self-update: git pull --ff-only on ${cwd} (branch=${branch.stdout})`);

  await gitCommand(cwd, ['fetch', '--prune', 'origin']);
  await gitCommand(cwd, ['pull', '--ff-only', 'origin', branch.stdout]);

  const after = await gitCommand(cwd, ['rev-parse', 'HEAD']);
  const changed = before.stdout !== after.stdout;
  return {
    previousSha: before.stdout,
    currentSha: after.stdout,
    branch: branch.stdout,
    changed,
    summary: changed
      ? `branch=${branch.stdout} ${before.stdout.slice(0, 7)} → ${after.stdout.slice(0, 7)}`
      : `branch=${branch.stdout} already up to date (${after.stdout.slice(0, 7)})`,
  };
}
