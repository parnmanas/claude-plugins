// Smoke test — Phase 4 self-update.
//
// Sets up a tmp bare repo + clone, runs runSelfUpdate against the clone, and
// verifies the changed/no-op branch + the SHA delta when an upstream commit
// is added. Actual git CLI required (assumed present in CI / dev hosts).

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

import { runSelfUpdate } from '../lib/self-update.mjs';

function git(cwd, args, env = {}) {
  const r = spawnSync('git', args, {
    cwd,
    encoding: 'utf8',
    env: {
      ...process.env,
      GIT_AUTHOR_NAME: 'test',
      GIT_AUTHOR_EMAIL: 'test@example.com',
      GIT_COMMITTER_NAME: 'test',
      GIT_COMMITTER_EMAIL: 'test@example.com',
      ...env,
    },
  });
  if (r.status !== 0) throw new Error(`git ${args.join(' ')} failed: ${r.stderr || r.stdout}`);
  return r.stdout.trim();
}

function makeFixture() {
  const root = mkdtempSync(join(tmpdir(), 'awb-selfupdate-'));
  const upstream = join(root, 'upstream.git');
  const clone = join(root, 'clone');
  spawnSync('git', ['init', '--bare', '-b', 'main', upstream], { stdio: 'pipe' });
  // Seed upstream with one commit on main.
  const seed = join(root, 'seed');
  spawnSync('git', ['init', '-q', '-b', 'main', seed], { stdio: 'pipe' });
  writeFileSync(join(seed, 'README.md'), 'seed\n');
  git(seed, ['add', 'README.md']);
  git(seed, ['commit', '-q', '-m', 'seed']);
  git(seed, ['remote', 'add', 'origin', upstream]);
  git(seed, ['push', '-q', '-u', 'origin', 'main']);
  // Fresh clone to simulate the daemon's working tree. The bare repo's HEAD
  // is symbolic refs/heads/main now, so the clone gets a working tree.
  const cloneR = spawnSync('git', ['clone', '-q', '-b', 'main', upstream, clone], { stdio: 'pipe' });
  if (cloneR.status !== 0) throw new Error(`clone failed: ${cloneR.stderr}`);
  return { root, upstream, clone, seed };
}

test('runSelfUpdate reports no-op when working tree matches upstream', async () => {
  const f = makeFixture();
  try {
    const result = await runSelfUpdate({ log: () => {}, repoRoot: f.clone });
    assert.equal(result.changed, false);
    assert.equal(result.branch, 'main');
    assert.equal(result.previousSha, result.currentSha);
    assert.match(result.summary, /already up to date/);
  } finally {
    rmSync(f.root, { recursive: true, force: true });
  }
});

test('runSelfUpdate fast-forwards and reports SHA delta', async () => {
  const f = makeFixture();
  try {
    // Add a new commit to upstream by pushing from the seed clone.
    writeFileSync(join(f.seed, 'README.md'), 'seed\nupdate\n');
    git(f.seed, ['add', 'README.md']);
    git(f.seed, ['commit', '-q', '-m', 'update']);
    git(f.seed, ['push', '-q', 'origin', 'main']);
    const result = await runSelfUpdate({ log: () => {}, repoRoot: f.clone });
    assert.equal(result.changed, true);
    assert.notEqual(result.previousSha, result.currentSha);
    assert.match(result.summary, /→/);
  } finally {
    rmSync(f.root, { recursive: true, force: true });
  }
});

test('runSelfUpdate throws when fast-forward is impossible', async () => {
  const f = makeFixture();
  try {
    // Diverge: commit locally, then rewrite the upstream to a different branch
    // tip via force-push from seed.
    writeFileSync(join(f.clone, 'LOCAL'), 'local divergent\n');
    git(f.clone, ['add', 'LOCAL']);
    git(f.clone, ['commit', '-q', '-m', 'local divergent']);
    writeFileSync(join(f.seed, 'README.md'), 'seed\nremote-different\n');
    git(f.seed, ['add', 'README.md']);
    git(f.seed, ['commit', '-q', '-m', 'remote different']);
    git(f.seed, ['push', '-q', 'origin', 'main']);

    await assert.rejects(
      runSelfUpdate({ log: () => {}, repoRoot: f.clone }),
      /failed/,
    );
  } finally {
    rmSync(f.root, { recursive: true, force: true });
  }
});
