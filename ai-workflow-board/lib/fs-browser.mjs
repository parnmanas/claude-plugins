// ─── FS Browser handler ──────────────────────────────────────────
// Reverse-RPC target: server emits fs_request over SSE, we perform the op
// against the local filesystem, and POST the result back to
// /api/fs/responses/:request_id.
//
// Scope enforcement lives here (server is a pure forwarder). Every path is
// resolved to an absolute realpath, then matched against the realpath of each
// configured root — that blocks both `..` tricks and symlinks pointing outside.
// An empty roots list means fs browsing is off even if `enabled` is true.

import { promises as fsp, realpathSync } from 'fs';
import { resolve as pathResolve, sep as PATH_SEP } from 'path';
import { log } from './logging.mjs';

const DEFAULT_LIST_LIMIT = 5000;
const DEFAULT_READ_CAP = 5 * 1024 * 1024; // 5 MB — server cap mirrors this
// Heuristic utf-8 sniff: a NUL byte in the first sampleSize bytes is a
// reliable "this is binary" signal for anything a human would open.
const BINARY_SNIFF_BYTES = 512;

export class FsBrowser {
  /**
   * @param {object} config loaded channel config
   * @param {object} fsSection config.fs_browser — { enabled, roots }
   */
  constructor(config, fsSection) {
    this.config = config;
    this.enabled = !!fsSection?.enabled;
    this.rawRoots = Array.isArray(fsSection?.roots) ? fsSection.roots.filter((r) => typeof r === 'string' && r) : [];
    // Realpath the roots once at construction. Broken/missing roots are
    // dropped with a warning — we don't want a typo to silently grant
    // "outside scope" because realpath resolves to undefined.
    this.roots = [];
    this._resolveRootsSync();
  }

  _resolveRootsSync() {
    for (const root of this.rawRoots) {
      try {
        const abs = pathResolve(root);
        const real = realpathSync(abs);
        this.roots.push(real);
      } catch (err) {
        log(`[fs-browser] scope root unreachable, dropped: ${root} (${err.code || err.message})`);
      }
    }
    if (this.enabled && this.roots.length === 0) {
      log('[fs-browser] fs_browser.enabled=true but no valid roots — requests will be denied');
    } else if (this.enabled) {
      log(`[fs-browser] enabled with ${this.roots.length} root(s): ${this.roots.join(', ')}`);
    }
  }

  /**
   * Run a single fs request and return { ok, data } or { ok:false, error, code }.
   * Never throws — errors are mapped to structured codes.
   */
  async handle(req) {
    if (!this.enabled || this.roots.length === 0) {
      return { ok: false, error: 'File browsing is disabled on this agent', code: 'FS_BROWSER_DISABLED' };
    }
    if (!req || typeof req !== 'object') {
      return { ok: false, error: 'Malformed request', code: 'PATH_INVALID' };
    }
    const op = req.op;
    const rawPath = req.path;
    if (typeof rawPath !== 'string' || !rawPath) {
      return { ok: false, error: 'path is required', code: 'PATH_INVALID' };
    }
    // Absolute-only. A relative path would be ambiguous (cwd on the agent
    // side is not meaningful to the UI user picking files) and makes scope
    // enforcement fiddly — reject early.
    if (!rawPath.startsWith(PATH_SEP) && !/^[A-Za-z]:[\\/]/.test(rawPath)) {
      return { ok: false, error: 'path must be absolute', code: 'PATH_INVALID' };
    }

    let realPath;
    try {
      realPath = await fsp.realpath(pathResolve(rawPath));
    } catch (err) {
      return { ok: false, error: err.message, code: err.code || 'ENOENT' };
    }

    if (!this._inScope(realPath)) {
      return { ok: false, error: `Path outside configured roots: ${rawPath}`, code: 'SCOPE_DENIED' };
    }

    try {
      switch (op) {
        case 'list': return { ok: true, data: await this._list(realPath) };
        case 'stat': return { ok: true, data: await this._stat(realPath) };
        case 'read': return { ok: true, data: await this._read(realPath, req.offset ?? 0, req.limit ?? DEFAULT_READ_CAP) };
        default:     return { ok: false, error: `Unknown op: ${op}`, code: 'PATH_INVALID' };
      }
    } catch (err) {
      return { ok: false, error: err.message, code: err.code || 'FS_ERROR' };
    }
  }

  _inScope(realPath) {
    for (const root of this.roots) {
      if (realPath === root) return true;
      if (realPath.startsWith(root + PATH_SEP)) return true;
    }
    return false;
  }

  async _list(realPath) {
    const stat = await fsp.stat(realPath);
    if (!stat.isDirectory()) {
      const err = new Error(`Not a directory: ${realPath}`);
      err.code = 'ENOTDIR';
      throw err;
    }
    const entries = await fsp.readdir(realPath, { withFileTypes: true });
    const limit = DEFAULT_LIST_LIMIT;
    const truncated = entries.length > limit;
    const slice = truncated ? entries.slice(0, limit) : entries;

    const out = await Promise.all(slice.map(async (dirent) => {
      const full = `${realPath}${PATH_SEP}${dirent.name}`;
      let type = 'other';
      let size = 0;
      let mtime = '';
      let mode = 0;
      try {
        // lstat here so symlinks identify as 'symlink' and don't accidentally
        // report the target's size (which could be misleading if the target
        // lives outside scope).
        const st = await fsp.lstat(full);
        mode = st.mode;
        size = Number(st.size) || 0;
        mtime = st.mtime.toISOString();
        if (st.isDirectory()) type = 'directory';
        else if (st.isSymbolicLink()) type = 'symlink';
        else if (st.isFile()) type = 'file';
      } catch {
        // Unreadable entry (permission etc.) — keep the name, leave type='other'.
      }
      return { name: dirent.name, type, size, mtime, mode };
    }));

    return { path: realPath, entries: out, truncated };
  }

  async _stat(realPath) {
    const st = await fsp.lstat(realPath);
    let type = 'other';
    let realTarget;
    if (st.isSymbolicLink()) {
      type = 'symlink';
      try { realTarget = await fsp.realpath(realPath); } catch { /* broken link — ignore */ }
    } else if (st.isDirectory()) type = 'directory';
    else if (st.isFile()) type = 'file';
    return {
      path: realPath,
      real_path: realTarget,
      type,
      size: Number(st.size) || 0,
      mtime: st.mtime.toISOString(),
      mode: st.mode,
    };
  }

  async _read(realPath, offset, limit) {
    const st = await fsp.stat(realPath);
    if (!st.isFile()) {
      const err = new Error(`Not a file: ${realPath}`);
      err.code = 'EISDIR';
      throw err;
    }
    const off = Math.max(0, Math.floor(Number(offset) || 0));
    const cap = Math.min(Math.max(1, Math.floor(Number(limit) || DEFAULT_READ_CAP)), DEFAULT_READ_CAP);
    const remaining = Math.max(0, Number(st.size) - off);
    const readBytes = Math.min(remaining, cap);
    const truncated = readBytes < remaining;

    const fh = await fsp.open(realPath, 'r');
    try {
      const buf = Buffer.alloc(readBytes);
      if (readBytes > 0) {
        await fh.read(buf, 0, readBytes, off);
      }
      const binary = this._looksBinary(buf);
      return {
        path: realPath,
        content: binary ? buf.toString('base64') : buf.toString('utf8'),
        encoding: binary ? 'base64' : 'utf8',
        size: Number(st.size) || 0,
        read_bytes: readBytes,
        offset: off,
        truncated,
        mtime: st.mtime.toISOString(),
      };
    } finally {
      await fh.close();
    }
  }

  _looksBinary(buf) {
    const scan = Math.min(buf.length, BINARY_SNIFF_BYTES);
    for (let i = 0; i < scan; i++) {
      if (buf[i] === 0) return true;
    }
    return false;
  }
}
