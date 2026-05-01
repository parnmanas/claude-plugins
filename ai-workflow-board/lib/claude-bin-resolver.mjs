// ─── Claude bin resolver (back-compat shim) ────────────────
// The CLI-agnostic resolver lives in ./cli-resolver.mjs since Phase 2.
// This file is preserved as a thin shim so any external caller importing
// `resolveClaudeBin` keeps working without code changes.

import { resolveCliBin } from './cli-resolver.mjs';

/**
 * Resolve the `claude` CLI to an absolute path. Identical contract to the
 * original v0.36.0 implementation; delegates to the generalized resolver.
 *
 * @param {string} [configured='claude']
 * @returns {string}
 */
export function resolveClaudeBin(configured = 'claude') {
  return resolveCliBin('claude', configured);
}
