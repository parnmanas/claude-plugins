// ─── CLI adapter factory (Phase 2) ────────────────────────
// Single entry point: `createAdapter(cliType)`. daemon.mjs / proxy.mjs call
// this once at startup with config.cli (default 'claude').

import { ClaudeCliAdapter } from './claude.mjs';
import { GeminiCliAdapter } from './gemini.mjs';
import { CliAdapter, ADAPTER_CAPABILITIES, PARSE_STAGE } from './base.mjs';

/**
 * Build an adapter instance for the requested CLI type. Falls back to the
 * claude adapter on unknown types (logged by the caller via the returned
 * `adapter.cliType` — the manager just sees a working adapter).
 *
 * @param {string} cliType  config.cli value
 * @returns {CliAdapter}
 */
export function createAdapter(cliType) {
  const t = String(cliType || 'claude').toLowerCase();
  switch (t) {
    case 'claude': return new ClaudeCliAdapter();
    case 'gemini': return new GeminiCliAdapter();
    default: {
      // Unknown CLI — fall back to claude so the proxy/daemon still boots and
      // the user sees a sensible default. The /awb:setup skill should have
      // gated invalid values; this is just a safety net.
      return new ClaudeCliAdapter();
    }
  }
}

/** Tag set used by tests + adapters. */
export const KNOWN_CLI_TYPES = Object.freeze(['claude', 'gemini']);

export { CliAdapter, ADAPTER_CAPABILITIES, PARSE_STAGE };
export { ClaudeCliAdapter } from './claude.mjs';
export { GeminiCliAdapter } from './gemini.mjs';
