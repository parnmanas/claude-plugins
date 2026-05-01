// ─── CliAdapter base interface (Phase 2) ──────────────────
//
// One adapter per CLI flavor. Managers (SubagentManager / BaseSessionManager
// subclasses) hold a single adapter instance and consult it for everything
// that varies across CLIs:
//
//   - bin resolution
//   - argv construction (one-shot vs persistent session)
//   - stdin turn formatting (persistent only)
//   - stdout line parsing (turn-progress + completion signals)
//   - one-shot result aggregation (so non-MCP CLIs can post their answer
//     back to AWB through the daemon's REST connection)
//
// Capability flags expose what the adapter can/cannot do, so managers can
// short-circuit (e.g. BaseSessionManager refuses to spawn a session against
// an adapter without PERSISTENT_SESSION).

/** Capability flags an adapter may declare. */
export const ADAPTER_CAPABILITIES = Object.freeze({
  /** Bidirectional stream-json over stdin/stdout, multi-turn over one process. */
  PERSISTENT_SESSION: 'persistent_session',
  /** The spawned CLI itself can call AWB MCP tools (claude). When false, the
   *  daemon collects the CLI's stdout via collectOneshotResult() and posts the
   *  answer to AWB on the adapter's behalf. */
  NATIVE_MCP: 'native_mcp',
});

/**
 * Spawn descriptor returned by the adapter's build*Spawn methods. The caller
 * (manager) feeds this into Node's child_process.spawn() and any post-spawn
 * stdin handling. Fields:
 *
 *   args        argv passed to spawn() (excluding the binary itself)
 *   stdio       ['ignore'|'pipe', 'pipe', 'pipe'] tuple
 *   shell       true → cmd.exe wrap (only for .cmd/.bat/.ps1 wrappers)
 *   writePrompt optional (child) => void; fired AFTER spawn returns to push
 *               a single prompt over stdin (then stdin.end()). Used by stdin-
 *               prompted CLIs (gemini one-shot). When unset, the manager
 *               doesn't write anything to stdin for one-shots (claude reads
 *               its prompt from argv).
 *   needsMcpConfig
 *               true → the manager must write a per-spawn mcp-config tempfile
 *               and pass its path back to the adapter via spec.mcpConfigPath.
 *               false (gemini) → no tempfile needed; manager skips mcp-config
 *               creation/cleanup entirely.
 */

/** Result returned by parseStdoutLine. */
export const PARSE_STAGE = Object.freeze({
  THINKING: 'thinking',
  COMPOSING: 'composing',
});

export class CliAdapter {
  /** @type {string} unique tag — subclasses override. */
  static cliType = 'base';

  /** @type {Set<string>} declared capability flags. */
  capabilities = new Set();

  /** Capability check helper. */
  has(cap) { return this.capabilities.has(cap); }

  /** Convenient alias for the static cliType so call sites can read `adapter.cliType`. */
  get cliType() { return this.constructor.cliType; }

  /**
   * Resolve the absolute binary path. Returns the literal CLI name on
   * resolution failure (caller's spawn error listener absorbs ENOENT).
   * @param {string|null} _configured caller override (config.delegation.<bin>)
   * @returns {string}
   */
  resolveBin(_configured) { throw new Error(`${this.cliType}: resolveBin not implemented`); }

  /**
   * Build the spawn descriptor for a one-shot subagent (single trigger →
   * single child process → exit). spec fields the adapter may consult:
   *   - rolePrompt    (string)
   *   - taskText      (string)
   *   - mcpConfigPath (string, present iff this.needsMcpConfig === true)
   *
   * @returns {{ args: string[], stdio: any[], shell?: boolean,
   *            writePrompt?: (child: import('child_process').ChildProcess) => void,
   *            needsMcpConfig?: boolean }}
   */
  buildOneshotSpawn(_spec) { throw new Error(`${this.cliType}: buildOneshotSpawn not implemented`); }

  /**
   * Build the spawn descriptor for a persistent multi-turn session (chat or
   * ticket). Throws if !this.has(PERSISTENT_SESSION).
   */
  buildSessionSpawn(_spec) { throw new Error(`${this.cliType}: buildSessionSpawn not implemented`); }

  /**
   * Format an outgoing turn into the bytes to write to a persistent session's
   * stdin. Returns the raw string (no trailing newline; manager appends it).
   * Throws if !this.has(PERSISTENT_SESSION).
   */
  formatTurn(_text) { throw new Error(`${this.cliType}: formatTurn not implemented`); }

  /**
   * Parse a single stdout line and return what the manager should infer:
   *   stage   one of PARSE_STAGE.* (or null when the line is uninteresting)
   *   isResult true → the turn is complete (manager clears health watchdog,
   *           fires onResult hooks, ends turn-progress timers)
   *   isError true → the result was an error (informational, manager logs)
   *   raw     parsed payload (any) for callers that need the structured form
   */
  parseStdoutLine(_line) { throw new Error(`${this.cliType}: parseStdoutLine not implemented`); }

  /**
   * Collect a final response from the captured stdout lines of a one-shot
   * spawn. claude returns null (the subagent posted to AWB itself via MCP);
   * gemini returns the assembled answer text. Manager invokes this on
   * subagent exit ONLY when !adapter.has(NATIVE_MCP) — claude-style adapters
   * never need it.
   */
  collectOneshotResult(_lines) { return null; }
}
