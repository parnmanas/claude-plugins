// ─── Claude CLI adapter (Phase 2) ─────────────────────────
//
// Lifts the claude-specific argv/format/parse logic out of SubagentManager
// and BaseSessionManager into one place. Behavior must match v0.36.0 exactly
// — the test suite verifies this (test/subagent-manager.test.mjs etc.).

import { resolveCliBin } from '../cli-resolver.mjs';
import { CliAdapter, ADAPTER_CAPABILITIES, PARSE_STAGE } from './base.mjs';

const { PERSISTENT_SESSION, NATIVE_MCP } = ADAPTER_CAPABILITIES;

export class ClaudeCliAdapter extends CliAdapter {
  static cliType = 'claude';

  constructor() {
    super();
    this.capabilities = new Set([PERSISTENT_SESSION, NATIVE_MCP]);
  }

  resolveBin(configured) {
    return resolveCliBin('claude', configured);
  }

  buildOneshotSpawn({ rolePrompt, taskText, mcpConfigPath }) {
    return {
      args: [
        '--print',
        '--output-format', 'json',
        '--mcp-config', mcpConfigPath,
        '--strict-mcp-config',
        '--allowedTools', 'mcp__awb__*',
        '--append-system-prompt', rolePrompt || '',
        '--dangerously-skip-permissions',
        taskText,
      ],
      stdio: ['ignore', 'pipe', 'pipe'],
      needsMcpConfig: true,
    };
  }

  buildSessionSpawn({ rolePrompt, mcpConfigPath }) {
    return {
      args: [
        '--verbose',
        '--input-format', 'stream-json',
        '--output-format', 'stream-json',
        '--mcp-config', mcpConfigPath,
        '--strict-mcp-config',
        '--allowedTools', 'mcp__awb__*',
        '--append-system-prompt', rolePrompt || '',
        '--dangerously-skip-permissions',
      ],
      stdio: ['pipe', 'pipe', 'pipe'],
      needsMcpConfig: true,
    };
  }

  formatTurn(text) {
    const obj = {
      type: 'user',
      message: { role: 'user', content: [{ type: 'text', text: String(text) }] },
    };
    return JSON.stringify(obj);
  }

  parseStdoutLine(line) {
    let obj = null;
    try { obj = JSON.parse(line); } catch { /* non-JSON; manager treats as null */ }
    if (!obj) {
      return { stage: null, isResult: false, isError: false, raw: null };
    }
    return {
      stage: obj.type === 'assistant' ? PARSE_STAGE.COMPOSING : PARSE_STAGE.THINKING,
      isResult: obj.type === 'result',
      isError: obj.is_error === true,
      raw: obj,
    };
  }

  // Claude one-shots write to AWB themselves via MCP — no aggregate to collect.
  collectOneshotResult(_lines) { return null; }
}
