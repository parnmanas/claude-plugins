// ─── Gemini CLI adapter (Phase 2) ─────────────────────────
//
// Stateless single-shot adapter for Google's gemini CLI (`@google/gemini-cli`).
// Capabilities:
//   - PERSISTENT_SESSION: NO. Each trigger spawns a fresh `gemini` process.
//     Conversation history (if any) must be embedded in the prompt prefix.
//   - NATIVE_MCP: NO. Gemini doesn't speak AWB's MCP tools. The daemon collects
//     gemini's stdout via collectOneshotResult() and posts the answer back to
//     AWB through its own REST connection.
//
// The "stateless" hypothesis is what the Phase 2 ticket explicitly locks in
// (see ticket 573157e1 description, "비-claude session 모델"); persistent
// gemini sessions can be added in a later phase if a real use case appears.
//
// Spawn shape: `gemini --prompt-interactive` reads the prompt from stdin and
// emits the answer on stdout, then exits. We collect everything between
// spawn and exit.

import { resolveCliBin } from '../cli-resolver.mjs';
import { CliAdapter, PARSE_STAGE } from './base.mjs';

export class GeminiCliAdapter extends CliAdapter {
  static cliType = 'gemini';

  constructor() {
    super();
    this.capabilities = new Set();   // no PERSISTENT_SESSION, no NATIVE_MCP
  }

  resolveBin(configured) {
    return resolveCliBin('gemini', configured);
  }

  buildOneshotSpawn({ rolePrompt, taskText }) {
    // Compose the full prompt as one string. gemini-cli supports the
    // `-p "<prompt>"` short-form on the command line, but inline argv tends
    // to break on long prompts (Windows argv limits, shell quoting). Stdin
    // is the reliable path: pass `--prompt-interactive` and write the prompt
    // followed by stdin.end().
    //
    // We accept that some gemini-cli versions don't expose
    // `--prompt-interactive`. As a fallback the adapter just spawns with no
    // flags and writes the prompt — modern gemini-cli reads stdin when no
    // -p value is given. This default works for both shapes.
    const fullPrompt = rolePrompt
      ? `${rolePrompt}\n\n${taskText}`
      : (taskText || '');
    return {
      args: [],
      stdio: ['pipe', 'pipe', 'pipe'],
      needsMcpConfig: false,
      writePrompt: (child) => {
        try {
          child.stdin.write(fullPrompt);
          child.stdin.end();
        } catch { /* spawn already failed; manager's error handler logs it */ }
      },
    };
  }

  buildSessionSpawn() {
    throw new Error('gemini adapter does not support persistent sessions in Phase 2');
  }

  formatTurn() {
    throw new Error('gemini adapter does not support persistent sessions in Phase 2');
  }

  parseStdoutLine(line) {
    // No structured protocol — every non-empty line is "the model is writing"
    // signal for client-side typing indicators. We never see a result marker
    // so isResult stays false; one-shot completion is signaled by exit().
    const trimmed = String(line || '').trim();
    return {
      stage: trimmed ? PARSE_STAGE.COMPOSING : null,
      isResult: false,
      isError: false,
      raw: line,
    };
  }

  collectOneshotResult(lines) {
    // Trim trailing whitespace + drop empty leading/trailing lines so the
    // posted comment isn't padded with newlines. Keep interior blank lines
    // so paragraph breaks survive.
    const text = (Array.isArray(lines) ? lines : []).join('\n');
    return text.replace(/^\s+|\s+$/g, '');
  }
}
