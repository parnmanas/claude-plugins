#!/usr/bin/env bash
#
# fake-claude.sh — Claude CLI stand-in for SubagentManager tests
#
# Usage: point SubagentManager config.delegation.claudeBin at this script's absolute path.
# The stub parses the same argv shape as the real CLI, honors env var controls for
# sleep and exit code, and emits a parseable JSON result on stdout so downstream
# capture code (Plan 04-03) can exercise its parsing path.
#
# Env vars:
#   AWB_FAKE_SLEEP       Seconds to sleep before exiting (default: 0). Set to 999 to
#                        simulate a hung subagent for TTL sweep testing.
#   AWB_FAKE_EXIT_CODE   Exit code (default: 0). Set non-zero for failure path.
#   AWB_FAKE_LOG         If set to "1", echo the parsed argv to stderr for debug.
#
# The stub does NOT actually talk to any MCP server — tests that care about MCP
# round-trips should stub that separately.

set -e

# Parse argv — we only care about recording we saw the expected flags
MCP_CONFIG=""
ALLOWED_TOOLS=""
SYSTEM_PROMPT=""
OUTPUT_FORMAT=""
STRICT_MCP=0
PRINT=0
SKIP_PERMS=0
TASK_TEXT=""

while [ $# -gt 0 ]; do
  case "$1" in
    --print)
      PRINT=1
      shift
      ;;
    --output-format)
      OUTPUT_FORMAT="$2"
      shift 2
      ;;
    --mcp-config)
      MCP_CONFIG="$2"
      shift 2
      ;;
    --strict-mcp-config)
      STRICT_MCP=1
      shift
      ;;
    --allowedTools)
      ALLOWED_TOOLS="$2"
      shift 2
      ;;
    --append-system-prompt)
      SYSTEM_PROMPT="$2"
      shift 2
      ;;
    --dangerously-skip-permissions)
      SKIP_PERMS=1
      shift
      ;;
    *)
      # Positional: the task text
      TASK_TEXT="$1"
      shift
      ;;
  esac
done

if [ "${AWB_FAKE_LOG:-0}" = "1" ]; then
  echo "fake-claude: print=$PRINT output=$OUTPUT_FORMAT mcp_config=$MCP_CONFIG strict=$STRICT_MCP allowed=$ALLOWED_TOOLS skip_perms=$SKIP_PERMS" >&2
  echo "fake-claude: system_prompt_len=${#SYSTEM_PROMPT}" >&2
  echo "fake-claude: task_text_len=${#TASK_TEXT}" >&2
  echo "fake-claude: AWB_API_KEY_set=$([ -n "${AWB_API_KEY:-}" ] && echo yes || echo no)" >&2
fi

# Honor sleep — tests use this to exercise TTL kill paths
SLEEP_SEC="${AWB_FAKE_SLEEP:-0}"
if [ "$SLEEP_SEC" -gt 0 ] 2>/dev/null; then
  sleep "$SLEEP_SEC"
fi

# Emit JSON result line matching real --output-format json shape
EXIT_CODE="${AWB_FAKE_EXIT_CODE:-0}"
if [ "$EXIT_CODE" = "0" ]; then
  IS_ERROR="false"
else
  IS_ERROR="true"
fi
cat <<EOF
{"type":"result","subtype":"success","is_error":${IS_ERROR},"duration_ms":${SLEEP_SEC}000,"result":"fake-claude ok","session_id":"fake-$$","total_cost_usd":0.0}
EOF

exit "$EXIT_CODE"
