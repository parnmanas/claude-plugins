#!/usr/bin/env bash
#
# fake-claude.sh — Claude CLI stand-in for SubagentManager / ChatSessionManager tests
#
# Modes:
#   Trigger/legacy (argv-driven):       one positional taskText → emit one result → exit.
#   Persistent chat (stream-json in):   loop on stdin, emit {"type":"result",...} per line,
#                                       exit when stdin closes.
#
# Env vars:
#   AWB_FAKE_SLEEP       Seconds to sleep before exiting the one-shot path (default 0).
#                        Per-turn sleep in stream-json mode (default 0).
#   AWB_FAKE_EXIT_CODE   Exit code (default: 0).
#   AWB_FAKE_LOG         If "1", echo parsed argv to stderr for debug.

set -e

MCP_CONFIG=""
ALLOWED_TOOLS=""
SYSTEM_PROMPT=""
OUTPUT_FORMAT=""
INPUT_FORMAT=""
STRICT_MCP=0
PRINT=0
VERBOSE=0
SKIP_PERMS=0
TASK_TEXT=""

while [ $# -gt 0 ]; do
  case "$1" in
    --print)
      PRINT=1
      shift
      ;;
    --verbose)
      VERBOSE=1
      shift
      ;;
    --input-format)
      INPUT_FORMAT="$2"
      shift 2
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
      TASK_TEXT="$1"
      shift
      ;;
  esac
done

if [ "${AWB_FAKE_LOG:-0}" = "1" ]; then
  echo "fake-claude: print=$PRINT input=$INPUT_FORMAT output=$OUTPUT_FORMAT mcp_config=$MCP_CONFIG strict=$STRICT_MCP allowed=$ALLOWED_TOOLS skip_perms=$SKIP_PERMS" >&2
  echo "fake-claude: system_prompt_len=${#SYSTEM_PROMPT}" >&2
  echo "fake-claude: task_text_len=${#TASK_TEXT}" >&2
fi

EXIT_CODE="${AWB_FAKE_EXIT_CODE:-0}"
if [ "$EXIT_CODE" = "0" ]; then
  IS_ERROR="false"
else
  IS_ERROR="true"
fi

# ── Stream-json mode: loop on stdin, emit one result per line ──
if [ "$INPUT_FORMAT" = "stream-json" ]; then
  TURN=0
  while IFS= read -r LINE; do
    TURN=$((TURN + 1))
    SLEEP_SEC="${AWB_FAKE_SLEEP:-0}"
    if [ "$SLEEP_SEC" -gt 0 ] 2>/dev/null; then
      sleep "$SLEEP_SEC"
    fi
    printf '{"type":"result","subtype":"success","is_error":%s,"turn":%d,"duration_ms":0,"result":"fake-claude turn ok","session_id":"fake-%d"}\n' \
      "$IS_ERROR" "$TURN" "$$"
  done
  exit "$EXIT_CODE"
fi

# ── Legacy one-shot mode (trigger/chat subagent tests) ──
SLEEP_SEC="${AWB_FAKE_SLEEP:-0}"
if [ "$SLEEP_SEC" -gt 0 ] 2>/dev/null; then
  sleep "$SLEEP_SEC"
fi

cat <<EOF
{"type":"result","subtype":"success","is_error":${IS_ERROR},"duration_ms":${SLEEP_SEC}000,"result":"fake-claude ok","session_id":"fake-$$","total_cost_usd":0.0}
EOF

exit "$EXIT_CODE"
