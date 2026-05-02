## Project

**Claude Plugins** — collection of Claude Code plugins published via the parnmanas marketplace.

Plugins in this repo:
- `ai-workflow-board/` — stdio MCP forwarder for AWB. Pure forwarder since v0.40.0 (SSE / subagent / channel logic moved to AWB's `agent-manager`).
- `discord/` — Discord MCP plugin.
- `prompt-presets/` — slash-command prompt template plugin.
- `claude-config/`, `claude-self/` — meta/runtime artifacts (not plugins themselves).

### Constraints

- **Plugin version sync**: marketplace 캐시에 변경이 반영되려면 반드시 plugin 의 `.claude-plugin/plugin.json` `version` 을 범프해야 함. 절차 → (1) plugin 코드 수정, (2) 해당 plugin 의 `.claude-plugin/plugin.json` version 범프, (3) commit + push to `main`. 버전을 안 올리면 사용자 쪽 캐시가 갱신되지 않는다.
- **AWB plugin scope (v0.40.0+)**: `ai-workflow-board/proxy.mjs` 는 순수 stdio↔HTTP MCP forwarder. SSE 이벤트 / subagent 위임 / 채널 처리는 plugin 책임이 **아님** — AWB 의 `apps/agent-manager/` (별도 standalone clone: `/mnt/data/repositories/ai-workflow-board/`) 가 담당.
- **Branches**: `main` 단일 브랜치 운영 (이전의 `dev` 는 v0.40.0 시점에 main 으로 merge 되어 삭제됨).
