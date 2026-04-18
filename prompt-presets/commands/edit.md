---
allowed-tools: Bash(test:*), Read, Edit, Write
description: Edit a saved prompt preset
argument-hint: <name>
---

저장된 preset 을 수정한다.

## 경로

```
PRESET_PATH="${CLAUDE_CONFIG_DIR:-$HOME/.claude}/prompt-presets/<name>.md"
```

## 절차

1. `$ARGUMENTS` 에서 preset 이름을 가져온다. 없으면 `/prompt-presets:list` 를 안내하고 중단.
2. `test -f "$PRESET_PATH"` 로 존재 확인. 없으면 중단.
3. Read 로 현재 content 표시.
4. 사용자에게 어떻게 바꿀지 묻고, Edit 또는 Write 로 반영.
