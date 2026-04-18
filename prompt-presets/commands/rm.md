---
allowed-tools: Bash(test:*), Bash(rm:*), Read
description: Remove a saved prompt preset
argument-hint: <name>
---

저장된 preset 파일을 삭제한다.

## 경로

```
PRESET_PATH="${CLAUDE_CONFIG_DIR:-$HOME/.claude}/prompt-presets/<name>.md"
```

## 절차

1. `$ARGUMENTS` 에서 preset 이름을 가져온다.
2. `test -f "$PRESET_PATH"` 로 존재 확인. 없으면 중단.
3. Read 로 지울 파일 content 를 먼저 보여주고 사용자 확인을 받는다.
4. 확인되면 `rm -- "$PRESET_PATH"` 로 삭제하고 완료 메시지만 한 줄로 남긴다.
