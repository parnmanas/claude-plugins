---
allowed-tools: Bash(test:*), Read
description: Execute a saved prompt preset
argument-hint: <name>
---

사용자가 저장해둔 prompt preset 을 불러와 그대로 실행한다.

## 인자

`$ARGUMENTS` = preset 이름. 없으면 `/prompt-presets:list` 를 안내한다.

## 경로

```
PRESET_PATH="${CLAUDE_CONFIG_DIR:-$HOME/.claude}/prompt-presets/<name>.md"
```

## 실행

1. `test -f "$PRESET_PATH"` 로 존재 확인. 없으면 어떤 preset 이 있는지 `ls` 로 보여주고 중단.
2. Read 도구로 파일 전체를 읽는다.
3. 읽은 content 를 **사용자가 방금 보낸 요청인 것처럼** 그대로 따른다. frontmatter (`---` 블록) 는 무시, 본문만 수행한다.
