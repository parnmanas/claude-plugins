---
allowed-tools: Bash(ls:*), Bash(test:*), Bash(mkdir:*), Bash(head:*), Read
description: List saved prompt presets
---

사용자가 저장한 모든 prompt preset 을 나열한다.

## 경로

```
PRESET_DIR="${CLAUDE_CONFIG_DIR:-$HOME/.claude}/prompt-presets"
```

## 출력

1. 디렉토리 없거나 비어있으면 "저장된 preset 이 없다. `/prompt-presets:save <name>` 으로 추가" 만 알린다.
2. 있으면 `ls "$PRESET_DIR"` 로 `.md` 목록을 얻고, 각 파일의 frontmatter 에서 `description:` 을 뽑아 다음 포맷으로 출력:

```
- <name> — <description or "(no description)">
```

이름 뒤 확장자 `.md` 는 제거해서 보여준다.
