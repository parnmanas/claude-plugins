---
allowed-tools: Bash(mkdir:*), Bash(test:*), Bash(ls:*), Read, Write
description: Save a prompt preset to local config
argument-hint: <name> [content...]
---

Save a prompt preset under the user's Claude config directory.

## 인자 파싱

`$ARGUMENTS` 의 첫 단어 = preset 이름. 나머지 = preset content (선택).

- 이름 유효성: 슬래시, `..`, 공백 포함 금지. `[a-zA-Z0-9_-]+` 만 허용.
- 이름만 주어졌으면: 사용자에게 preset 내용을 요청하고 다음 메시지의 본문 전체를 content 로 받는다.

## 저장 경로 해석

shell 에서 다음으로 path 를 해석한다:
```
PRESET_DIR="${CLAUDE_CONFIG_DIR:-$HOME/.claude}/prompt-presets"
```
`mkdir -p "$PRESET_DIR"` 로 디렉토리 확보.

## 쓰기 전 확인

`"$PRESET_DIR/<name>.md"` 가 이미 존재하면 사용자에게 덮어쓸지 확인 (Read 로 기존 content 먼저 보여줌).

## 쓰기

Write 도구로 절대경로 `"$PRESET_DIR/<name>.md"` 에 content 기록. 끝나면 저장 경로만 한 줄로 알린다.
