# prompt-presets

자주 쓰는 prompt 를 사용자 로컬에 preset 으로 저장해두고 slash command 로 불러 쓰는 플러그인.

Preset 파일은 플러그인 repo 가 아니라 **사용자의 Claude config 디렉토리**에 저장된다.

## 저장 위치

```
${CLAUDE_CONFIG_DIR:-~/.claude}/prompt-presets/<name>.md
```

- `CLAUDE_CONFIG_DIR` 환경변수가 있으면 그 폴더 기준.
- 없으면 `~/.claude` 기준.

## 커맨드

| Command | 설명 |
| --- | --- |
| `/prompt-presets:save <name> [content]` | Preset 저장. content 생략 시 다음 메시지 본문을 받는다. |
| `/prompt-presets:run <name>` | 저장된 preset 을 방금 보낸 요청처럼 실행. |
| `/prompt-presets:list` | 저장된 preset 목록. |
| `/prompt-presets:edit <name>` | 기존 preset 수정. |
| `/prompt-presets:rm <name>` | Preset 삭제. |

## 예시 사용 흐름

1. `/prompt-presets:save commit-push-all` → 내용 입력:
   ```
   parent repo 와 모든 submodule 의 변경을 한 번에 commit + push. 민감 파일 섞이면 경고. --force/--no-verify 금지.
   ```
2. `/prompt-presets:run commit-push-all` → Claude 가 저장된 지시를 읽어 그대로 수행.
