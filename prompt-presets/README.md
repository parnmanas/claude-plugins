# prompt-presets

자주 쓰는 prompt 를 미리 정의해두고 slash command 로 호출하는 플러그인.

## 사용법

1. `commands/<name>.md` 파일을 추가한다.
2. Claude Code 에서 `/prompt-presets:<name>` 으로 호출한다.

## 파일 형식

```markdown
---
description: 한 줄 설명
---

여기에 Claude 에게 전달할 prompt 본문을 쓴다.
`!`\``명령``` 로 shell 을 끼워넣거나 `@path/to/file` 로 파일 참조 가능.
```

## 새 preset 추가 가이드

- 커밋/푸시용 문장을 반복 입력하기 싫을 때 추가한다.
- 파일명이 곧 슬래시 커맨드 이름이다. `commands/foo.md` → `/prompt-presets:foo`.
- description frontmatter 는 슬래시 커맨드 목록에 표시된다.
