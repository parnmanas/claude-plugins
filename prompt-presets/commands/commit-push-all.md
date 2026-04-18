---
description: Parent + 모든 submodule 의 변경사항을 commit 하고 push 한다
---

현재 작업 중인 모든 변경사항을 한 턴에 commit + push 한다.

## 절차

1. Parent repo 와 각 submodule 의 상태를 병렬로 확인한다 (`git status`, `git diff`, `git log --oneline -5`).
2. 변경이 있는 submodule 부터 처리한다:
   - submodule 내부에서 의미 단위로 commit.
   - 현재 추적 브랜치로 push.
3. submodule ref 가 바뀐 경우 parent repo 에서 submodule pointer 를 함께 stage 한다.
4. Parent repo 의 남은 변경과 bumped submodule ref 를 commit + push.
5. 최종 `git status` 로 모든 repo 가 clean 한지 확인한다.

## 규칙

- 메시지는 repo 별 최근 커밋 스타일을 따른다 (영어/한글, prefix 사용 여부 등).
- `--no-verify`, `--force`, `reset --hard` 금지. hook 실패 시 근본 원인 수정 후 새 commit.
- 민감 파일(`.env`, credentials) 이 섞여 있으면 stage 전에 경고.
- 남는 변경이 있으면 안 된다. 모두 올린 뒤 마무리.
