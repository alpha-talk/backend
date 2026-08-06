프로젝트 지침의 단일 진실은 AGENTS.md다. 지침 수정은 AGENTS.md에서만 한다 — 이 파일에는 Claude Code 하네스에서만 동작하는 도구 설정만 덧붙인다.

@AGENTS.md

## 코드 리뷰 게이트 (Claude Code 전용)

코드 변경은 **작성 → 자체 리뷰 → 교차 리뷰(Codex) → 통과 후 종료** 순서를 거친다. 빌드·테스트 통과는 완료 조건이 아니라 리뷰 진입 조건이다.

1. **자체 리뷰**: 구현한 에이전트가 자기 변경을 적대적으로 다시 읽는다. `md/coding_convention.md` §5(운영 실패·규모)와 AGENTS.md의 불변 규칙 위반을 우선 확인하고, 발견이 없어도 "발견 없음"을 보고한다.
2. **교차 리뷰**: Codex가 같은 변경을 독립적으로 리뷰한다. 자체 리뷰와 교차 리뷰는 서로를 대체하지 않는다 — 같은 모델이 만든 사각지대는 같은 모델이 못 찾는다.
3. **지적 처리**: 리뷰 지적은 고치거나, 고치지 않을 근거를 남긴다. 조용히 넘기지 않는다.

2단계는 Codex 플러그인의 **stop-time review gate**로 자동화한다. 켜두면 코드를 수정한 턴이 끝날 때마다 Codex 리뷰가 자동 실행되고, `BLOCK`이면 지적을 해소해야 턴이 종료된다. 코드 변경이 없는 턴은 그냥 통과한다.

```bash
/codex:setup --enable-review-gate    # 게이트 켜기
/codex:setup --disable-review-gate   # 끄기
/codex:review --background           # 게이트와 별개로 현재 diff 전체를 리뷰
/codex:adversarial-review            # 커스텀 지시를 얹은 공격적 리뷰
```

게이트 설정은 저장소가 아니라 `~/.claude/plugins/data/codex-inline/state/`에 워크스페이스 경로 기준으로 저장된다 — **작업 환경마다 각자 한 번 켜야 한다**. Codex CLI가 없거나 로그인이 안 되어 있으면 게이트가 동작하지 않으므로 `/codex:setup`으로 먼저 확인한다. 게이트가 꺼져 있으면 2단계를 `/codex:review`로 수동 수행하며, **교차 리뷰 없이 코드 변경을 완료로 보고하지 않는다**.
