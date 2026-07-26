# Alpha Talk — Git 브랜치·커밋 규칙 v0.1

> 멀티모듈 모노레포([기획안 §3.1](기획안.md))의 작업 규약. 한 저장소에 여러 서버·라이브러리가 섞여 있으므로 **"무엇을(type) 어느 모듈에(scope)"** 가 로그·브랜치·PR 어디서나 한눈에 보여야 한다.

---

## 1. 원칙

1. **scope = 모듈** — 커밋/브랜치의 범위 토큰은 Gradle 모듈명(`ws`, `contracts`, `auth-jwt`, `core-api`, `worker-*`)으로 고정한다. `git log --oneline`만 봐도 어느 서버가 바뀌었는지 안다.
2. **한 브랜치 = 한 모듈 = 한 목적** — 여러 모듈을 한 브랜치에서 섞지 않는다(예외: §4의 계약 동반 변경).
3. **단계 단위 PR** — 커밋·PR은 계획서의 단계(ws는 S0~S7, 다른 모듈은 각자 정의) 단위로 쪼갠다. PR 본문에 해당 단계와 DoD 충족 여부를 적는다. (CLAUDE.md 컨벤션과 동일)
4. **main은 항상 초록** — main 직접 커밋 금지. 브랜치 → PR → CI 통과 → 머지.

---

## 2. 브랜치 규칙

### 2.1 형식

```
<type>/<scope>/<short-desc>
<type>/<scope>/<stage>-<short-desc>   # 단계가 있는 모듈(ws: S0~S7)은 단계 토큰 권장
```

- `type`: §3.2의 커밋 타입과 동일 집합
- `scope`: §2.3 모듈명
- `short-desc`: 소문자 kebab-case, 2~4단어
- `stage`: `s0`~`s7`(ws), 다른 모듈은 자체 단계/마일스톤(`m1` 등)

### 2.2 예시

```
feat/ws/s4-redis-relay
feat/ws/s5-watchlist-resolve
feat/auth-jwt/token-library
refactor/auth-jwt/autoconfiguration
feat/contracts/channel-codec
feat/core-api/m1-auth-login
fix/ws/heartbeat-timeout
docs/ws/architecture-solid
build/deps/bump-spring-boot
```

### 2.3 스코프(모듈) — 브랜치·커밋 공용

| scope | 대상 |
|---|---|
| `contracts` | `:contracts` |
| `auth-jwt` | `:auth-jwt` |
| `kis-client` | `:kis-client` |
| `db-migrations` | `:db-migrations` |
| `core-api` | `:core-api` |
| `ws` | `:ws` |
| `worker-price` `worker-batch` `worker-ingest` `worker-llm` | 각 워커 |
| `deps` | 버전 카탈로그·의존성 (`gradle/libs.versions.toml`) |
| `build` | 루트 빌드 스크립트·`settings.gradle.kts`·`gradlew` |
| `infra` | `docker-compose.yml`·`infra/` |
| `ci` | `.github/workflows/` |
| *(없음)* | 저장소 전역 문서(`기획안.md` 등)·`.gitignore` 등 특정 모듈 아닌 변경 |

### 2.4 수명주기

```
main 에서 분기 → 작업·커밋 → push → PR(Draft 가능)
   → CI(./gradlew build) 통과 → 리뷰 → Squash merge → 브랜치 삭제
```

- **Squash merge 권장**: main에는 단계/기능당 **커밋 1개**만 남아 히스토리가 단계별로 읽힌다. 이때 **PR 제목이 그대로 squash 커밋 메시지**가 되므로, PR 제목도 §3 커밋 규칙을 따른다.
- 브랜치 안에서는 자유롭게 여러 번 커밋해도 된다(어차피 합쳐진다). 단, 각 커밋도 §3 형식을 지키면 리뷰가 쉽다.

---

## 3. 커밋 규칙 — Conventional Commits

### 3.1 형식

```
<type>(<scope>): <제목>

<본문 — 선택. 왜/무엇을 바꿨는지>

<푸터 — 선택. 단계·이슈·BREAKING·공동작성>
```

### 3.2 타입 vs 스코프 (혼동 주의)

**type은 "변경의 성격", scope는 "어느 모듈"** 이다. 둘은 다른 축이다.

| type | 쓸 때 |
|---|---|
| `feat` | 기능 추가 |
| `fix` | 버그 수정 |
| `refactor` | 동작 불변, 구조 개선 |
| `perf` | 성능 개선 |
| `test` | 테스트만 추가/수정 |
| `docs` | 문서만 (`md/`, 주석, README) |
| `build` | 빌드·의존성 (gradle, 버전 카탈로그) |
| `ci` | CI 설정 |
| `chore` | 위에 안 걸리는 잡무 (설정 파일 등) |
| `style` | 포맷팅만 (ktlint 등, 의미 불변) |

> 예: `ws`의 문서를 고치면 **`docs(ws):`** — type은 docs, scope는 ws. `libs.versions.toml`을 올리면 **`build(deps):`**.

### 3.3 제목 규칙

- 한국어/영어 중 하나로 **일관되게**. 타입·스코프는 항상 영어 소문자.
- **명령형·현재형**, 마침표 없이. ("추가한다"가 아니라 "추가", "added"가 아니라 "add")
- 약 50자 이내. "무엇을 했는지"가 한 줄로 보이게.

### 3.4 본문·푸터

- **본문**: *왜* 바꿨는지(문제/근거). *어떻게*는 코드가 말하므로 생략 가능.
- **푸터 관례**:
  - `Phase: S4` — 계획서 단계 참조 (선택)
  - `Refs: #12` / `Closes: #12` — 이슈 연결
  - `BREAKING CHANGE: <내용>` — `:contracts`/`:auth-jwt` 등 **공유 계약**이 깨지는 변경은 반드시 명시 (소비 모듈 전체 영향)
  - `Co-Authored-By: ...` — AI 도구(Claude Code) 등과 함께 작업한 커밋

### 3.5 예시 (실제 작업 기준)

```
feat(ws): DemandRegistry refcount 전이 구현

관심목록·방 구독의 0↔1 전이를 감지해 Redis 채널을 subscribe/unsubscribe.
ChannelSubscriber 포트에 위임해 Redis 없이 단위 테스트한다.

Phase: S3
```

```
refactor(auth-jwt): 수동 결선을 Boot 자동구성으로 전환

서버마다 AuthConfig를 작성하던 것을 제거. 의존성 + alphatalk.auth.jwt.secret
프로퍼티만으로 TokenVerifier 빈이 등록된다.
```

```
feat(contracts): Channels 생성·파싱을 단일 지점으로

BREAKING CHANGE: 채널명 문자열 직접 조립 금지. ws·워커는 Channels.of/parse 사용.
```

```
build(deps): Spring Boot 3.5.16, Kotlin 2.2.21로 고정
docs(ws): 아키텍처를 MVC STOMP 기준으로 개정 (v0.2)
test(ws): 관심목록 diff broadcast-and-filter 케이스 추가
ci: PR·main push에 gradle build 워크플로 추가
```

---

## 4. 여러 모듈에 걸치는 변경

원칙은 **모듈당 커밋 분리**다. 하지만 계약(`contracts`/`auth-jwt`) 변경은 소비 모듈까지 한 번에 바꿔야 컴파일이 되는 경우가 있다.

- **가능하면 순서로 분리**: 먼저 `feat(contracts): ...` (하위호환 추가) → 다음 `feat(ws): ... 사용`. 두 커밋, 각자 초록.
- **원자적이어야 하면**: 한 커밋에 넣되 scope는 **계약 모듈**로 잡고(`feat(contracts):`) 본문에 영향받는 소비 모듈을 적는다. `BREAKING CHANGE` 푸터 필수.
- 브랜치도 이 경우 계약 모듈 기준으로 명명: `feat/contracts/add-alert-channel`.

---

## 5. PR 규칙

- **제목** = §3 커밋 형식 (Squash 시 그대로 커밋 메시지가 됨). 예: `feat(ws): S4 Redis relay end-to-end`
- **본문**에 반드시:
  1. 해당 **단계(S/M)와 DoD 충족 여부** (CLAUDE.md 컨벤션)
  2. 변경 요약 / 테스트 방법
  3. 계약 변경 시 영향 모듈
- **머지 조건**: `./gradlew build` 통과 + 리뷰 승인. `DemandRegistry` 등 refcount/인덱스 변경은 단위 테스트 동반(CLAUDE.md 불변 규칙).

---

## 6. 빠른 참조

```
브랜치   type/scope/desc            feat/ws/s4-redis-relay
커밋     type(scope): 제목          feat(ws): Redis relay 구현
PR 제목  = 커밋 형식               feat(ws): S4 Redis relay end-to-end
머지     Squash → 브랜치 삭제
scope    = 모듈명 (표 §2.3)        ws · contracts · auth-jwt · core-api · worker-* · deps · build · infra · ci
```
