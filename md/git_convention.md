# Alpha Talk — Git 브랜치·커밋 규칙 v0.1

> 이 저장소에서 브랜치를 만들고 커밋을 쓰고 PR을 올리는 모든 작업(사람·에이전트)의 규약이다. 이 저장소는 여러 서버·라이브러리를 Gradle 모듈로 담는 멀티모듈 모노레포라([기획안 §3.1](기획안.md)) **"무엇을(type) 어느 모듈에(scope)"** 바꿨는지가 로그·브랜치·PR 어디서나 한눈에 보여야 히스토리가 읽힌다. 아래 규칙은 전부 이 하나를 지키는 장치다. 모듈 목록·의존 규칙은 기획안 §3.1, 단계(S0~S7) 정의·DoD는 각 모듈 계획서 몫이고 이 문서는 표기 형식과 머지 절차만 정한다.

---

## 1. 원칙

아래 네 원칙에서 나머지 규칙이 전부 나온다.

1. **scope = 모듈** — 커밋/브랜치의 범위 토큰은 Gradle 모듈명(`ws`, `contracts`, `auth-jwt`, `core-api`, `worker-*`)으로 고정한다. 그래야 `git log --oneline`만 봐도 어느 서버가 바뀌었는지 안다.
2. **한 브랜치 = 한 모듈 = 한 목적** — 여러 모듈을 한 브랜치에서 섞지 않는다. 섞이면 scope 토큰 하나로 브랜치를 이름 붙일 수 없고 로그 추적도 깨진다. (예외: §4의 계약 동반 변경)
3. **단계 단위 PR** — 커밋·PR은 계획서의 단계(ws는 S0~S7, 다른 모듈은 각자 정의) 단위로 쪼갠다. 이렇게 쪼개야 Squash 머지 뒤 main 히스토리가 단계당 커밋 1개로 읽힌다(§2.4). PR 본문에는 해당 단계와 DoD 충족 여부를 적는다. (`AGENTS.md` 컨벤션과 동일)
4. **main은 항상 초록** — main 직접 커밋 금지. 브랜치 → PR → CI 통과 → 머지 순서만 허용해 CI를 거치지 않은 커밋이 main에 들어갈 길을 없앤다.

---

## 2. 브랜치 규칙

브랜치 이름에도 커밋과 같은 type·scope 축을 써서 이름만으로 어느 모듈의 어떤 작업인지 드러낸다.

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

scope는 Gradle 모듈명이 기본이고 모듈이 아닌 영역은 아래 전용 토큰을 쓴다.

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

- **Squash merge 권장**: main에 단계/기능당 **커밋 1개**만 남겨 히스토리를 단계별로 읽히게 한다. 이때 **PR 제목이 그대로 squash 커밋 메시지**가 되므로 PR 제목도 §3 커밋 규칙을 따른다.
- 브랜치 안에서는 자유롭게 여러 번 커밋해도 된다(어차피 합쳐진다). 단, 각 커밋도 §3 형식을 지키면 리뷰가 쉽다.

---

## 3. 커밋 규칙 — Conventional Commits

커밋 한 줄에 "어느 모듈에 어떤 성격의 변경인지"가 담기도록 Conventional Commits 형식을 쓴다.

### 3.1 형식

```
<type>(<scope>): <제목>

<본문 — 선택. 왜/무엇을 바꿨는지>

<푸터 — 선택. 단계·이슈·BREAKING>
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

- 한국어/영어 중 하나로 **일관되게** 쓴다. 타입·스코프는 항상 영어 소문자.
- **명령형·현재형**, 마침표 없이. ("추가한다"가 아니라 "추가", "added"가 아니라 "add")
- 약 50자 이내로 "무엇을 했는지"가 한 줄에 보이게 쓴다.

### 3.4 본문·푸터

- **본문**: *왜* 바꿨는지(문제/근거)를 적는다. *어떻게*는 코드가 말하므로 생략해도 된다.
- **푸터 관례**:
  - `Phase: S4` — 계획서 단계 참조 (선택)
  - `Refs: #12` / `Closes: #12` — 이슈 연결
  - `BREAKING CHANGE: <내용>` — `:contracts`/`:auth-jwt` 등 **공유 계약**이 깨지는 변경은 반드시 명시한다. 계약이 깨지면 소비 모듈 전체가 영향을 받는다.

- **공동 저자 서명 금지**: 커밋 메시지와 PR 본문에 `Co-Authored-By` 등 공동 저자 트레일러를 넣지 않는다. AI 도구 서명도 포함한다.

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

원칙은 **모듈당 커밋 분리**다(§1). 다만 계약(`contracts`/`auth-jwt`) 변경은 소비 모듈까지 한 번에 바꿔야 컴파일이 되는 경우가 있어 예외를 둔다.

- **가능하면 순서로 분리한다**: 먼저 `feat(contracts): ...`(하위호환 추가) → 다음 `feat(ws): ... 사용`. 두 커밋이 각자 초록이면 된다.
- **원자적이어야 하면**: 한 커밋에 넣되 scope는 **계약 모듈**로 잡는다(`feat(contracts):`). 본문에 영향받는 소비 모듈을 적고 `BREAKING CHANGE` 푸터를 반드시 단다.
- 이 경우 브랜치도 계약 모듈 기준으로 명명한다: `feat/contracts/add-alert-channel`.

---

## 5. PR 규칙

- **제목** = §3 커밋 형식. Squash 머지 시 제목이 그대로 커밋 메시지가 된다. 예: `feat(ws): S4 Redis relay end-to-end`
- **본문**에 반드시 적는다:
  1. 해당 **단계(S/M)와 DoD 충족 여부** (`AGENTS.md` 컨벤션)
  2. 변경 요약 / 테스트 방법
  3. 계약 변경 시 영향 모듈
- **머지 조건**: `./gradlew build` 통과 + 리뷰 승인. `DemandRegistry` 등 refcount/인덱스 변경은 단위 테스트를 동반한다(`AGENTS.md` 불변 규칙).

### 5.1 PR의 base는 언제나 `main`이다

여러 단계를 잇달아 올릴 때도 **각 PR의 base를 main으로 둔다.** 앞 PR의 브랜치를 base로 삼는 "스택 PR"은 만들지 않는다.

앞 브랜치를 base로 두면 GitHub이 그 브랜치로 머지한다. PR 목록에는 전부 "Merged"로 뜨지만 코드는 중간 브랜치에 갇히고 main에는 첫 단계만 들어간다. 실제로 이 저장소에서 worker-price P4~P7이 이렇게 유실돼 회수 PR을 따로 열어야 했다.

단계가 여러 개면 둘 중 하나로 한다.

- **한 PR로 합친다** — 리뷰 단위가 커도 되면 이쪽이 가장 안전하다. 브랜치 안에서 단계별 커밋을 유지하면 리뷰어가 커밋 단위로 읽을 수 있다.
- **앞 PR을 머지한 뒤 다음 브랜치를 main 위로 rebase해서 연다** — 리뷰를 단계별로 쪼개야 할 때. 앞 PR이 머지되기 전에는 다음 PR을 열지 않는다.

앞 단계에 의존하는 코드를 미리 작업해야 한다면 로컬에서 그 브랜치 위에 쌓되, **PR은 앞 단계가 main에 들어간 뒤에 연다.**

---

## 6. 빠른 참조

```
브랜치   type/scope/desc            feat/ws/s4-redis-relay
커밋     type(scope): 제목          feat(ws): Redis relay 구현
PR 제목  = 커밋 형식               feat(ws): S4 Redis relay end-to-end
PR base  = 항상 main (§5.1)        스택 PR 금지 — 앞 PR 머지 후 rebase
머지     Squash → 브랜치 삭제
scope    = 모듈명 (표 §2.3)        ws · contracts · auth-jwt · core-api · worker-* · deps · build · infra · ci
```
