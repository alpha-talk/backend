# Alpha Talk — backend (Gradle 멀티모듈 모노레포)

"종목 하나 = 방 하나" 주식 커뮤니티 **Alpha Talk**의 백엔드 저장소.
**한 저장소에 여러 서버(Spring Boot 앱)를 Gradle 서브모듈로 담는다.** 서버끼리는 코드로 의존하지 않고 Redis/DB 계약으로만 통신한다. 전체 모듈 목록·의존 규칙·포트 배치는 [md/기획안.md](md/기획안.md) §3.1이 단일 진실.

- **서버(앱)**: `core-api`(메인 REST) · `ws`(WS 게이트웨이) · `worker-price/batch/ingest/llm`
- **라이브러리(공유)**: `contracts`(채널·키·봉투 DTO) · `auth-jwt`(JWT 발급·검증) · `kis-client`(KIS 연동)

**현재 집중 = `ws` 모듈** (실시간 푸시 전용 WebSocket(STOMP) 게이트웨이). 아래 스택·불변 규칙은 `ws` 모듈 기준이며, 다른 서버는 각자 착수 시 문서를 보강한다.

## 문서 우선 — 코드보다 문서가 먼저다

설계 결정은 전부 `md/`에 있다. 구현·리뷰 전에 해당 문서를 먼저 확인할 것.

| 문서 | 역할 |
|---|---|
| [md/ws_architecture.md](md/ws_architecture.md) | **코드 레벨 설계 기준** — 컴포넌트 책임, 인덱스/동시성, 시퀀스, 에러 정책 |
| [md/ws_module_plan.md](md/ws_module_plan.md) | 구현 계획 — 단계(S0~S7)별 범위·DoD, 열린 합의 안건 |
| [md/ws_api_spec.md](md/ws_api_spec.md) | 클라 ↔ 게이트웨이 STOMP 프로토콜 계약 (v0.5) |
| [md/redis_contract.md](md/redis_contract.md) | 게이트웨이 ↔ 워커 ↔ 메인서버 Redis 계약 — **서비스 간 단일 진실** (v0.5) |
| [md/alphatalk_core_api_spec.md](md/alphatalk_core_api_spec.md) | 클라 ↔ 메인서버 REST 계약 (v0.1 + §12 구현 노트) — core-api 구현 기준 |
| [md/alphatalk_kis_worker_spec.md](md/alphatalk_kis_worker_spec.md) | KIS/OpenDART 수집 워커 명세 — 워커 적재 테이블 스키마(§4)의 원천 |
| [md/alphatalk_news_worker_spec.md](md/alphatalk_news_worker_spec.md) | 뉴스 파이프라인 명세 — worker-ingest·worker-llm (수집·클러스터링·일일 호재/악재 브리핑) |
| [md/기획안.md](md/기획안.md) | 전체 서비스 요구사항·아키텍처 (4개 처리 평면) |

문서와 코드가 어긋나면: 코드를 문서에 맞추거나, 문서를 먼저 고치고 나서 코드를 바꾼다. 조용히 어긋난 채 두지 않는다.

## 기술 스택

- Kotlin 2.x / JVM 21 / Spring Boot 3.5.x (3.x 최신 패치)
- WebSocket: **Spring MVC 스택 + `@EnableWebSocketMessageBroker`(SimpleBroker)** — WebFlux 아님 (v0.2에서 전환 결정)
- Redis: Spring Data Redis (Lettuce), Pub/Sub 구독 전용
- 빌드: Gradle Kotlin DSL 멀티모듈 + `gradle/libs.versions.toml`
- 테스트: JUnit5 + Testcontainers(Redis) + `WebSocketStompClient`

## 모듈 구조 (저장소 전체)

```
backend/
├─ settings.gradle.kts · build.gradle.kts · gradle/libs.versions.toml
├─ docker-compose.yml                    # 로컬 PG·Redis
├─ contracts/    [라이브러리] 채널·키·STOMP 목적지·봉투 DTO (Spring 무의존)
├─ auth-jwt/     [라이브러리] JWT 발급·검증 (TokenIssuer/TokenVerifier, Spring 무의존) — core-api·ws 공유
├─ kis-client/   [라이브러리] KIS 인증·유량제어·REST/WS 클라
├─ core-api/     [서버:8080]  메인 REST (MVC + Modulith)
├─ ws/           [서버:8081]  게이트웨이 — config/ auth/ subscription/ relay/ client/ watchlist/ presence/
├─ worker-price/ [서버]       실시간 시세 수집
├─ worker-batch/ [서버]       마스터/봉/수급/재무 배치
├─ worker-ingest/[서버]       뉴스 수집
└─ worker-llm/   [서버]       LLM 요약
```

**의존 규칙**: 서버는 라이브러리에만 의존한다 — **서버 → 서버 의존 금지**. `contracts`·`auth-jwt`는 순수(무의존). `ws → contracts·auth-jwt` 단방향. 상세는 [md/기획안.md](md/기획안.md) §3.1.
**인증 규칙**: JWT 발급·검증 코드는 반드시 `:auth-jwt`를 쓴다 — 서버마다 따로 구현하면 클레임 스키마가 어긋난다. Boot 자동구성이라 **의존성 추가 + `alphatalk.auth.jwt.secret` 프로퍼티만으로 빈이 등록**된다(결선 코드 작성 금지). ws는 `TokenVerifier` 타입으로만 주입(검증 전용, 발급 금지).
지금 존재하는 모듈만 `settings.gradle.kts`에 include하고, 나머지 서버는 착수 시점에 등록한다 (M0 스캐폴딩: `contracts → ws → core-api`).

인프라 경계(Redis·브로커·JWT)는 **포트(인터페이스)** 뒤에 둔다. 도메인 로직은 구체 인프라를 컴파일타임에 모른다: `ChannelSubscriber`·`ClientMessageSink`·`TokenVerifier`·`WatchlistResolver`·`PresenceRegistry`·`RedisChannelHandler`. 포트는 도메인 패키지에 구현과 함께 두고 별도 `port/` 패키지로 몰지 않는다. 상세 근거는 [md/ws_architecture.md](md/ws_architecture.md) §3(SOLID 적용).

## 불변 규칙 (어기면 설계 위반)

1. **푸시 전용 엣지**: 클라 SEND 프레임은 무조건 거부. 콘텐츠를 받는 코드를 만들지 않는다.
2. **게이트웨이는 Redis Pub/Sub SUBSCRIBE + 프레즌스 쓰기만**: DB 접근, `queue:ingest`(Streams), `price:{code}` 캐시 읽기 코드 금지.
3. **채널명·키·목적지 문자열 하드코딩 금지**: 반드시 `:contracts`의 `Channels`/`Keys`/`Destinations` 상수 사용.
4. **best-effort 푸시**: 재전송·전달 보장 로직을 만들지 않는다. 유실 복구는 클라의 REST 몫.
5. **보안**: JWT는 STOMP CONNECT 헤더로만 받는다 (URL 쿼리파라미터 금지). 토큰 원문·시크릿 로그 금지. 시크릿은 환경변수 주입.
6. **인메모리 상태는 DemandRegistry가 단일 소유**: 세션/수요 인덱스를 다른 곳에 중복 보관하지 않는다. 재기동 시 0에서 재구축이 전제 — 영속화하지 않는다. 외부에는 읽기용 `DemandQuery`·쓰기용 `DemandMutator` 역할 인터페이스로만 노출한다(단일 소유는 유지, ISP로 좁게 노출).
7. **경계는 포트로 뒤집는다(DIP)**: 도메인 클래스가 `LettuceConnectionFactory`·`SimpMessagingTemplate`·JWT 라이브러리를 직접 import하지 않는다. 새 인프라 의존이 생기면 포트부터 정의한다 — 그래야 인프라 없이 단위 테스트된다.

## 빌드·실행

```bash
./gradlew build            # 전 모듈 빌드 + 테스트
./gradlew :ws:test         # 게이트웨이 테스트만
./gradlew :ws:bootRun --args='--spring.profiles.active=local'   # 로컬 실행 (Redis 필요: docker compose up -d)
# JWT 시크릿은 기본값 없음(fail-closed) — 로컬은 local 프로파일, 운영은 ALPHATALK_AUTH_JWT_SECRET 환경변수
```

> **에이전트 실행 허용**: `./gradlew` 테스트·빌드 명령(`build`, `:모듈:test`, `check`, `compileKotlin`, `assemble` 등)은 확인 없이 실행해도 된다. 변경한 코드는 관련 모듈 테스트로 검증하는 것을 기본으로 한다. `bootRun`·`docker compose` 같은 장기 실행·외부 부작용 명령은 예외로 사용자에게 먼저 확인한다.

로컬 인프라는 루트 `docker-compose.yml`(Redis). 통합 테스트는 Testcontainers가 자체 기동하므로 별도 준비 불요.

## 컨벤션

- **코딩 컨벤션**: [md/coding_convention.md](md/coding_convention.md). 핵심 — **코드에 주석을 달지 않는다**(self-documenting code). 설계 근거·불변식·포트 계약은 코드가 아니라 `md/` 설계 문서가 소유하고, 코드에서는 테스트로 고정한다.
- **브랜치·커밋·PR 규칙**: [md/git_convention.md](md/git_convention.md). 핵심 — 브랜치 `type/scope/desc`, 커밋 `type(scope): 제목`, **scope=모듈명**(ws·contracts·auth-jwt·core-api·worker-*). main 직접 커밋 금지.
- 커밋·PR은 계획서의 단계(S0~S7) 단위. PR 본문에 해당 단계와 DoD 충족 여부를 적는다.
- 테스트 없는 refcount/인덱스 변경 금지 — `DemandRegistry` 전이 로직은 반드시 단위 테스트 동반.
- 로그는 구조화(JSON) 지향, `sessionId`/`userId` 태깅. 뜨거운 경로(틱 relay)에 debug 이상 로그 금지.
- 새 Redis 채널/키가 필요하면 코드보다 먼저 [md/redis_contract.md](md/redis_contract.md)에 합의 내용을 반영한다 (팀 공유 계약).

## 현재 상태 (2026-07 기준)

- **S0~S5 구현 완료**: `:contracts` + `:auth-jwt` + `:ws` — 인증(JWT CONNECT)·수요 인덱스(DemandRegistry)·Redis relay·관심목록 해소·watchlist:updated·프레즌스·메트릭. 테스트 49개(E2E 4개 포함) 통과.
- 버전: Spring Boot 3.5.16 · Kotlin 2.2.21 · JDK 21(toolchain 자동 다운로드) · jjwt 0.12.7.
- 남은 단계: **S6**(graceful shutdown 시나리오 검증, quote 샘플러 여부 판단) · **S7**(41종목×500세션 부하 스모크). 계획서 §5 참조.
- 미해결 합의 안건은 [md/ws_module_plan.md](md/ws_module_plan.md) §7 (RS256 전환 여부, 관심목록 조회 경로 등). 해당 코드는 포트로 격리된 구현(`:auth-jwt`의 `JwtTokenProvider` HS256, `RedisWatchlistResolver`)을 쓴다.
- **뉴스 파이프라인 N0~N6 구현 완료**: `:worker-ingest`(RSS·네이버 검색 수집→정규화→매핑→XADD + 다이제스트 트리거, 테스트 31개) + `:worker-llm`(소비→클러스터링(pgvector)→LLM 요약·감성→scope 사다리 fan-out→persist·publish·ack + 일일 브리핑·DLQ, 테스트 57개). LLM·임베딩 키는 fail-closed(로컬·테스트만 allow-fake). 상세·잔여 설정은 [md/alphatalk_news_worker_spec.md](md/alphatalk_news_worker_spec.md) §9.
