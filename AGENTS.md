# Alpha Talk — backend (Gradle 멀티모듈 모노레포)

"종목 하나 = 방 하나" 주식 커뮤니티 **Alpha Talk**의 백엔드 저장소.
**한 저장소에 여러 서버(Spring Boot 앱)를 Gradle 서브모듈로 담는다.** 서버끼리는 코드로 의존하지 않고 Redis/DB 계약으로만 통신한다. 전체 모듈 목록·의존 규칙·포트 배치는 [md/기획안.md](md/기획안.md) §3.1이 단일 진실.

- **서버(앱)**: `core-api`(메인 REST) · `ws`(WS 게이트웨이) · `worker-price/batch/ingest/llm`
- **라이브러리(공유)**: `contracts`(채널·키·봉투 DTO) · `auth-jwt`(JWT 발급·검증) · `kis-client`(KIS 연동) · `db-migrations`(Liquibase changelog — DB 스키마 단일 소유)

**현재 집중 = `ws` 모듈** (실시간 푸시 전용 WebSocket(STOMP) 게이트웨이). 아래 스택·불변 규칙은 `ws` 모듈 기준이며, 다른 서버는 각자 착수 시 문서를 보강한다.

## 문서 우선 — 코드보다 문서가 먼저다

설계 결정은 전부 `md/`에 있다. 구현·리뷰 전에 해당 문서를 먼저 확인할 것.

| 문서 | 역할 |
|---|---|
| [md/ws_architecture.md](md/ws_architecture.md) | **코드 레벨 설계 기준** — 컴포넌트 책임, 인덱스/동시성, 시퀀스, 에러 정책 |
| [md/ws_module_plan.md](md/ws_module_plan.md) | 구현 계획 — 단계(S0~S7)별 범위·DoD, 열린 합의 안건 |
| [md/ws_api_spec.md](md/ws_api_spec.md) | 클라 ↔ 게이트웨이 STOMP 프로토콜 계약 (v0.9) |
| [md/redis_contract.md](md/redis_contract.md) | 게이트웨이 ↔ 워커 ↔ 메인서버 Redis 계약 — **서비스 간 단일 진실** (v0.22) |
| [md/alphatalk_core_api_spec.md](md/alphatalk_core_api_spec.md) | 클라 ↔ 메인서버 REST 계약 (v0.3) — core-api 구현 기준 |
| [md/alphatalk_kis_worker_spec.md](md/alphatalk_kis_worker_spec.md) | KIS/OpenDART 수집 워커 명세 — 워커 적재 테이블 스키마(§4)의 원천 |
| [md/alphatalk_news_worker_spec.md](md/alphatalk_news_worker_spec.md) | 뉴스 파이프라인 명세 — worker-ingest·worker-llm (수집·클러스터링·일일 호재/악재 브리핑) |
| [md/local_embedding_setup.md](md/local_embedding_setup.md) | 로컬 무료 임베딩 실행 가이드 — Ollama+BGE-M3 설정·검증·문제 해결 |
| [md/기획안.md](md/기획안.md) | 전체 서비스 요구사항·아키텍처 (4개 처리 평면) |

문서와 코드가 어긋나면: 코드를 문서에 맞추거나, 문서를 먼저 고치고 나서 코드를 바꾼다. 조용히 어긋난 채 두지 않는다.

## 기술 스택

- Kotlin 2.x / JVM 21 / Spring Boot 3.5.x (3.x 최신 패치)
- WebSocket: **Spring MVC 스택 + `@EnableWebSocketMessageBroker`(SimpleBroker)** — WebFlux 아님 (v0.2에서 전환 결정)
- Redis: Spring Data Redis (Lettuce), Pub/Sub 구독 전용
- 관계형 DB 접근: DB를 사용하는 모든 서버 모듈은 **Spring Data JPA를 기본이자 우선 구현으로 사용한다**. 우선순위는 `JpaRepository` 기본 CRUD·파생 쿼리(필요 시 projection·`@EntityGraph`) → `@Query` JPQL(DTO projection·fetch join 포함)이다. native query, `JdbcTemplate`, `JdbcClient`, 직접 JDBC와 문자열 SQL은 에이전트가 자체 판단으로 도입하지 않는다. 불가피하다고 판단하면 구현 전에 JPA/JPQL로 해결할 수 없는 근거와 측정 결과를 제시하고 사용자 합의를 받은 뒤 설계 문서에 예외를 기록한다.
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
├─ db-migrations/[라이브러리] Liquibase changelog — DB 쓰는 서버가 의존, 기동 시 적용
├─ core-api/     [서버:8080]  메인 REST (MVC + Modulith)
├─ ws/           [서버:8081]  게이트웨이 — config/ auth/ subscription/ relay/ client/ watchlist/ presence/
├─ worker-price/ [서버]       실시간 시세 수집
├─ worker-batch/ [서버]       마스터/봉/수급/재무 배치
├─ worker-ingest/[서버]       뉴스 수집
└─ worker-llm/   [서버]       LLM 요약
```

**의존 규칙**: 서버는 라이브러리에만 의존한다 — **서버 → 서버 의존 금지**. `contracts`·`auth-jwt`는 순수(무의존). `ws → contracts·auth-jwt` 단방향. 상세는 [md/기획안.md](md/기획안.md) §3.1. **유일한 서버 간 HTTP 예외**: core-api → worker-price 분봉 신선화 내부 API — 멱등 트리거·응답에 데이터 없음·best-effort(실패해도 조회는 저장분으로 동작)로 한정하며, 데이터 전달 채널로 확장하지 않는다([md/alphatalk_kis_worker_spec.md](md/alphatalk_kis_worker_spec.md) §2.6).
**인증 규칙**: JWT 발급·검증 코드는 반드시 `:auth-jwt`를 쓴다 — 서버마다 따로 구현하면 클레임 스키마가 어긋난다. Boot 자동구성이라 **의존성 추가 + `alphatalk.auth.jwt.secret` 프로퍼티만으로 빈이 등록**된다(결선 코드 작성 금지). ws는 `TokenVerifier` 타입으로만 주입(검증 전용, 발급 금지).
지금 존재하는 모듈만 `settings.gradle.kts`에 include하고, 나머지 서버는 착수 시점에 등록한다 (M0 스캐폴딩: `contracts → ws → core-api`).

인프라 경계(Redis·브로커·JWT)는 **포트(인터페이스)** 뒤에 둔다. 도메인 로직은 구체 인프라를 컴파일타임에 모른다: `ChannelSubscriber`·`ClientMessageSink`·`TokenVerifier`·`WatchlistResolver`·`PresenceRegistry`·`RedisChannelHandler`. 포트는 도메인 패키지에 구현과 함께 두고 별도 `port/` 패키지로 몰지 않는다. 상세 근거는 [md/ws_architecture.md](md/ws_architecture.md) §3(SOLID 적용).

## 불변 규칙 (어기면 설계 위반)

1. **푸시 전용 엣지**: 클라 SEND 프레임은 무조건 거부. 콘텐츠를 받는 코드를 만들지 않는다.
2. **게이트웨이는 Redis Pub/Sub SUBSCRIBE + 프레즌스·수요(demand) 쓰기만**: 수요 쓰기는 `demand:*:{gwId}` refcount·`demand:updated` 전이 발행·`gw:alive:{gwId}` 하트비트·`gw:registry` 자기 gwId 등록/철수([redis_contract.md](md/redis_contract.md) §1.3·§3 v0.20)로 한정. DB 접근, `queue:ingest`(Streams), `price:{code}` 캐시 읽기 코드 금지.
3. **채널명·키·목적지 문자열 하드코딩 금지**: 반드시 `:contracts`의 `Channels`/`Keys`/`Destinations` 상수 사용.
4. **best-effort 푸시**: 재전송·전달 보장 로직을 만들지 않는다. 유실 복구는 클라의 REST 몫.
5. **보안**: JWT는 STOMP CONNECT 헤더로만 받는다 (URL 쿼리파라미터 금지). 토큰 원문·시크릿 로그 금지. 시크릿은 환경변수 주입.
6. **인메모리 상태는 DemandRegistry가 단일 소유**: 세션/수요 인덱스를 다른 곳에 중복 보관하지 않는다. 재기동 시 0에서 재구축이 전제 — 영속화하지 않는다. 외부에는 읽기용 `DemandQuery`·쓰기용 `DemandMutator` 역할 인터페이스로만 노출한다(단일 소유는 유지, ISP로 좁게 노출).
7. **경계는 포트로 뒤집는다(DIP)**: 도메인 클래스가 `LettuceConnectionFactory`·`SimpMessagingTemplate`·JWT 라이브러리를 직접 import하지 않는다. 새 인프라 의존이 생기면 포트부터 정의한다 — 그래야 인프라 없이 단위 테스트된다.
8. **운영 실패와 규모를 정상 조건으로 취급한다**: 모든 로직은 처리 중 프로세스 종료·부분 실패·타임아웃·중복·순서 역전·동시 실행·재기동·롤링 배포와 운영 데이터 규모를 검토한다. 재시도·멱등성·트랜잭션 경계·복구 방식·자원 상한·백프레셔·관측성·부하 검증은 [md/coding_convention.md](md/coding_convention.md) §5를 따른다. 단, 모든 경로에 전달 보장을 덧붙이지 않고 각 설계 문서의 전달 계약(best-effort 또는 at-least-once)을 우선한다.

## 빌드·실행

```bash
./gradlew build            # 전 모듈 빌드 + 테스트
./gradlew :ws:test         # 게이트웨이 테스트만
./gradlew :ws:bootRun --args='--spring.profiles.active=local'   # 로컬 실행 (Redis 필요: docker compose up -d)
# JWT 시크릿은 기본값 없음(fail-closed) — 로컬은 local 프로파일, 운영은 ALPHATALK_AUTH_JWT_SECRET 환경변수
```

> **에이전트 실행 허용**: `./gradlew` 테스트·빌드 명령(`build`, `:모듈:test`, `check`, `compileKotlin`, `assemble` 등)은 확인 없이 실행해도 된다. 변경한 코드는 관련 모듈 테스트로 검증하는 것을 기본으로 한다. `bootRun`·`docker compose` 같은 장기 실행·외부 부작용 명령은 예외로 사용자에게 먼저 확인한다.

로컬 인프라는 루트 `docker-compose.yml`(Redis·PostgreSQL). 통합 테스트는 Testcontainers가 자체 기동하므로 별도 준비 불요. 모니터링은 `docker compose --profile monitoring up -d`로 opt-in 기동 — Prometheus(127.0.0.1:9090)가 각 서버의 `/actuator/prometheus`를 스크레이프하고 Grafana(127.0.0.1:3000, admin/admin)에 데이터소스·대시보드가 프로비저닝된다(설정은 `infra/monitoring/`). `/actuator/prometheus`는 모든 서버가 local 프로파일에서만 노출한다(fail-closed — 운영은 보호 경계를 갖춘 뒤 환경변수로 opt-in).

**로컬 시크릿**은 저장소 루트 `secrets.yml`에 모은다 — `cp secrets.yml.example secrets.yml` 후 값을 채운다. 각 서버의 `application-local.yml`(local 프로파일)이 `spring.config.import`로 optional 로 읽는다 — local 프로파일을 켜지 않는 테스트·운영 기동에서는 읽히지 않고, 파일이 없으면 무시되고 환경변수 경로로 동작한다. import된 파일이 우선이라 `secrets.yml` 값이 `application-local.yml`의 같은 키를 덮어쓴다. `secrets.yml`은 `.gitignore` 대상이며 **절대 커밋하지 않는다**. 운영은 이 파일이 아니라 환경변수·시크릿 매니저로 주입한다. import를 기본 `application.yml`로 옮기지 않는다 — 테스트 JVM의 작업 디렉터리가 모듈 디렉터리라 개발자 로컬 시크릿이 테스트 컨텍스트에 스며든다.

## 컨벤션

- **코딩 컨벤션**: [md/coding_convention.md](md/coding_convention.md). 핵심 —
  - **코드에 주석을 달지 않는다**(self-documenting code). 설계 근거·불변식·포트 계약은 코드가 아니라 `md/` 설계 문서가 소유하고, 코드에서는 테스트로 고정한다.
  - **빈은 컴포넌트 스캔으로 등록한다**. 우리가 만든 클래스는 `@Service`/`@Repository`/`@Component`를 붙이고, `@Configuration`+`@Bean` 손조립은 판단이 필요할 때만 쓴다(프레임워크 타입, 조건부·fail-closed 등록). 포트 인터페이스에는 어노테이션을 붙이지 않는다.
  - **Spring Boot 3.5와 Kotlin의 현재 방식을 쓴다**. 생성자 주입·불변 객체·타입 안전 설정·얇은 컨트롤러·서비스 트랜잭션 경계를 기본으로 하고, deprecated API와 과거 Spring 관용구를 새 코드에 복사하지 않는다.
  - **관계형 DB는 JPA로 구현한다**. native/raw SQL과 `JdbcTemplate`/`JdbcClient`/직접 JDBC는 사전 합의 없는 우회 수단으로 사용하지 않는다.
  - **운영 실패와 규모를 정상 조건으로 설계한다**. 로직의 모든 외부 I/O·상태 전이에서 중단과 재실행 결과를 정하고, 중복·동시성·과부하·느린 의존성에 대한 안전장치와 이를 검증하는 테스트·메트릭을 함께 둔다.
- **브랜치·커밋·PR 규칙**: [md/git_convention.md](md/git_convention.md). 핵심 — 브랜치 `type/scope/desc`, 커밋 `type(scope): 제목`, **scope=모듈명**(ws·contracts·auth-jwt·core-api·worker-*). main 직접 커밋 금지.
- **AI 공동 저자 서명 금지**: 커밋 메시지·PR 본문에 `Co-Authored-By` 등 공동 저자(co-author) 트레일러를 **절대 넣지 않는다** — AI 도구(Claude·Codex 등) 서명 포함.
- 커밋·PR은 계획서의 단계(S0~S7) 단위. PR 본문에 해당 단계와 DoD 충족 여부를 적는다.
- 테스트 없는 refcount/인덱스 변경 금지 — `DemandRegistry` 전이 로직은 반드시 단위 테스트 동반.
- 로그는 구조화(JSON) 지향, `sessionId`/`userId` 태깅. 뜨거운 경로(틱 relay)에 debug 이상 로그 금지.
- 새 Redis 채널/키가 필요하면 코드보다 먼저 [md/redis_contract.md](md/redis_contract.md)에 합의 내용을 반영한다 (팀 공유 계약).

## 현재 상태 (2026-07 기준)

- **S0~S5 구현 완료**: `:contracts` + `:auth-jwt` + `:ws` — 인증(JWT CONNECT)·수요 인덱스(DemandRegistry)·Redis relay·관심목록 해소·watchlist:updated·프레즌스·메트릭·방 quote 토픽(WS 명세 v0.9 — 관심목록 없이 방만 열람해도 시세 수신). 테스트 77개(E2E 5개 포함) 통과.
- 버전: Spring Boot 3.5.16 · Kotlin 2.2.21 · JDK 21(toolchain 자동 다운로드) · jjwt 0.12.7.
- 남은 단계: **S6**(graceful shutdown 시나리오 검증, quote 샘플러 여부 판단) · **S7**(41종목×500세션 부하 스모크). 계획서 §5 참조.
- 미해결 합의 안건은 [md/ws_module_plan.md](md/ws_module_plan.md) §7 (RS256 전환 여부, 관심목록 조회 경로 등). 해당 코드는 포트로 격리된 구현(`:auth-jwt`의 `JwtTokenProvider` HS256, `RedisWatchlistResolver`)을 쓴다.
- **core-api 7모듈 구현 완료 (M1~M5)**: auth·search·subscription·stream·notification·community·stockinfo — REST 33개 엔드포인트 + 쓰기 레이트리밋·멱등키(명세 §1.5·§1.6). 알림은 fan-out-on-read(ADR A4)이며 증권사 투자의견 전역 알림 포함(명세 v0.4 §6.1), 글은 더블라이트(ADR A6), 주/월봉은 일봉 파생(ADR A8). 테스트 259개(동시성·E2E 포함) 통과. 남은 것: M6 경화(CORS 화이트리스트·메트릭·배포)와 springdoc OpenAPI 상호 검증(§0).
- **worker-price·worker-batch 수집 잡**: price는 실시간(세션풀·conflation·수요 리컨실)과 일봉·분봉(신선화 API·일 배치·퍼지) 구현. batch는 잡 카탈로그(KIS 워커 명세 §3.1) 중 `stock_master_sync`·`industry_sync`(+`dart_corp_map`)·`invest_opinion_sync`·`valuation_daily`·`investor_flow_daily`·`financials_sync` 구현 — KIS 잡(opinion·valuation·investor)과 financials 모두 기본 off, 켜면 각각 KIS 계정·DART 키 필수(fail-closed). `minute_candle_backfill`도 구현(§9-9 계측 종결, 조회 트리거 비동기 백필 — 명세 v0.8 §2.6).
- **뉴스 파이프라인 N0~N7 구현 완료**: `:worker-ingest`(RSS 수집→정규화→XADD + 다이제스트 트리거(종목 18:00·시장 17:40) — 텍스트 사전 매핑은 v0.6에서, 네이버 검색 API 소스는 2026-08-11에 제거, 종목 후보는 소스 부여만. 테스트 48개) + `:worker-llm`(소비→클러스터링(pgvector)→LLM 요약·감성→scope 사다리 fan-out→persist·publish·ack + 일일 브리핑·시장 다이제스트(3층 입력·웹 리서치)·DLQ, 테스트 115개). 운영 LLM·임베딩은 미구성 시 fail-closed한다. 로컬 LLM은 Claude CLI 구독이 기본이고 Codex CLI 구독을 선택할 수 있으며, fake LLM은 `provider=fake` 명시 opt-in이다(테스트는 fake 사용). 상세·잔여 설정은 [md/alphatalk_news_worker_spec.md](md/alphatalk_news_worker_spec.md) §9.
