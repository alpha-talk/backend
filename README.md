<div align="center">

# Alpha Talk

**종목 하나 = 방 하나.**
뉴스·공시·AI 요약·실시간 시세·커뮤니티 글을 종목별 시간순 스트림 하나로 모으는 주식 커뮤니티의 백엔드.

[![Kotlin](https://img.shields.io/badge/Kotlin-2.2-7F52FF?logo=kotlin&logoColor=white)](https://kotlinlang.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![JDK](https://img.shields.io/badge/JDK-21-437291?logo=openjdk&logoColor=white)](https://openjdk.org/projects/jdk/21/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%20%2B%20pgvector-4169E1?logo=postgresql&logoColor=white)](https://github.com/pgvector/pgvector)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)](https://redis.io)
[![tests](https://img.shields.io/badge/tests-306%20passing-brightgreen)](#개발)

한국어 · [English](README.en.md)

</div>

---

## 무엇을 푸는가

한 종목을 추적하려면 뉴스는 포털에서, 공시는 DART에서, 시세와 차트는 HTS에서, 잡담은 종토방에서 봐야 한다. 정보가 다섯 군데로 흩어져 있고, 종토방은 노이즈가 많고, 쏟아지는 뉴스를 다 읽기도 벅차다.

Alpha Talk는 **종목 코드 하나를 축으로** 이 조각들을 `StreamEvent`라는 단위로 정규화해 한 흐름으로 보여준다.

- **통합 스트림** — 방에 들어가면 뉴스·공시·리포트·AI 요약·유저 글이 시간순 한 줄기로 흐른다
- **기사 클러스터링** — 같은 사건을 다룬 여러 언론사 기사를 임베딩으로 묶어 클러스터당 한 건만 노출한다
- **AI 3줄 요약과 호재/악재 판정** — 감성은 (사건, 종목) 단위다. 같은 뉴스가 A사엔 호재, B사엔 악재일 수 있으니까
- **실시간 푸시** — 관심목록 종목의 시세 틱과 새 이벤트가 WebSocket으로 도착한다
- **일일 브리핑** — 매일 18시, 종목별 호재·악재를 하루치로 묶어 요약한다

## 어떻게 동작하나

런타임 특성이 다른 것만 떼어낸 **모듈러 모놀리스 + 워커** 구성이다. MSA가 아니다.

```
① 실시간 평면
   KIS WebSocket ─► worker-price ─► Redis Pub/Sub (quote:{code}) ─► ws ─► 브라우저(STOMP)

② 비동기 가공 평면
   RSS·뉴스검색 ─► worker-ingest ─► queue:ingest (Redis Streams)
                                      └► worker-llm ─► ①DB 저장 ②PUBLISH stream:{code} ③XACK

③ 동기 API 평면
   브라우저 ─(REST)─► core-api ─► PostgreSQL / Redis
                        └─ 글 작성: ①DB 저장 ②PUBLISH post:{code}

④ 배치 평면
   KIS REST · 마스터파일 · OpenDART ─► worker-batch ─► PostgreSQL
```

세 가지 규칙이 이 그림을 지탱한다.

1. **서버끼리 코드로 의존하지 않는다.** 통신은 Redis 채널·큐와 DB 계약으로만 한다. 빌드 그래프가 이 경계를 강제한다.
2. **DB가 진실이고 푸시는 best-effort다.** 순서는 항상 저장 → 발행 → ack. 놓친 이벤트는 재전송이 아니라 REST 조회로 복구한다.
3. **게이트웨이는 얇다.** 클라이언트가 보내는 SEND 프레임은 받지 않고, DB도 보지 않는다. 하는 일은 Redis 구독과 fan-out뿐이다.

## 빠른 시작

필요한 건 Docker와 Git뿐이다. JDK 21은 Gradle 툴체인이 알아서 받아온다.

```bash
git clone https://github.com/alpha-talk/backend.git
cd backend

docker compose up -d      # Redis 7 · PostgreSQL 16(pgvector)
./gradlew build           # 전 모듈 빌드 + 테스트 306개
```

게이트웨이만 띄워보려면:

```bash
./gradlew :ws:bootRun --args='--spring.profiles.active=local'
```

`ws://localhost:8081/ws`로 STOMP 연결이 열린다. JWT는 **CONNECT 프레임 헤더**로 보낸다 — URL 쿼리파라미터는 프록시 로그에 남으므로 받지 않는다.

RSS 기사가 요약을 거쳐 브라우저 카드로 뜨는 것까지 눈으로 확인하려면 [test-front/README.md](test-front/README.md)를 따라가면 된다.

## 저장소 구조

한 저장소에 여러 서버를 Gradle 서브모듈로 담는다. 모든 폴더가 서버는 아니다 — 독립 실행되는 앱과 공유 라이브러리를 구분한다.

| 모듈 | 종류 | 역할 | 포트 |
|---|---|---|---|
| `contracts` | 라이브러리 | 채널명·키·STOMP 목적지·봉투 DTO — 서비스 간 공유 문자열의 단일 소유자 | — |
| `auth-jwt` | 라이브러리 | JWT 발급·검증. 의존성과 시크릿 프로퍼티만 넣으면 빈이 등록된다 | — |
| `kis-client` | 라이브러리 | 한국투자증권 OpenAPI — 토큰 수명주기·유량 제어·REST/WS 클라이언트 | — |
| `db-migrations` | 라이브러리 | Liquibase changelog. DB 스키마의 단일 소유자 | — |
| `ws` | 서버 | STOMP 게이트웨이 — 푸시 전용 엣지 | 8081 |
| `worker-price` | 서버 | KIS 실시간 시세 수집·conflation·발행, 일봉 적재 | 8082 |
| `worker-batch` | 서버 | 종목마스터·수급·재무 배치 | 8083 |
| `worker-ingest` | 서버 | 뉴스 수집·정규화·큐 적재 | 8084 |
| `worker-llm` | 서버 | 큐 소비 → 클러스터링 → LLM 요약 → 저장·발행 | 8085 |
| `core-api` | 서버 *(예정)* | REST 메인서버 — 인증·검색·스트림 조회·커뮤니티 | 8080 |

## 기술 스택

| 영역 | 선택 |
|---|---|
| 언어 · 런타임 | Kotlin 2.2 / JVM 21 |
| 프레임워크 | Spring Boot 3.5 — MVC + `@EnableWebSocketMessageBroker`(SimpleBroker) |
| 데이터 | PostgreSQL 16 + pgvector(기사 클러스터링), Redis 7(Pub/Sub · Streams · 캐시) |
| 마이그레이션 | Liquibase — `db-migrations`가 changelog를 단독으로 들고 있다 |
| LLM · 임베딩 | Anthropic API 또는 Claude/Codex CLI · OpenAI 호환 임베딩 엔드포인트(로컬은 Ollama + BGE-M3) |
| 빌드 | Gradle Kotlin DSL 멀티모듈 + 버전 카탈로그 |
| 테스트 | JUnit 5 · Testcontainers(PostgreSQL·Redis) · `WebSocketStompClient` |

## 문서

설계 결정은 코드가 아니라 `md/`에 있다. 코드와 문서가 어긋나면 둘 중 하나를 고치지, 어긋난 채로 두지 않는다.

| 문서 | 다루는 것 |
|---|---|
| [기획안](md/기획안.md) | 요구사항·전체 아키텍처·로드맵·법적 검토 |
| [ws_architecture](md/ws_architecture.md) | 게이트웨이 코드 레벨 설계 — 컴포넌트 책임·동시성·에러 정책 |
| [ws_api_spec](md/ws_api_spec.md) | 클라이언트 ↔ 게이트웨이 STOMP 프로토콜 계약 |
| [redis_contract](md/redis_contract.md) | 게이트웨이 ↔ 워커 ↔ 메인서버 Redis 계약 — 서비스 간 단일 진실 |
| [alphatalk_core_api_spec](md/alphatalk_core_api_spec.md) | 클라이언트 ↔ 메인서버 REST 계약 |
| [alphatalk_kis_worker_spec](md/alphatalk_kis_worker_spec.md) | KIS·OpenDART 수집 워커 명세 |
| [alphatalk_news_worker_spec](md/alphatalk_news_worker_spec.md) | 뉴스 파이프라인 — 수집·클러스터링·요약·일일 브리핑 |
| [local_embedding_setup](md/local_embedding_setup.md) | 로컬 무료 임베딩(Ollama + BGE-M3) 실행 가이드 |
| [coding_convention](md/coding_convention.md) · [git_convention](md/git_convention.md) | 코딩·브랜치·커밋 규약 |

## 개발

```bash
./gradlew build              # 전 모듈 빌드 + 테스트
./gradlew :ws:test           # 게이트웨이만
./gradlew :worker-llm:test   # 뉴스 요약 워커만
```

통합 테스트는 Testcontainers가 Redis·PostgreSQL을 직접 띄우므로 따로 준비할 게 없다. 현재 306개가 모두 통과한다.

몇 가지 알아둘 것:

- **시크릿에 기본값은 없다.** JWT 시크릿·KIS 계정·LLM 키가 없으면 기동에 실패한다(fail-closed). 로컬 개발은 `local` 프로파일을 쓴다.
- **코드에 주석을 달지 않는다.** 설계 근거와 불변식은 `md/` 문서가 갖고, 코드에서는 테스트로 고정한다.
- **채널명·키를 문자열로 적지 않는다.** `:contracts`의 상수를 쓴다.
- 브랜치는 `type/scope/desc`, 커밋은 `type(scope): 제목`, scope는 모듈명이다. main 직접 커밋은 하지 않는다. 자세한 건 [git_convention](md/git_convention.md).

## 현재 상태

| 영역 | 상태 |
|---|---|
| 게이트웨이(`ws`) | JWT 인증·수요 인덱스·Redis relay·관심목록 해소·프레즌스·메트릭까지 동작 |
| 뉴스 파이프라인(`worker-ingest` · `worker-llm`) | 수집부터 클러스터링·요약·감성·fan-out·일일 브리핑·DLQ까지 동작 |
| KIS 실시간(`kis-client` · `worker-price`) | 세션 풀·conflation·발행·REST 폴링 강등·일봉 적재까지 동작 |
| 배치(`worker-batch`) | 종목마스터 적재 완료. 수급·재무·투자의견은 명세만 |
| 메인서버(`core-api`) | 명세 작성 완료, 구현 착수 전 |

다음 순서는 `core-api` 스캐폴딩, 워커 수요 연동, 부하 스모크다.

## 면책

포트폴리오 목적의 개인·팀 프로젝트다. 여기서 나오는 모든 정보와 AI 산출물은 참고용이며 투자 권유가 아니다.

실시간 시세를 일반에 재배포하려면 코스콤 정보시세 이용계약이 필요하고, AI로 가공한 데이터도 예외가 아니다. 공개 배포 시에는 20분 지연 모드로 전환할 수 있게 설계했다. 자세한 검토는 [기획안 §6](md/기획안.md)에 있다.
