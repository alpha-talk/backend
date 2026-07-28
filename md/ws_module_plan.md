# Alpha Talk — `ws` 모듈(게이트웨이) 구현 계획 v0.3

`ws` 게이트웨이를 어떤 순서로 어디까지 만들지 정하는 계획서다. **컴포넌트 구조·책임의 단일 진실은 [아키텍처 문서](ws_architecture.md)**이고, 이 문서는 단계(S0~S7)별 범위·완료 정의(DoD)와 팀 합의가 필요한 열린 안건(§7)을 소유한다. 각 단계에 착수하기 전에 그 단계의 범위와 DoD를 여기서 확인한다.

> 기준 문서: [기획안 v2](기획안.md) · [WS API 명세 v0.6](ws_api_spec.md) · [Redis 계약 v0.7](redis_contract.md) · [아키텍처 설계 v0.7](ws_architecture.md)
> 담당 범위: **클라 ↔ WS 게이트웨이** 경계. 문서상 명칭은 `gateway`, 본 저장소 모듈명은 `ws`로 한다.

> **v0.2 → v0.3 변경**: 컴포넌트 경계에 SOLID 반영(포트/인터페이스 도입). 클래스 이름·패키지 구조를 [아키텍처 v0.2](ws_architecture.md)와 일치시킴.
> **v0.1 → v0.2 변경**: 구현 스택을 **WebFlux(STOMP 자작) → Spring MVC + `@EnableWebSocketMessageBroker`** 로 전환. 클라이언트 노출 프로토콜(WS 명세)은 변경 없음.

---

## 1. 목표 & 범위

게이트웨이는 실시간 푸시 전용 엣지다. 클라에게 받는 것은 제어 프레임뿐이고 콘텐츠·상태의 진실은 전부 core-api와 워커에 있다.

### 1.1 이 모듈이 하는 것 (WS 명세 §1, §3, §9)

- STOMP 1.2 over WebSocket 수용 (`/ws` 엔드포인트)
- CONNECT 프레임 JWT 검증 → 세션 수립
- 서버 해소 구독: 유저 관심목록 N종목의 `quote`/`stream`을 `/user/queue/quote`, `/user/queue/stream`으로 relay
- 클라 동적 구독: `/topic/rooms/{code}/posts` (M1), `trade`/`depth` (확장)
- Redis Pub/Sub 업스트림 구독: `quote:{code}` · `stream:{code}` · `post:{code}` · `watchlist:updated`
- **채널당 1회 구독 + N세션 fan-out** (refcount 기반 구독/해지)
- 하트비트(10s/10s)·프레즌스(`presence:{userId}`)·graceful shutdown
- 느린 클라 보호(송신 버퍼/시간 제한), `quote` 방어적 conflation

### 1.2 하지 않는 것 (WS 명세 §5, §8)

- 콘텐츠 SEND 수신 — **SEND 프레임은 인터셉터에서 거부** (제어 프레임만 허용)
- DB 쓰기, Redis Streams(`queue:ingest`) 접근, `price:{code}` 캐시 읽기
- 과거 메시지 재전송(live-only), 틱 복구
- 로그인/토큰 발급, 관심목록 편집 — 전부 core-api REST 담당

---

## 2. 기술 스택 & 버전

핵심 선택은 **MVC 스택 + SimpleBroker**다. STOMP 파싱·하트비트·구독 수명 관리를 프레임워크에 맡기면 우리는 도메인 로직만 만들면 된다 — 경계는 §2.1에 정리했다.

| 영역 | 선택 | 비고 |
|---|---|---|
| 언어 | **Kotlin 2.x** (JVM 21 toolchain) | |
| 프레임워크 | **Spring Boot 3.5.x (3.x 최신)** + `spring-boot-starter-websocket` (MVC 스택, 내장 Tomcat) | `@EnableWebSocketMessageBroker` + SimpleBroker |
| 빌드 | Gradle Kotlin DSL + `libs.versions.toml` 버전 카탈로그 | 멀티모듈 모노레포 |
| Redis 클라이언트 | Spring Data Redis (Lettuce) | `RedisMessageListenerContainer`로 Pub/Sub 동적 구독 |
| JSON | Jackson (kotlin-module) | 봉투 DTO 직렬화 |
| JWT | jjwt 또는 Nimbus(spring-security-oauth2-jose) | core-api와 서명키/클레임 합의 필요 (§7-1) |
| 테스트 | JUnit5, **Testcontainers(Redis)**, `WebSocketStompClient`(테스트 클라이언트) | |

### 2.1 프레임워크가 대신 해주는 것 / 우리가 만드는 것

| 프레임워크 제공 (설정만) | 우리가 구현 |
|---|---|
| STOMP 프레임 파싱·인코딩·부분 프레임 버퍼링 | JWT 검증 `ChannelInterceptor` (CONNECT 처리, SEND 거부, 목적지 화이트리스트) |
| 하트비트 협상·송신·타임아웃 세션 정리 | 관심목록 해소 (`WatchlistResolver`) 및 유저별 라우팅 인덱스 |
| 구독 레지스트리 (SUBSCRIBE/UNSUBSCRIBE/DISCONNECT 수명 관리) | **수요 카운트** (code→세션) + Redis 채널 refcount 구독/해지 |
| `/user/queue/*` 목적지 해소 (`convertAndSendToUser`) | Redis 수신 → 브로커 발행 relay (`MessageRouter`) |
| MESSAGE fan-out, ERROR 프레임 | `watchlist:updated` 반영, 프레즌스, 메트릭, quote 방어적 conflation |

> SimpleBroker가 인메모리·단일 인스턴스라는 한계는 이 구조에서 문제가 안 된다. 게이트웨이 간 버스는 **Redis Pub/Sub이 이미 담당**하고(broadcast-and-filter), 각 인스턴스의 브로커는 자기 세션만 fan-out하면 된다. 외부 브로커(RabbitMQ relay)는 불필요.

---

## 3. 저장소/모듈 구조

기획안 §3.1 모노레포 구조를 따르되, 이번 단계에서는 필요한 것만 만든다.

```
backend/
├─ settings.gradle.kts
├─ build.gradle.kts                # 공통 컨벤션 (kotlin, jvm toolchain, ktlint 등)
├─ gradle/libs.versions.toml
├─ contracts/                      # 채널명 상수 · 봉투 DTO · eventId 규약 (순수 라이브러리, Spring 무의존)
└─ ws/                             # 게이트웨이 (Spring Boot 앱, MVC + WebSocket)
```

### 3.1 `:contracts` — 이번에 넣을 것 (Redis 계약 §7)

| 항목 | 형태 |
|---|---|
| 채널명 빌더 | `Channels.quote(code)` → `"quote:005930"`, `stream(code)`, `post(code)`, `trade(code)`, `depth(code)`, `WATCHLIST_UPDATED` |
| 공통 봉투 | `Envelope(type, code, eventId?, ts, data)` — `data`는 타입별 DTO (`QuoteData`, `StreamData`, `PostData`, …) |
| watchlist 이벤트 | `WatchlistUpdated(userId, added, removed, ts)` |
| 상태 키 빌더 | `Keys.presence(userId)`, `Keys.price(code)` 등 |

> 원칙: **채널명·키 문자열 하드코딩 금지** (기획안 §2.5-6). 게이트웨이 코드는 전부 `:contracts` 상수만 참조한다 — 채널명이 바뀌어도(§7-4 리네임 안건) 상수 한 곳만 고치면 된다.

### 3.2 `:ws` 패키지 구조

포트(인터페이스)는 도메인 패키지에 구현과 함께 둔다. 상세 책임·계약은 [아키텍처 §4·§9](ws_architecture.md).

```
ws/src/main/kotlin/com/alphatalk/ws/
├─ WsApplication.kt
├─ config/          # WebSocketConfig, RedisConfig, WsProperties, MetricsConfig
├─ auth/            # StompAuthChannelInterceptor, StompPrincipal (TokenVerifier 포트·구현은 :auth-jwt)
├─ subscription/    # DemandQuery(P)·DemandMutator(P)·DemandRegistry, SessionEventListener
├─ relay/           # RedisChannelHandler(P)·{Quote,Stream,Post,WatchlistUpdate}Handler,
│                   #   ChannelSubscriber(P)·RedisChannelSubscriber, MessageRouter
├─ client/          # ClientMessageSink(P)·BrokerMessageSink (브로커 아웃바운드 경계)
├─ watchlist/       # WatchlistResolver(P)·RedisWatchlistResolver (임시)
└─ presence/        # PresenceRegistry(P)·RedisPresenceService
```

> `(P)` = 포트. DIP 대상 6개(TokenVerifier·WatchlistResolver·ClientMessageSink·ChannelSubscriber·PresenceRegistry·RedisChannelHandler)와 ISP 역할 분리(DemandQuery/DemandMutator)의 근거는 [아키텍처 §3](ws_architecture.md).

---

## 4. 핵심 설계

### 4.1 데이터 흐름 (한 장 요약)

```
[인바운드 — 프레임워크가 STOMP 해석]
  CONNECT     → StompAuthChannelInterceptor: JWT 검증 → Principal(userId) 부여
  CONNECTED 후 → SessionConnectedEvent: 관심목록 해소 → 유저 인덱스 등록 → 수요 카운트 반영 → 프레즌스 등록
  SUBSCRIBE   → 인터셉터: 목적지 화이트리스트 검증
              → SessionSubscribeEvent: /topic/rooms/{code}/posts 면 수요 카운트 +1 → ChannelSubscriber 조정
  UNSUBSCRIBE/DISCONNECT/하트비트 타임아웃
              → SessionUnsubscribeEvent/SessionDisconnectEvent: 수요 회수, 프레즌스 정리
  SEND        → 인터셉터에서 즉시 거부 (푸시 전용 엣지)

[아웃바운드 — Redis 수신을 브로커로 발행]
  quote:{code}  → MessageRouter: code로 유저 인덱스 조회
                → convertAndSendToUser(userId, "/queue/quote", envelope)   (해당 유저 전 세션)
  stream:{code} → 동일하게 "/queue/stream"으로
  post:{code}   → convertAndSend("/topic/rooms/{code}/posts", envelope)    (브로커가 구독 세션에 fan-out)
```

### 4.2 브로커 설정 (WebSocketConfig)

```kotlin
@Configuration
@EnableWebSocketMessageBroker
class WebSocketConfig : WebSocketMessageBrokerConfigurer {
    override fun registerStompEndpoints(registry: StompEndpointRegistry) {
        registry.addEndpoint("/ws")            // SockJS 미사용 — 명세는 순수 WS
    }
    override fun configureMessageBroker(registry: MessageBrokerRegistry) {
        registry.enableSimpleBroker("/topic", "/queue")
            .setHeartbeatValue(longArrayOf(10_000, 10_000))   // 명세 §2.2
            .setTaskScheduler(heartbeatScheduler())            // 하트비트에 스케줄러 필수
        registry.setUserDestinationPrefix("/user")
    }
    override fun configureWebSocketTransport(registry: WebSocketTransportRegistration) {
        registry.setSendTimeLimit(10_000)          // 느린 클라 보호 (§4.7)
            .setSendBufferSizeLimit(512 * 1024)    // 초과 시 세션 강제 종료
    }
}
```

### 4.3 인증 (WS 명세 §2.1) — `StompAuthChannelInterceptor`

핸드셰이크(HTTP)에는 토큰이 없고 **STOMP CONNECT 헤더에 JWT**가 오므로, 인증은 `clientInboundChannel`의 `ChannelInterceptor.preSend`에서 처리한다 (MVC STOMP의 표준 패턴).

- `CONNECT`: `Authorization: Bearer …` 검증 → 성공 시 `accessor.user = Principal(userId)`. 실패 시 예외 → 프레임워크가 `ERROR` 프레임 후 종료.
- `SUBSCRIBE`: ① Principal 없으면 거부(CONNECTED 이전 구독 차단 — 명세 §2.1) ② 목적지 화이트리스트 검증: `/user/queue/quote|stream`, `/topic/rooms/{code}/posts` (+확장 `trade`/`depth`)만 허용.
- `SEND`: 무조건 거부.
- 토큰을 URL 쿼리파라미터로 받지 않는다. 토큰 원문 로그 금지 (NFR-07).
- 연결 중 토큰 만료: v1은 연결 유지 (재연결 시 재검증). §7-3.

### 4.4 라우팅 인덱스 & 수요 카운트 (DemandRegistry)

브로커가 세션↔목적지 레지스트리를 관리해주지만, **"어느 code를 누가 필요로 하나"는 우리 도메인**이라 별도 인덱스를 유지한다. 이 인덱스가 곧 Redis 구독의 refcount다.

| 인덱스 | 용도 | 갱신 시점 |
|---|---|---|
| `code → Set<userId>` (watchlist용) | `quote:`/`stream:` 수신 시 대상 유저 조회 + refcount | 접속/종료, `watchlist:updated` |
| `code → Set<sessionId>` (방 입장용) | `post:` 채널 refcount (fan-out은 브로커가 함) | `/topic/rooms/{code}/posts` SUBSCRIBE/UNSUBSCRIBE |
| `userId → Set<sessionId>` | 멀티디바이스, 유저 퇴장 판정(세션 0개 시 watchlist 수요 회수) | 접속/종료 |

- 집합 전이 0→1: Redis SUBSCRIBE, 1→0: UNSUBSCRIBE. 전이 판정은 인덱스 락 안에서 직렬화(경쟁 조건 방지).
- 게이트웨이 재기동 시 세션이 전부 사라지므로 수요는 0에서 재구축 — 리컨실 불필요 (클라 재연결이 곧 재해소).

### 4.5 관심목록 해소 & `watchlist:updated`

- `SessionConnectedEvent`에서 `WatchlistResolver.resolve(userId): Set<code>` 호출 → 유저 인덱스 등록.
- **문제**: 관심목록의 진실 소스(DB)는 core-api 몫인데 core-api가 아직 없다. 그래서 인터페이스로 격리하고 단계적으로 구현한다:
  1. *지금*: `RedisWatchlistResolver` — `watchlist:{userId}` Set을 읽는 임시 구현 (개발·테스트용 시드 가능)
  2. *core-api 생기면*: REST 호출 또는 공유 DB 읽기로 교체 — 팀 합의 필요 (§7-2)
- `watchlist:updated` (전역 단일 채널, Redis 계약 §1.1): 상시 구독. `{userId, added, removed}` 수신 시 **자기 인스턴스에 그 유저 세션이 있으면** 인덱스 갱신 + refcount 증감 (broadcast-and-filter). 세션 없으면 무시.

### 4.6 Redis 구독 관리 (ChannelSubscriber + MessageRouter)

- `ChannelSubscriber`(포트) 구현이 `RedisMessageListenerContainer` 하나로 채널 동적 `addMessageListener`/`removeMessageListener`. `DemandRegistry`는 이 포트만 호출하고 Redis를 모른다(DIP).
- 상시 구독: `watchlist:updated` 1개. 동적 구독: 수요 있는 code의 `quote:`/`stream:`/`post:`만.
- 수신 콜백(`MessageRouter`)은 컨테이너 스레드에서 돈다 — 채널을 파싱해 종류별 `RedisChannelHandler`에 위임하고 핸들러가 `ClientMessageSink`로 발행한다. 여기가 병목 지점이라 무거운 작업을 금지한다. 파싱 실패는 카운터만 올리고 드랍한다.
- 구조·포트 근거: [아키텍처 §4.4·§3.5](ws_architecture.md).

### 4.7 느린 클라 보호 & quote conflation

- 1차 conflation(100~250ms)은 **price-worker 책임** (기획안 FR-07). 게이트웨이는 방어만:
  - `setSendTimeLimit`/`setSendBufferSizeLimit` — 송신이 막힌 세션은 프레임워크가 버퍼 초과 시 강제 종료. `stream`/`post`는 유실돼도 REST 복구 경로가 있어 안전 (명세 §6 best-effort).
  - MVC 스택은 Reactor 같은 세밀한 백프레셔 연산자가 없으므로, 필요 시 `quote`만 게이트웨이 자체 샘플러(종목별 최신값을 모아 250ms 주기 일괄 발행)를 **옵션으로** 추가. 스모크 테스트(S7) 결과를 보고 결정 — 조기 최적화 금지.

### 4.8 프레즌스 & graceful shutdown

- 접속 시 `SADD presence:{userId} {sessionId}` + TTL, 종료 시 SREM (Redis 계약 §3).
- 하트비트를 직접 볼 수 없으므로 TTL 갱신은 **주기 작업**: 스케줄러가 유저 인덱스의 활성 세션 기준으로 TTL 연장. 하트비트 타임아웃으로 죽은 세션은 `SessionDisconnectEvent`가 발생해 자연 정리.
- shutdown 훅: 새 연결 거부 → 전 세션 close → Redis 구독 해지. 클라는 백오프 재연결 + REST 복구 (명세 §7).

### 4.9 관측성 (기획안 §5.3 중 게이트웨이 몫)

Micrometer: `ws_connected_clients` · `ws_sessions_per_user` · `redis_subscribed_channels` · `relay_out_rate` · `slow_client_disconnects` · `stomp_errors`. 구조화 JSON 로그(sessionId, userId 태깅). 스레드 풀(`clientOutboundChannel`) 큐 깊이 모니터링 — MVC STOMP의 병목 신호다.

---

## 5. 구현 순서 (단계별 DoD)

순서는 앞 단계 산출물이 다음 단계의 전제가 되게 잡았다. 인증 인터셉터(S2)는 S1이 세운 브로커의 인바운드 채널에 끼운다. refcount 구독(S4)은 S3이 만든 수요 인덱스를 그대로 쓴다 — 인덱스가 곧 refcount이기 때문이다(§4.4). 관심목록 라우팅(S5)은 S4의 relay 위에 얹는다. S6~S7은 기능 추가가 아니라 운영 품질 단계다. quote 샘플러처럼 측정이 필요한 결정은 S7 스모크 결과를 보고 내린다(§4.7).

| 단계 | 내용 | 완료 정의 |
|---|---|---|
| **S0** 골격 | 모노레포 Gradle 세팅, `:contracts` + `:ws` 빈 앱, docker-compose(Redis), CI 스켈레톤 | `./gradlew build` 전 모듈 통과 |
| **S1** 브로커 기동 | `WebSocketConfig` — 엔드포인트/SimpleBroker/하트비트/전송 제한 | STOMP 클라로 접속·하트비트 유지, `/topic` 에코 확인 |
| **S2** 인증 | `StompAuthChannelInterceptor` — CONNECT JWT, SUBSCRIBE 화이트리스트, SEND 거부 | 유효/만료/변조 토큰, 미인증 구독, SEND 케이스 테스트 |
| **S3** 수요 인덱스 | `DemandRegistry` + STOMP 이벤트 리스너, 종료 시 전량 회수 | 구독→인덱스 반영, 멀티세션, 정리 테스트 |
| **S4** Redis relay | refcount 구독, `post:{code}` → `/topic/rooms/{code}/posts` E2E | Testcontainers: PUBLISH → STOMP MESSAGE 수신, 채널 1회 구독 검증 |
| **S5** 관심목록 | WatchlistResolver(임시 Redis 구현), `/user/queue/quote·stream` 라우팅, `watchlist:updated` 반영 | 구독 변경이 재접속 없이 반영 (FR-03 수용 기준) |
| **S6** 경화 | 프레즌스, graceful shutdown, 메트릭, (필요시) quote 샘플러 | 느린 클라·재기동 시나리오 테스트 |
| **S7** 통합 검증 | 가짜 price-worker 스크립트로 41종목 틱 fan-out 스모크 | p95 지연 측정, 동접 수백 세션 스모크, outbound 큐 깊이 확인 |

각 단계는 독립 PR 단위다. S4를 전반부에 둔 이유이기도 하다 — S4까지 가면 팀원(워커) 없이도 `redis-cli PUBLISH`만으로 데모할 수 있다.

---

## 6. 테스트 전략 (기획안 §4 중 게이트웨이 몫)

기본은 단위 계층이다 — 포트에 페이크를 꽂으면 인프라 없이 도메인을 검증할 수 있다. 실제 Redis·브로커가 붙는 검증만 Testcontainers 통합 계층에 맡긴다.

| 계층 | 대상 | 방법 |
|---|---|---|
| 단위 | **DemandRegistry refcount 전이** — `ChannelSubscriber` 포트를 페이크로 주입해 Redis 없이 검증(DIP의 배당금), 인터셉터(인증·화이트리스트·SEND 거부, `TokenVerifier` 페이크), relay 핸들러(`ClientMessageSink` 페이크) | JUnit5 + 수동 페이크/mock |
| 포트 계약(LSP) | `WatchlistResolver`(미존재 유저 빈 Set)·`TokenVerifier`(유효→userId/무효→예외) 등 포트별 계약 테스트를 **구현 공용**으로 작성 → 임시·실제 구현이 같은 테스트 통과 | 추상 계약 테스트(구현마다 상속) |
| 계약(:contracts) | 봉투 직렬화 왕복, `Channels` 생성·파싱 라운드트립 | `:contracts` 테스트 (워커/메인서버와 공용) |
| 통합 | PUBLISH→MESSAGE 전달, `/user` 목적지 멀티세션 전달, watchlist:updated 반영, 세션 정리 | Testcontainers(Redis) + `WebSocketStompClient` |
| 성능 스모크 | 41종목 × 500세션 fan-out | 부하 스크립트 (k6 ws 또는 자작 Kotlin 클라) |

---

## 7. 결정 필요 / 열린 질문 (팀 합의 안건)

결정 전까지는 아래 표의 "현재 가정"대로 구현한다. 각 항목은 포트나 `:contracts` 상수 뒤에 격리돼 있어 결정이 바뀌어도 교체 범위가 구현 하나로 좁혀진다.

| # | 항목 | 현재 가정 | 결정 주체 |
|---|---|---|---|
| 1 | JWT 서명 방식·클레임 스키마 (core-api와 공유) | HS256 공유 시크릿, `sub`=userId — **구현은 `:auth-jwt` 공유 라이브러리로 단일화 완료** (스키마 변경 시 라이브러리만 수정). 남은 결정: RS256 전환 여부·추가 클레임 | 민균(core-api) ↔ 게이트웨이 |
| 2 | 관심목록 조회 경로 (REST vs 공유 DB vs Redis 미러) | 임시로 Redis Set `watchlist:{userId}` | 공동 |
| 3 | 연결 중 액세스 토큰 만료 시 강제 종료 여부 | v1은 유지 | 공동 |
| 4 | `stream:{code}` → `feed:{code}` 리네임 | 보류, `:contracts` 상수 참조로 방어 (기획안 §2.5-6) | 공동 |
| 5 | 모듈명 `ws` vs 문서상 `gateway` | 저장소는 `ws` 사용, 문서 언급 시 병기 | 공동 |

---

*다음 액션: 이 계획 승인 후 S0(모노레포 골격 + `:contracts` + `:ws` 빈 앱) 착수.*
