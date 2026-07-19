# Alpha Talk — `ws` 모듈 아키텍처 설계 v0.5

> 상위 문서: [구현 계획 v0.2](ws_module_plan.md) · [WS API 명세 v0.4](ws_api_spec.md) · [Redis 계약 v0.1](redis_contract.md)
> 이 문서는 **코드 레벨 설계 기준**이다. "무엇을/왜"는 계획서가, "어떤 구조로"는 이 문서가 답한다.

> **v0.4 → v0.5**: 관심목록 해소를 **세션의 첫 SUBSCRIBE**로 단순화(§11.6 최종) — 프리페치 인터셉터·스태시 삭제, 같은 스레드 배치를 더 적은 구조로 달성. 미구독 세션의 헛수요도 제거.
> **v0.3 → v0.4**: §11.6 적용 — 관심목록 해소를 `WatchlistPrefetchInterceptor`(CONNECT preSend, WS 전송 스레드)로 이동해 outbound 풀 블로킹 제거. §5.4·§6.1을 실측 스레드 기준으로 갱신.
> **v0.2 → v0.3**: §11 설계 문답 추가 — 구현 리뷰에서 나온 결정들(관심목록 수요의 스위치, UNSUBSCRIBE 비대칭, 락 vs CHM, DemandRegistry 단일 클래스 유지, 제어/데이터 평면, onConnected 블로킹[열림])을 근거와 함께 기록. §5.4 스레딩 모델을 실측 기준으로 정정(SessionConnectedEvent는 inbound가 아니라 **outbound 풀**에서 발화).
> **v0.1 → v0.2**: SOLID를 컴포넌트 경계에 반영. 구체 클래스 직접 의존을 **포트(인터페이스)** 로 뒤집고(DIP), `DemandRegistry`/`MessageRouter`의 과다 책임을 분리(SRP)하고, 채널 종류별 relay를 전략으로 열었다(OCP). 클라 노출 프로토콜·불변 규칙은 그대로. 상세는 §3.

---

## 1. 설계 원칙

1. **얇은 엣지** — 게이트웨이는 인증·구독 관리·relay만 한다. DB 무접촉, Redis는 Pub/Sub SUBSCRIBE + 프레즌스만.
2. **프레임워크에 맡길 수 있는 것은 맡긴다** — STOMP 프레이밍·하트비트·구독 레지스트리·`/user` 해소·fan-out은 SimpleBroker의 일. 우리는 그 위의 도메인 로직(수요 카운트, relay)만 소유한다.
3. **모든 공유 문자열은 `:contracts`에서** — 채널명·목적지 패턴·봉투 DTO를 하드코딩하지 않는다.
4. **베스트 에포트 푸시** — 전달 실패는 에러가 아니다. 유실은 클라의 REST 복구가 메운다. 게이트웨이는 절대 재전송·버퍼링으로 신뢰성을 흉내 내지 않는다.
5. **세션이 유일한 상태** — 게이트웨이의 모든 인메모리 상태는 "현재 연결된 세션"에서 파생된다. 재기동하면 0에서 재구축되며, 복구할 영속 상태가 없다.
6. **경계는 포트로, 도메인은 인프라를 모른다** — Redis·브로커·JWT 라이브러리 같은 인프라는 전부 우리가 정의한 인터페이스(포트) 뒤에 둔다. 도메인 로직(수요 전이, relay 라우팅)은 Lettuce도 `SimpMessagingTemplate`도 모른 채 테스트된다. (SOLID 적용 — §3)

---

## 2. 컴포넌트 구성도

`(P)` = 포트(인터페이스), `→` = 의존 방향. 도메인은 항상 포트에 의존하고, 인프라 어댑터가 포트를 구현한다.

```
                        ┌────────────────────────────────────────────────────────┐
 클라 (STOMP)           │                        :ws 모듈                          │
   │                    │                                                        │
   ▼                    │  [Spring STOMP 계층 — 프레임워크 제공]                  │
 /ws 엔드포인트 ────────┼─► StompSubProtocolHandler (프레이밍·하트비트·fan-out)   │
                        │        │ clientInboundChannel                          │
                        │        ▼                                               │
                        │  StompAuthChannelInterceptor ──► TokenVerifier (P)      │
                        │        │              (:auth-jwt 소유) ▲ JwtTokenProvider│
                        │        ▼                                               │
                        │  SimpleBroker ◄──────────── ClientMessageSink (P)       │
                        │                                    ▲ BrokerMessageSink  │
                        │  [도메인 계층]                     │                    │
                        │  SessionEventListener              │                    │
                        │    ├─► WatchlistResolver (P) ◄ RedisWatchlistResolver   │
                        │    ├─► PresenceRegistry (P)  ◄ RedisPresenceService      │
                        │    └─► DemandMutator (P) ┐                              │
                        │                          ▼                              │
                        │                  DemandRegistry ──► ChannelSubscriber(P) │
                        │                    ▲ DemandQuery (P)     ▲               │
                        │                    │                RedisChannelSubscriber
                        │  RedisChannelHandler(P) 들 ─────────────┤ (listener 등록)│
                        │   {Quote,Stream,Post,WatchlistUpdate}   │               │
                        │        ▲                                ▼               │
                        │  MessageRouter ◄──────────────── Redis Pub/Sub (수신)   │
                        └────────────────────────────────────────────────────────┘
```

핵심: `DemandRegistry`는 `ChannelSubscriber` **포트**만 호출한다 — Redis를 모른다. relay 핸들러는 `ClientMessageSink`/`DemandQuery` **포트**만 안다 — `SimpMessagingTemplate`을 모른다.

---

## 3. SOLID 적용

이 모듈에서 각 원칙이 **어디에, 왜** 적용됐는지. 원칙을 위한 원칙이 아니라 실제 이음새(교체점·테스트 seam·확장점)가 있는 곳에만 건다.

### 3.1 SRP — 한 클래스, 한 변경 이유

| 분리 전 (v0.1) | 문제 | 분리 후 (v0.2) |
|---|---|---|
| `DemandRegistry`가 상태 소유 + 전이 판정 + **Redis 구독 호출** | Redis 방식이 바뀌면 도메인 클래스가 바뀜 | 상태·전이는 `DemandRegistry`, 구독 실행은 `ChannelSubscriber` 구현으로 분리 |
| `MessageRouter`가 채널 파싱 + 역직렬화 + **종류별 분기 + 발행** | 채널 종류 추가마다 라우터 수정 | 라우터는 "채널 → 핸들러 위임"만. 종류별 파싱·발행은 각 `RedisChannelHandler` |
| `Channels`가 이름 생성만 | 파싱 로직이 라우터로 샘 | `Channels`가 생성·**파싱**을 함께 소유(채널명 문법의 단일 지점) |

> 불변 규칙 6("인메모리 상태는 DemandRegistry 단일 소유")과 충돌하지 않는다. 상태의 **소유**는 여전히 `DemandRegistry` 하나다. 분리한 것은 상태가 아니라 *구독을 실행하는 부수효과*와 *채널별 발행 로직*이다.

### 3.2 OCP — 확장에 열리고 수정에 닫힘

- **채널 종류 확장**: `trade`/`depth`(명세 확장분)를 추가할 때 `MessageRouter`를 수정하지 않는다. `RedisChannelHandler`를 구현한 `TradeRelayHandler`를 `@Component`로 추가하면 종류별 핸들러 맵에 자동 등록된다.
- **인증 방식 교체**: HS256 → RS256/Nimbus 전환 시 `TokenVerifier` 구현만 교체(§7 합의 안건 대응).
- **관심목록 소스 교체**: 임시 Redis → core-api REST/DB 전환 시 `WatchlistResolver` 구현만 교체.

### 3.3 LSP — 구현은 계약을 배신하지 않음

포트마다 구현이 바뀌어도 성립해야 하는 계약을 문서화한다(§4.5). 예: `WatchlistResolver.resolve`는 **미존재 유저에 빈 Set 반환(널·예외 아님)**, `TokenVerifier.verify`는 **유효 시 userId, 무효 시 정해진 예외**. 임시 구현과 실제 구현이 이 계약을 공유하므로 서로 대체 가능하다.

### 3.4 ISP — 클라이언트별로 좁은 인터페이스

- `DemandRegistry`를 **두 역할 인터페이스**로 노출한다:
  - `DemandQuery` (읽기 전용, `usersWatching(code)`) — 뜨거운 relay 경로의 핸들러가 의존. 실수로 상태를 못 바꾼다.
  - `DemandMutator` (쓰기, `registerUser`/`enterRoom`/…) — 세션 수명 이벤트 처리기가 의존.
- `ClientMessageSink`는 `sendToUser(userId, type, envelope)`·`sendToRoom(code, envelope)` **둘만** 노출한다. `SimpMessagingTemplate`의 광범위한 API를 도메인에 흘리지 않고, STOMP 목적지 문자열 조립 지식(→ `Destinations`)을 한 곳에 가둔다.
- `ChannelSubscriber`는 `subscribe(channel)`·`unsubscribe(channel)` **둘만**. `DemandRegistry`는 Redis 리스너 컨테이너의 나머지를 볼 필요가 없다.

### 3.5 DIP — 도메인이 추상에, 인프라가 추상을 구현

의존 방향을 뒤집는 포트 목록. 왼쪽(도메인)은 오른쪽(인프라 구현)을 **컴파일타임에 모른다**.

| 포트 (도메인이 의존) | 구현 (인프라) | 뒤집는 이유 / 테스트 seam |
|---|---|---|
| `TokenVerifier` (**:auth-jwt** 소유) | `JwtTokenProvider` (:auth-jwt) | core-api와 발급·검증 공유 → 클레임 스키마 불일치 원천 차단. 인터셉터는 가짜 검증기로 단위 테스트 |
| `WatchlistResolver` | `RedisWatchlistResolver` (임시) | 조회 경로 미합의(§7). core-api 없이도 시드로 테스트 |
| `ClientMessageSink` | `BrokerMessageSink` (`SimpMessagingTemplate` 래핑) | 브로커 없이 relay 핸들러 검증. 목적지 문자열 캡슐화 |
| `ChannelSubscriber` | `RedisChannelSubscriber` (리스너 컨테이너 래핑) | **Redis 없이 `DemandRegistry` 전이 로직 단위 테스트** — 이 프로젝트에서 가장 값진 seam |
| `PresenceRegistry` | `RedisPresenceService` | 세션 수명 테스트에서 프레즌스 부수효과 스텁 |
| `RedisChannelHandler` | `Quote/Stream/Post/WatchlistUpdate…Handler` | 종류별 발행을 각자 테스트·확장(OCP와 짝) |

### 3.6 일부러 추상화하지 않는 곳 (과설계 경계)

SOLID는 이음새가 있는 곳의 도구다. 이음새가 없는데 인터페이스를 만들면 간접호출만 늘고 읽기 어려워진다. 다음은 **구체 클래스로 둔다**:

- `SessionEventListener` — Spring 이벤트를 포트 호출로 번역 + **세션 수명 정책**(첫 구독 시 해소·등록, 해소 실패 시 빈 목록) 소유. 이 정책을 `DemandRegistry`에 넣지 않는 이유: resolve는 I/O(≤1s)라 레지스트리 락 규율(락 안은 빠른 전이+ChannelSubscriber만)과 양립 불가하고, "어디서 가져오나"는 인덱스 불변식이 아니라 수명 정책이다(§11.4의 상태+불변식 응집 원칙). 구현이 하나뿐이고 교체점이 없어 인터페이스 없음.
- `DemandRegistry` 자체 — 인덱스 자료구조는 이 클래스에 응집돼 있어야 한다. 저장소를 인터페이스로 빼면 "단일 소유" 불변식이 약해진다.
- `WsProperties`·`WebSocketConfig`·`MetricsConfig` 등 설정 — 프레임워크 결선 지점.
- 봉투 DTO(`Envelope` 등) — 순수 데이터. 인터페이스로 감싸지 않는다.

---

## 4. 컴포넌트 책임 (포트 + 구현)

### 4.1 Spring STOMP 설정 계층 (config/)

| 클래스 | 책임 |
|---|---|
| `WebSocketConfig` | `/ws` 엔드포인트(SockJS 미사용), SimpleBroker(`/topic`,`/queue`) + 하트비트 10s/10s + 전용 TaskScheduler, `/user` prefix, 전송 제한(`sendTimeLimit` 10s / `sendBufferSizeLimit` 512KB), 인바운드 인터셉터 등록 |
| `RedisConfig` | `LettuceConnectionFactory`, `RedisMessageListenerContainer`(전용 스레드풀), 봉투 역직렬화용 `ObjectMapper` |
| `MessageRouter` (relay/) | `List<RedisChannelHandler>` 주입 → `kind → handler` 맵 결선(OCP 결선점)은 라우터 생성자가 담당 |
| `RelayBootstrap` (relay/) | 기동 시 `watchlist:updated` 상시 구독 등록 (ApplicationReadyEvent) |
| `WsProperties` | `@ConfigurationProperties(prefix="ws")` — JWT 시크릿, 전송 제한값, 프레즌스 TTL, 피처 플래그 외부화 |

### 4.2 인증 계층 (auth/)

| 타입 | 종류 | 책임 |
|---|---|---|
| `TokenVerifier` | **포트 (:auth-jwt)** | `verify(token): Long(userId)`. 무효 시 `InvalidTokenException`. 발급(`TokenIssuer`)·검증을 **공유 라이브러리 :auth-jwt가 소유** — ws는 검증만 결선(ISP), 발급은 core-api 몫 |
| `JwtTokenProvider` | 구현 (:auth-jwt) | HS256 서명·만료(exp 필수)·클레임(sub=userId) 검증. **토큰 원문 로그 금지**. 결선은 :auth-jwt의 **Boot 자동구성**(`JwtAuthAutoConfiguration`) — ws에 결선 코드 없음, `alphatalk.auth.jwt.secret` 프로퍼티만 |
| `StompAuthChannelInterceptor` | 구현 | `preSend` 분기: CONNECT→검증 후 `accessor.user=StompPrincipal(userId)` / SUBSCRIBE→Principal 필수 + 목적지 화이트리스트 / SEND→무조건 거부 |
| `StompPrincipal` | 구현 | `Principal.name = userId` (user-destination 해소 키) |

허용 목적지 (그 외 SUBSCRIBE 거부):

```
/user/queue/quote · /user/queue/stream
/topic/rooms/{6자리 code}/posts
/topic/rooms/{6자리 code}/trade · /depth   (피처 플래그 trade-depth-enabled로 차단 가능)
```

### 4.3 세션 수명 & 수요 계층 (subscription/)

| 타입 | 종류 | 책임 |
|---|---|---|
| `DemandQuery` | **포트(읽기)** | `usersWatching(code): Set<userId>`(불변 스냅샷) 등. relay 핸들러가 의존 |
| `DemandMutator` | **포트(쓰기)** | `registerUser`/`removeSession`/`enterRoom`/`leaveRoom`/`applyWatchlistDiff`. 세션 리스너가 의존 |
| `DemandRegistry` | 구현(둘 다) | §5 인덱스 단일 소유. 수요 전이(0↔1) 감지 시 `ChannelSubscriber` 호출. **상태 변경은 락 안에서 직렬화** |
| `SessionEventListener` | 구현 | Spring 이벤트 4종 → 포트 호출 번역 + 세션 수명 정책(첫 SUBSCRIBE에서 resolve→registerSession, §11.6). resolve는 락 밖(I/O), 등록은 멱등 |

### 4.4 relay 계층 (relay/ · client/)

| 타입 | 종류 | 책임 |
|---|---|---|
| `MessageRouter` | 구현 | Redis 수신 진입점. 채널명 파싱(`Channels.parse`) → `kind`로 핸들러 선택 → 위임. 이게 전부(SRP) |
| `RedisChannelHandler` | **포트** | `kind: ChannelKind` + `handle(code, body)`. 종류별 파싱·발행을 각자 소유 |
| `QuoteRelayHandler`/`StreamRelayHandler` | 구현 | 봉투 역직렬화 → `demandQuery.usersWatching(code)` 순회 → `sink.sendToUser(...)` |
| `PostRelayHandler` | 구현 | 봉투 역직렬화 → `sink.sendToRoom(code, envelope)` |
| `WatchlistUpdateHandler` | 구현 | `WatchlistUpdated` 역직렬화 → `demandMutator.applyWatchlistDiff(...)` (자기 인스턴스에 세션 있을 때만) |
| `ChannelSubscriber` | **포트** | `subscribe/unsubscribe(channel)` 둘만. `DemandRegistry`가 의존 |
| `RedisChannelSubscriber` | 구현 | `RedisMessageListenerContainer`에 리스너 add/remove. 콜백은 `MessageRouter`로 |
| `ClientMessageSink` | **포트** | `sendToUser(userId, type, envelope)`·`sendToRoom(code, envelope)` |
| `BrokerMessageSink` | 구현(client/) | `SimpMessagingTemplate` 래핑. 목적지 문자열은 `Destinations`로만 조립 |

### 4.5 지원 계층 & 포트 계약 (LSP)

| 타입 | 종류 | 계약 (구현이 반드시 지킬 것) |
|---|---|---|
| `WatchlistResolver` | **포트** | `resolve(userId): Set<code>`. 미존재 유저 → **빈 Set**. 널·예외 금지. 블로킹 ≤ 1s |
| `RedisWatchlistResolver` | 구현(임시) | `SMEMBERS watchlist:{userId}`. core-api 생기면 교체 |
| `PresenceRegistry` | **포트** | `add(userId, sessionId)`/`remove(...)`/`refresh(...)`. 멱등 |
| `RedisPresenceService` | 구현 | `SADD/SREM presence:{userId}` + TTL. 주기 스케줄러가 활성 세션 TTL 연장 |
| `WsMetrics` | 구현 | Micrometer 게이지/카운터 (계획서 §4.9) |

---

## 5. 상태 모델 & 동시성

### 5.1 DemandRegistry가 소유하는 인덱스

```kotlin
// 전부 DemandRegistry 내부에서만 변경. 외부에는 DemandQuery(읽기 스냅샷)로만 노출.
sessions:        Map<sessionId, SessionInfo(userId, watchlist: Set<code>)>
userSessions:    Map<userId, Set<sessionId>>          // 멀티디바이스
watchlistIndex:  Map<code, Set<userId>>               // quote:/stream: 라우팅 + refcount
roomIndex:       Map<code, Set<sessionId>>            // post: refcount (fan-out은 브로커가)
```

### 5.2 수요 전이 규칙 (DemandMutator 안에서)

| 이벤트 | 전이 | 부수효과 (포트 호출) |
|---|---|---|
| 유저 첫 세션 접속 | watchlist 각 code에 userId 추가 | 새 code면 `ChannelSubscriber.subscribe(quote/stream:code)` |
| 유저 마지막 세션 종료 | 각 code에서 userId 제거 | 집합이 비면 `unsubscribe` |
| `watchlist:updated` diff | 접속 중 유저만 적용 | 같은 전이 규칙 |
| 방 SUBSCRIBE / UNSUBSCRIBE·종료 | roomIndex 갱신 | 0↔1 전이 시 `subscribe/unsubscribe(post:code)` |

### 5.3 동시성 전략

- 세션 이벤트 스레드와 `watchlist:updated`(Redis 리스너 스레드)가 동시에 인덱스를 만질 수 있다.
- **v1은 단순하게**: `DemandRegistry`의 변경 메서드를 단일 락(`ReentrantLock`)으로 직렬화한다. `ChannelSubscriber` 호출도 **락 안에서 동기로** 일어난다(구독 해지 직후 재구독 유실 같은 레이스 차단). 변경 빈도는 초당 수십 건이라 경합이 문제되지 않는다.
- 읽기(`DemandQuery.usersWatching` — 틱마다 호출)는 **락 없이** 읽는다. `watchlistIndex`를 `ConcurrentHashMap<code, 불변 Set>`으로 두고, 변경 시 Set을 통째로 교체(copy-on-write)한다.
- 근거: 정합성 버그(refcount 꼬임 → 유령/유실 구독)가 성능보다 훨씬 비싸다. 락 최적화는 S7 스모크에서 병목으로 확인될 때만.

### 5.4 스레딩 모델

| 스레드풀 | 소속 | 하는 일 | 주의 |
|---|---|---|---|
| WS 전송(Tomcat) 워커 | 컨테이너 | 프레임 수신 + 인바운드 인터셉터 preSend(JWT 검증) + **`SessionSubscribeEvent` 리스너 = watchlist 해소·등록**(실측 `o-auto-N-exec-*`) + `SessionDisconnectEvent` 리스너(실측) | 인터셉터·구독 이벤트는 **호출 스레드에서 실행**된다. 프레임 단위로 빌리는 **공유 워커 풀(기본 200)** — outbound(코어×2)보다 10배 커서 접속 폭풍의 블로킹이 틱 전달과 격리된다. Redis 장애 대비 커맨드 타임아웃 1s 상한 (§11.6) |
| `clientInboundChannel` | Spring | 브로커 앞단 프레임 처리 | 제어 프레임 경로 — 틱 전달과 무관 |
| `clientOutboundChannel` | Spring | 클라 송신(MESSAGE 포함) + **`SessionConnectedEvent` 리스너**(실측: CONNECTED ack가 이 풀에서 나가며 이벤트도 여기서 발화) | §11.6 적용 후 여기 남은 것은 프레즌스 쓰기(SADD/EXPIRE, 1s 타임아웃)뿐. 큐 깊이 = 과부하 신호, 메트릭 필수 |
| broker channel | Spring | 브로커 fan-out | — |
| heartbeat `TaskScheduler` | 우리 등록 | 하트비트 송신/감시 | 브로커 설정 필수 |
| Redis listener 풀 | RedisConfig | `MessageRouter` → 핸들러 | **무거운 작업 금지.** 파싱+발행만. 파싱 실패는 카운터+드랍 |
| presence 스케줄러 | 우리 | TTL 연장 (~10s) | — |

---

## 6. 핵심 시퀀스

### 6.1 접속 (CONNECT → 첫 SUBSCRIBE에서 관심목록 해소)

관심목록 해소+등록은 **세션의 첫 SUBSCRIBE**에서 한다(§11.6) — 그 시점엔 세션이 이미
성립돼 있고, SessionSubscribeEvent는 전송 스레드에서 발화하므로 outbound 풀을 건드리지 않는다.

```
[WS 전송 스레드]
CONNECT(JWT) → StompAuthChannelInterceptor: TokenVerifier.verify → Principal
[outbound 스레드]
CONNECTED ack → SessionConnectedEvent → PresenceRegistry.add(userId, sessionId)
[WS 전송 스레드]
클라 첫 SUBSCRIBE (아무 목적지) → SessionSubscribeEvent → SessionEventListener
    → isSessionRegistered? 아니면: WatchlistResolver.resolve(userId)
    → DemandMutator.registerSession(...)   (신규 code마다 ChannelSubscriber.subscribe(quote/stream))
    → (방 토픽이면 이어서 subscribeRoom)
```

- registerSession은 멱등이라 같은 세션의 후속 SUBSCRIBE는 no-op. isSessionRegistered
  check-then-act의 동시 SUBSCRIBE 레이스도 멱등성으로 무해(중복 resolve 1회뿐).
- 아무것도 구독하지 않는 세션은 수요를 만들지 않는다 — 접속만 하고 노는 클라의 헛수요 제거.

### 6.2 틱 relay (가장 뜨거운 경로)

```
price-worker PUBLISH quote:005930 {envelope}
→ MessageRouter (Redis 스레드): Channels.parse → kind=QUOTE, code=005930 → QuoteRelayHandler
→ QuoteRelayHandler: 역직렬화 → DemandQuery.usersWatching("005930") 스냅샷(락 없이)
    → 각 userId: ClientMessageSink.sendToUser(userId, QUOTE, envelope)
→ BrokerMessageSink → convertAndSendToUser → 브로커 fan-out(outbound)
```

### 6.3 방 입장/퇴장

```
SUBSCRIBE /topic/rooms/005930/posts → 인터셉터 검증 → SessionSubscribeEvent
→ DemandMutator.enterRoom: roomIndex 0→1 이면 ChannelSubscriber.subscribe(post:005930)
(fan-out은 SimpleBroker 몫 — 우리는 Redis 구독 여부만 관리)
퇴장: UNSUBSCRIBE/DISCONNECT → 1→0 이면 unsubscribe
```

### 6.4 관심목록 변경 (재접속 없이 반영 — FR-03)

```
core-api: DB 커밋 후 PUBLISH watchlist:updated {userId, added, removed}
→ 모든 게이트웨이 MessageRouter 수신(상시 구독) → WatchlistUpdateHandler
→ 자기 인스턴스에 userId 세션 없으면 무시(broadcast-and-filter)
→ 있으면 DemandMutator.applyWatchlistDiff → refcount 전이 → ChannelSubscriber 조정
```

### 6.5 종료 (정상/하트비트 타임아웃 공통)

```
DISCONNECT 또는 하트비트 미수신 → 프레임워크 세션 정리 → SessionDisconnectEvent
→ DemandMutator.removeSession: 방 refcount 회수, 유저 마지막 세션이면 watchlist 수요 회수
→ PresenceRegistry.remove
```

---

## 7. 에러 처리 정책

| 상황 | 처리 |
|---|---|
| CONNECT JWT 실패 | `TokenVerifier`가 예외 → 프레임워크 `ERROR` + 종료. `message:unauthorized` |
| 허용 외 SUBSCRIBE / SEND | 인터셉터 예외 → `ERROR` + 종료. `stomp_errors` 증가 |
| watchlist 해소 실패(Redis 장애) | 세션 유지 + **빈 watchlist 시작** + 경고. 방 토픽 구독은 가능. 재연결 시 자연 복구 |
| 봉투 역직렬화 실패 | 해당 `RedisChannelHandler`가 드랍 + 카운터. relay 계속(한 건의 독이 채널을 막지 않게) |
| 느린 클라(송신 버퍼 초과) | 프레임워크가 세션 강제 종료 → 정리 이벤트 정상 발화 → 클라 재연결+REST 복구. `slow_client_disconnects` |
| Redis 연결 단절 | Lettuce 자동 재연결. 복구 시 `RedisChannelSubscriber`가 `DemandQuery` 스냅샷 기준 전 채널 재구독 |
| graceful shutdown | 새 연결 거부 → 전 세션 close → 리스너 컨테이너 stop |

---

## 8. 설정 (application.yml 골격)

```yaml
alphatalk:
  auth:
    jwt:
      secret: ${ALPHATALK_AUTH_JWT_SECRET}  # :auth-jwt 자동구성 — core-api와 동일 env (커밋 금지)

ws:
  transport:
    send-time-limit-ms: 10000
    send-buffer-size-limit-bytes: 524288
  presence:
    ttl-seconds: 30
    refresh-interval-seconds: 10
  features:
    trade-depth-enabled: false        # /topic/rooms/{code}/trade·depth 화이트리스트 토글
spring:
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: ${REDIS_PORT:6379}
```

---

## 9. 패키지 배치 (최종) — 포트는 도메인 패키지에 함께

```
ws/src/main/kotlin/com/alphatalk/ws/
├─ WsApplication.kt
├─ config/        WebSocketConfig · RedisConfig · WsProperties · MetricsConfig
├─ auth/          StompAuthChannelInterceptor · StompPrincipal   (TokenVerifier 포트는 :auth-jwt 소유)
├─ subscription/  DemandQuery(P) · DemandMutator(P) · DemandRegistry · SessionEventListener
├─ relay/         RedisChannelHandler(P) · {Quote,Stream,Post,Trade,Depth}RelayHandler · WatchlistUpdateHandler
│                 ChannelSubscriber(P) · RedisChannelSubscriber · MessageRouter · RelayBootstrap
├─ client/        ClientMessageSink(P) · BrokerMessageSink
├─ watchlist/     WatchlistResolver(P) · RedisWatchlistResolver
└─ presence/      PresenceRegistry(P) · RedisPresenceService

contracts/src/main/kotlin/com/alphatalk/contracts/
├─ Channels.kt      quote()/stream()/post()/trade()/depth()/WATCHLIST_UPDATED + parse() (생성·파싱 단일 지점)
├─ ChannelKind.kt   QUOTE·STREAM·POST·TRADE·DEPTH·WATCHLIST
├─ Keys.kt          presence()/price()/watchlist()/cursor()
├─ Destinations.kt  STOMP 목적지 패턴 (게이트웨이·클라 문서 공유 기준)
└─ envelope/        Envelope · QuoteData · StreamData · PostData · WatchlistUpdated
```

> **포트를 별도 `port/` 패키지에 몰지 않는다.** 인터페이스는 그것이 표현하는 도메인 옆에 둔다(응집). 예: `ClientMessageSink`는 `client/`. 단, **여러 서버가 공유하는 포트는 공유 라이브러리가 소유**한다 — `TokenVerifier`/`TokenIssuer`는 `:auth-jwt`(core-api와 공유 계약이므로).
> 의존 방향: `ws → contracts·auth-jwt` 단방향. 두 라이브러리 모두 Spring 무의존.

```
auth-jwt/src/main/kotlin/com/alphatalk/auth/
├─ TokenPorts.kt        TokenVerifier · TokenIssuer · InvalidTokenException
├─ JwtTokenProvider.kt  HS256 발급+검증 (jjwt는 내부 구현 — api로 노출하지 않음)
└─ spring/JwtAuthAutoConfiguration.kt  Boot 자동구성 + JwtAuthProperties(alphatalk.auth.jwt.*)
    (+ META-INF/spring/...AutoConfiguration.imports)
```
> 코어와 자동구성을 한 모듈에 두는 이유: 소비자가 전부 Boot 앱이라 모듈 분리(core/starter 2단)는 과설계. 코어 클래스는 여전히 Spring 없이 테스트된다.

---

## 10. 이 설계가 지키는 상위 요구사항

| 요구 | 설계 대응 |
|---|---|
| FR-03 재접속 없이 구독 반영 | §6.4 `watchlist:updated` → `applyWatchlistDiff` |
| FR-06/07 p95 ≤ 1s | 뜨거운 경로(§6.2)는 파싱+스냅샷 조회+발행뿐, 락·블로킹 I/O 없음 |
| NFR-03 동접 500 | 인덱스 O(1) 조회, 전송 제한으로 느린 클라 격리. S7 스모크로 검증 |
| NFR-06 core-api 배포 무영향 | 게이트웨이는 프로세스·배포 분리, 의존은 Redis뿐 |
| NFR-07 보안 | JWT는 CONNECT 헤더만, 쿼리파라미터 금지, 토큰 로그 금지, 시크릿 환경변수 |
| NFR-10 관측성·유지보수 | 포트 경계로 각 컴포넌트를 인프라 없이 단위 테스트(§3.5), 장애 원인 격리 |
| Redis 계약 §4 "게이트웨이는 SUBSCRIBE만" | DB·Streams·price 캐시 접근 코드 없음 (프레즌스 쓰기만 예외 — 계약 명시) |

---

## 11. 설계 문답 — 구현 리뷰에서 확정한 결정들

리뷰·면접에서 반복해서 나오는 질문과 확정 답. 코드만 봐서는 "왜"가 안 보이는 지점들이다.

### 11.1 관심목록 Redis 구독을 왜 SUBSCRIBE가 아니라 CONNECT 시점에 시작하나

**수요의 스위치는 그 수요의 진실이 있는 곳을 따른다.**

| 수요 | 진실의 위치 | 스위치 |
|---|---|---|
| 관심목록 (quote/stream) | 서버 측 영속 상태 (core-api DB, REST로 편집) | 켬: **세션의 첫 SUBSCRIBE**(§11.6) · 끔: **종료** · 조정: **`watchlist:updated`** |
| 보는 방 (post/trade/depth) | 클라이언트 화면 상태 (서버는 알 수 없음) | **해당 토픽의 STOMP SUBSCRIBE/UNSUBSCRIBE** |

- 관심목록은 서버가 스스로 조회할 수 있으므로 접속 시점에 해소한다(명세 §3.1 서버 해소 모델). 클라의 `/user/queue/*` SUBSCRIBE는 **전달 계층의 파이프 연결일 뿐, 수요 신호가 아니다.**
- SUBSCRIBE 기준으로 바꾸면 큐 종류별 구독 상태를 code→유저 인덱스와 조합해 추적해야 해 refcount 모델이 복잡해지고, 정상 흐름(CONNECTED 직후 즉시 구독)에서 얻는 이득은 수 ms뿐이다.
- 반대로 방 수요를 CONNECT 시점으로 통일할 수도 없다 — 서버는 유저가 지금 어느 방을 보는지 알 수 없고, trade/depth는 무거워 "보는 방만"이 명세 요구(기획안 §2.5-2)다.

### 11.2 user-queue의 UNSUBSCRIBE는 왜 수요를 해제하지 않나 (비대칭)

§11.1의 귀결이다. `/user/queue/quote` UNSUBSCRIBE는 **브로커 계층에서만 유효**(그 세션으로 전달 중단)하고, Redis 수요는 유지된다. 관심목록 수요를 끄는 정당한 경로는 ① 연결 종료 ② REST로 관심목록 제거(→ `watchlist:updated`)다. 방 토픽은 UNSUBSCRIBE가 두 계층 모두 해제한다. 이 비대칭은 `DemandRegistryTest`("방 구독이 아닌 subId의 UNSUBSCRIBE - 무해")로 고정되어 있다.

- 알려진 한계: **구독 후 UNSUBSCRIBE하고 접속만 유지**하는 비정상 클라는 관심목록 크기만큼 헛 fan-out을 만든다(아예 구독하지 않는 클라의 헛수요는 §11.6의 첫-SUBSCRIBE 등록으로 해소됨). 유저당 관심목록 크기로 유계 — S7 부하 측정에서 유의미하면 "세션별 큐 구독 여부" 게이트를 추가한다.

### 11.3 DemandRegistry는 왜 ConcurrentHashMap이 아니라 HashMap + 단일 락인가

**동시성 도구는 보장 단위로 고른다 — 연산이면 CHM, 트랜잭션이면 락.**

- CHM은 연산 하나의 원자성만 보장한다. 이 클래스의 변경은 맵 4개 + `ChannelSubscriber` 부수효과에 걸친 **트랜잭션**이다. CHM만 쓰면: ① check-then-act 레이스로 접속 없는 유저의 채널이 영원히 구독되는 **유령 구독**, ② 전이 판정 순서와 Redis 명령 실행 순서가 어긋나는 **구독 유실**(1→0의 unsubscribe가 0→1의 subscribe보다 늦게 실행)이 가능하다. 락은 판정과 부수효과 실행을 한 임계구역에 묶어 이를 차단한다.
- **뜨거운 읽기만 락 프리로 한다**: `usersWatching`(틱마다 호출)은 CHM + 불변 Set 교체(copy-on-write)로 락 없이 읽는다. 나머지 읽기(`connectedUserIds` 등 — 프레즌스 10s 주기·메트릭 스크랩)는 분당 몇 회 수준이라 락을 잡는다. HashMap을 락 없이 읽으면 데이터 레이스이고, 락 잡은 읽기는 트랜잭션 중간 상태를 보지 않는 일관 스냅샷이라는 덤이 있다.
- 쓰기 빈도(접속/구독 변경, 초당 수십 건)에서 단일 락 경합은 사실상 0. 락 세분화는 S7에서 병목으로 확인될 때만.

### 11.4 DemandRegistry를 저장/조회/수정 클래스로 쪼개지 않는 이유

- **전이 규칙은 상태의 불변식이다.** 상태(인덱스 4개)와 그 불변식(0↔1 전이 + 부수효과 순서)을 다른 클래스로 나누면, 저장 클래스가 내부 맵을 노출해야 하고 "인메모리 상태 단일 소유"(불변 규칙 6)가 컴파일 타임 보증에서 관례로 격하된다. 캡슐화를 지키는 방향으로 나누면 저장 클래스가 곧 지금의 DemandRegistry이고 나머지는 위임 껍데기다.
- 역할 분리는 이미 **인터페이스 레벨**(ISP — `DemandQuery`/`DemandMutator`)에서 하고 있다. "인터페이스는 좁게, 구현은 불변식 단위로 응집"은 `JwtTokenProvider`(TokenIssuer+TokenVerifier)와 같은 패턴.
- **DI 순환의 범인은 클래스 합침이 아니다.** 순환(`DemandRegistry → ChannelSubscriber → MessageRouter → 핸들러 → Demand*`)의 본질은 "수신이 구독을 바꾼다"는 `watchlist:updated`의 피드백 루프다. Query 구현을 분리해도 Mutator 경로(`WatchlistUpdateHandler → DemandMutator → ChannelSubscriber`)의 순환은 남는다. 절단점은 `RedisChannelSubscriber`의 `ObjectProvider<MessageRouter>`(생성 시점이 아닌 첫 사용 시점 해소) — 우회가 아니라 의도된 설계다.

### 11.5 watchlist:updated를 왜 데이터 채널과 같은 라우터로 처리하나

- 성격은 다르다 — quote/stream/post는 **데이터 평면**(클라에게 전달할 콘텐츠, 드랍 무해), `watchlist:updated`는 **제어 평면**(게이트웨이 자신의 수요 인덱스를 바꾸는 명령).
- 그럼에도 현재는 단일 `MessageRouter` + 핸들러 맵으로 균일하게 처리한다(OCP 균일성, 코드 최소). 제어 리스너를 컨테이너에 직접 등록하는 분리안은 §11.4의 DI 순환도 근본 제거하지만, 채널 하나를 위해 등록 경로가 이원화되는 비용이 있어 보류.
- **재검토 트리거**: 제어 채널이 하나 더 생기거나, trade/depth 활성화로 라우터 구조를 손댈 때 — 그 시점에는 제어/데이터 평면 분리가 이득이다.

### 11.6 [적용됨] watchlist 해소의 블로킹 위치 — 최종: 첫 SUBSCRIBE에서 해소·등록

- **문제(실측)**: `SessionConnectedEvent`는 CONNECTED ack가 클라로 나가는 `clientOutboundChannel` 스레드에서 동기 발화한다. 초기 구현은 onConnected에서 watchlist 해소(SMEMBERS)를 수행해, **틱 MESSAGE 전달과 같은 풀**에서 Redis I/O가 블로킹됐다. 평시(산발 접속)엔 무해하나, **재접속 폭풍**(재배포 → 동접 전원 백오프 재연결) 시 부하가 가장 큰 순간에 틱 지연(p95 스파이크)을 만드는 구조였다.
- **최종 해법 — 세션의 첫 SUBSCRIBE에서 해소+등록**: `SessionSubscribeEvent`(전송 스레드에서 발화 — 실측 `o-auto-N-exec-*`)에서 `isSessionRegistered` 확인 후 미등록이면 resolve → `registerSession`. 이 시점의 세 가지 이점: ① 세션이 **이미 성립**돼 있어 유령 등록 없음 ② 해소와 등록이 **같은 시점·같은 스레드**라 핸드오프 구조 불필요 ③ 아무것도 구독 않는 세션은 수요를 안 만듦(§11.2의 헛수요 한계 해소). registerSession 멱등 + check-then-act 레이스는 중복 resolve 1회로 무해.
- **거쳐간 대안들**:
  - *CONNECT 프리페치 인터셉터 + 스태시* (1차 적용 후 대체): 해소(전송 스레드)와 등록(outbound)의 시점이 갈라져 `PendingWatchlists` 핸드오프 버퍼 + 잔류 정리가 필요했다. 첫 SUBSCRIBE 방식이 같은 스레드 배치를 더 적은 구조로 달성해 대체.
  - *SessionConnectEvent 청취(한 줄 변경)*: 스레드 목표는 달성하나 event.user가 비어 있고(핸드셰이크 주체), **성립 전 등록**이라 실패 세션의 유령 수요를 disconnect 정리에 의존 — 기각.
- **블로킹은 제거가 아니라 이전이다**: 블로킹 스택(MVC + 동기 Lettuce)에서 Redis I/O는 반드시 어떤 스레드를 점유한다 — 선택할 수 있는 건 지불하는 풀뿐이다. 전송 워커는 프레임 단위로 빌리는 공유 풀(기본 200)로 outbound(코어×2)의 10배라, 폭풍의 Redis I/O가 워커당 수 ms로 분산되고 틱 전달과 격리된다. Tomcat 워커의 블로킹 I/O는 서블릿 스택의 설계 중심(모든 REST 핸들러가 하는 일)이며, 제로 블로킹이 필요해지면 그건 WebFlux 전환의 문제다(v0.2에서 기각).
- **Redis 장애 방어**: 진짜 위험은 폭풍이 아니라 Redis 자체가 느려질 때다. 포트 계약(블로킹 ≤ 1s, §4.5)을 `spring.data.redis.timeout: 1s`로 강제 — Lettuce 기본(60s)대로면 장애 시 워커가 장시간 물려 풀 고갈로 번진다.
- **잔여 사항**: outbound에 남은 Redis I/O는 프레즌스 쓰기(SADD/EXPIRE, 1s 상한)뿐. S7 재접속 폭풍 시나리오에서 최종 검증.
