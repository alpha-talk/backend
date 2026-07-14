# Alpha Talk — WebSocket(STOMP) API 명세 v0.4
**WS Gateway · 실시간 푸시 전용**

> **v0.3 → v0.4**: 게이트웨이 구현 스택을 WebFlux → **Spring MVC + STOMP 브로커**로 변경 (하단 구현 노트만 수정, 클라이언트 노출 프로토콜 §1~§9는 변경 없음)
 
---

## 1. 개요

이 문서는 **WS 게이트웨이**가 클라이언트에 제공하는 실시간 푸시 API다.

- 게이트웨이는 **푸시 전용 얇은 엣지**다: 서버→클라 실시간 전달 + 구독 제어만 한다.
- **클라이언트가 콘텐츠를 보내는 SEND 프레임은 없다.** 글/댓글 작성·관심목록 편집·로그인·과거 조회는 전부 **메인서버 REST**가 담당한다(§8).
- 전달 데이터 3종: **① 주식 틱(현재가) · ② 소식(뉴스/리포트/AI) · ③ 글/댓글 — 게시판(post·comment)**
- 프로토콜: **STOMP 1.2 over WebSocket**
> 설계 원칙: DB가 진실의 원천이고 WS 푸시는 best-effort다. 끊기면 재연결 + REST로 복구한다(§6).
 
---

## 2. 연결 (Connection)

| 항목 | 값 |
|---|---|
| 엔드포인트 | `wss://{host}/ws` (리버스 프록시 `/ws` → 게이트웨이) |
| 프로토콜 | WebSocket 업그레이드 → STOMP 1.2 |
| 하트비트 | `heart-beat:10000,10000` (양방향 10초) |

### 2.1 인증

WS 핸드셰이크 직후 보내는 **STOMP `CONNECT` 프레임 헤더에 JWT**를 싣는다.

```
CONNECT
Authorization:Bearer <accessToken>
heart-beat:10000,10000
accept-version:1.2
```

- **토큰을 `wss://` URL 쿼리파라미터로 보내지 말 것** — 프록시/서버 로그에 남아 유출된다. 반드시 CONNECT 헤더로.
- 게이트웨이가 JWT를 검증한다(메인서버와 공유하는 검증 모듈). 실패 시 `ERROR` 프레임 후 연결 종료.
- 유효한 `CONNECTED` 이전의 모든 `SUBSCRIBE`는 거부한다.
### 2.2 하트비트 / 타임아웃

- 클라·서버 모두 10초마다 핑. 리버스 프록시의 유휴 타임아웃은 **하트비트보다 길게**(예: 60초) 설정해야 프록시가 멀쩡한 연결을 끊지 않는다.
- half-open 대비로 게이트웨이는 하트비트 미수신 시 세션을 정리하고 프레즌스 TTL을 만료시킨다.
---

## 3. 구독 모델 (Subscriptions)

두 종류로 나뉜다. **가벼운 건 서버가 관심목록으로 대신 구독, 무거운 건 클라가 보는 방만 구독.**

### 3.1 서버 해소 — 관심목록 전체 (클라는 큐 2개만 구독)

입장 시 게이트웨이가 그 유저의 관심목록을 조회해, **관심목록 N종목의 틱·소식을 아래 큐로 흘린다.** 클라는 종목별로 구독하지 않고, 데이터 타입 큐 2개만 구독한다. 어느 종목인지는 페이로드의 `code`로 구분한다.

| 구독 목적지 | 데이터 | 범위 | 구독 주체 |
|---|---|---|---|
| `/user/queue/quote` | 현재가 틱 | 관심목록 N종목 | **서버** (입장 시 해소) |
| `/user/queue/stream` | 소식 | 관심목록 N종목 | **서버** (입장 시 해소) |

- 관심목록 변경은 메인서버 REST로 이뤄진다. 메인서버가 **단일 채널 `watchlist:updated`(Pub/Sub)** 로 `{userId, added, removed}`를 발행하면, 모든 게이트웨이가 이를 구독해 **자신이 들고 있는 세션이면 그 세션의 구독을 조정**한다(broadcast-and-filter — 유저별 채널을 만들지 않는다). *(간단 대안: 알림 없이 다음 재연결 시 재해소)*
### 3.2 클라 동적 — 보는 방 1개 (입장 시 구독, 퇴장 시 해제)

방(종목 상세 화면)에 들어갈 때 그 방의 토픽을 `SUBSCRIBE`하고, 나갈 때 `UNSUBSCRIBE`한다. **구독 프레임 자체가 "방 입장" 신호**이고, 게이트웨이는 그 시점에 해당 Redis 채널을 (아직 안 했으면) 구독해 relay한다.

| 구독 목적지 | 데이터 | 범위 |
|---|---|---|
| `/topic/rooms/{code}/posts` | 글/댓글(post·comment) | 보는 방 1개 |
| `/topic/rooms/{code}/trade` *(선택)* | 체결 | 보는 방 1개 |
| `/topic/rooms/{code}/depth` *(선택)* | 호가 | 보는 방 1개 |

> `trade`/`depth`는 호가창·체결 풀데이터로 무거우므로 보는 방에서만. 지금 단계 필수는 `post`이고 나머지는 확장.
 
---

## 4. 메시지 스키마 (Server → Client)

모든 푸시는 **공통 봉투 + 타입별 `data`** 형태다.

### 4.1 공통 봉투

```json
{
  "type": "quote | stream | post | trade | depth",
  "code": "005930",
  "eventId": "01J9Z8X7...",
  "ts": 1719600000000,
  "data": { }
}
```

| 필드 | 설명 |
|---|---|
| `type` | 이벤트 종류 |
| `code` | 종목 코드 (어느 종목/방인지) |
| `eventId` | ULID. **stream·post은 필수**(순서·중복제거). quote/trade/depth는 선택(스냅샷성) |
| `ts` | 서버 송신 시각 (epoch ms) |
| `data` | 타입별 페이로드 |

### 4.2 `quote` (현재가 틱)

```json
{
  "price": 71200, "prevClose": 70500,
  "change": 700, "changeRate": 0.99,
  "volume": 1234567,
  "open": 70600, "high": 71500, "low": 70400
}
```

### 4.3 `stream` (소식)

```json
{
  "category": "news | report | ai | disclosure",
  "title": "...",
  "summary": "...",
  "sourceUrl": "https://...",
  "occurredAt": 1719500000000
}
```

### 4.4 `post` (글/댓글)

```json
{
  "kind": "post | comment",
  "postId": "01J9...",
  "parentId": "01J8... | null",
  "author": { "id": 123, "nickname": "..." },
  "content": "...",
  "createdAt": 1719600000000
}
```

### 4.5 `trade` / `depth` *(선택)*

```json
// trade
{ "price": 71200, "qty": 10, "side": "buy | sell", "tradedAt": 1719600000000 }
// depth
{ "bids": [[71100, 320], [71000, 540]], "asks": [[71200, 210], [71300, 480]] }
```
 
---

## 5. 클라이언트 프레임 (제어 전용)

클라가 보내는 프레임은 **제어 프레임뿐**이다. 콘텐츠 SEND는 없다.

| 프레임 | 용도 |
|---|---|
| `CONNECT` | JWT 인증 + 하트비트 협상 |
| `SUBSCRIBE` | §3의 목적지 구독 (서버 해소 큐 2개 + 방 토픽) |
| `UNSUBSCRIBE` | 방 퇴장 시 해당 방 토픽 해제 |
| `DISCONNECT` | 정상 종료 |

> 글/댓글 작성, 관심목록 편집 등 **상태를 바꾸는 행위는 전부 메인서버 REST**다. WS로는 `SEND` 콘텐츠를 받지 않는다.
 
---

## 6. 전달 의미론 (Delivery Semantics)

- **틱 coalescing**: `quote`는 합쳐져서 온다(종목당 약 100~250ms 간격의 최신값). **델타가 아니라 항상 "최신 스냅샷"** 이므로 클라는 받은 값으로 덮어쓰면 된다. 중간 틱 누락은 정상이다.
- **순서 / 중복제거**: `stream`·`post`은 `eventId`(ULID) **오름차순**으로 의미를 가진다. 클라는 `eventId`로 중복 제거하고 정렬한다.
- **복구**: 끊긴 동안 놓친 `stream`·`post`은 **메인서버 REST로 "마지막 eventId 이후"를 조회**해 메운다. WS는 과거 메시지를 재전송하지 않는다(live-only). **틱은 복구하지 않는다**(다음 틱이 대체).
- **best-effort**: WS 전달 실패는 에러가 아니다. DB가 진실의 원천이고, 재연결 + REST 복구로 보강한다.
---

## 7. 에러 & 재연결

- **인증 실패/만료**: `ERROR` 프레임(예: `message:unauthorized`) 후 연결 종료. 클라는 토큰 갱신 후 재연결.
- **비정상 종료**: 클라는 **지수 백오프 + 지터**로 재연결(동시 재접속 폭주 완화). 재연결 후: ① 서버가 관심목록 재해소 → ② 클라가 보던 방 재구독 → ③ 놓친 `stream`/`post`은 REST 복구.
- **배포**: graceful close. 게이트웨이/메인서버가 분리돼 있어 **메인서버 배포는 WS 연결에 영향이 없다.**
---

## 8. 범위 밖 — 메인서버 REST가 담당

WS 게이트웨이가 하지 않는 것(같은 클라가 REST로 별도 호출):

- 로그인 / 토큰 발급·갱신
- 글·댓글 작성·수정·삭제
- 관심목록 조회·편집
- 과거 피드·글/댓글 조회 + **커서 기반 복구**
- 차트(봉) 조회
---

## 9. 업스트림 계약 — 게이트웨이 ← Redis (`:contracts`)

게이트웨이는 워커·메인서버가 발행한 Redis 채널을 구독해 클라 목적지로 relay한다. 채널명·DTO는 `:contracts` 서브프로젝트에서 공유한다(게이트웨이/워커/메인서버 합의 필요).

> 아래는 **게이트웨이가 직접 구독하는 실시간 Pub/Sub 채널**만이다. 작업 큐(`queue:ingest`, Redis Streams)·자료구조·발행 순서·LLM 요약 파이프라인을 포함한 전체 규약은 **별도 문서 「Alpha Talk — Redis 계약」** 참조.
> ⚠️ 여기 `stream:{code}`는 **Pub/Sub 채널**이다. Redis Streams(데이터 구조)는 작업 큐 `queue:ingest` 하나뿐이며 게이트웨이는 그걸 보지 않는다.

| 클라 목적지 | Redis 채널 (구독 대상) | 발행 주체 |
|---|---|---|
| `/user/queue/quote` (해당 종목) | `quote:{code}` | price-worker |
| `/user/queue/stream` (해당 종목) | `stream:{code}` | llm-worker |
| `/topic/rooms/{code}/posts` | `post:{code}` | 메인서버(글 작성 시 발행) |
| `/topic/rooms/{code}/trade` *(선택)* | `trade:{code}` | price-worker |
| `/topic/rooms/{code}/depth` *(선택)* | `depth:{code}` | price-worker |

- 게이트웨이는 같은 종목을 보는 클라가 N명이어도 Redis 채널은 **한 번만 구독**하고 N명에게 fan-out한다(중복 제거).
---

## 10. 부록 — 프레임 시퀀스 예시

```
# 연결 + 인증
C: CONNECT
   Authorization:Bearer eyJ...
   heart-beat:10000,10000
   accept-version:1.2
S: CONNECTED
   heart-beat:10000,10000
 
# 관심목록 전체 자동 수신 (큐 2개만 구독)
C: SUBSCRIBE  id:sub-quote   destination:/user/queue/quote
C: SUBSCRIBE  id:sub-stream  destination:/user/queue/stream
   # (서버: 관심목록 N종목의 quote·stream relay 시작)
S: MESSAGE  destination:/user/queue/quote   {"type":"quote","code":"005930",...}
S: MESSAGE  destination:/user/queue/stream  {"type":"stream","code":"000660","eventId":"01J...",...}
 
# 방 입장 → 글/댓글 수신
C: SUBSCRIBE  id:sub-room-005930  destination:/topic/rooms/005930/posts
S: MESSAGE  destination:/topic/rooms/005930/posts  {"type":"post","code":"005930","eventId":"01J...","data":{"kind":"post",...}}
 
# 방 퇴장 → 종료
C: UNSUBSCRIBE  id:sub-room-005930
C: DISCONNECT
```
 
---

## 구현 노트 (게이트웨이 = Spring MVC + STOMP 브로커)

- 게이트웨이는 **Spring MVC 스택 + `@EnableWebSocketMessageBroker`(SimpleBroker)** 로 구현한다. STOMP 프레이밍·하트비트·구독 레지스트리·`/user` 목적지 해소·MESSAGE fan-out은 프레임워크가 담당한다.
- 인증은 `clientInboundChannel`의 `ChannelInterceptor`에서 처리한다: CONNECT의 JWT 검증, CONNECTED 이전 SUBSCRIBE 거부, 목적지 화이트리스트, **콘텐츠 SEND 무조건 거부**(§5).
- 게이트웨이 고유 로직은 **수요 카운트**(code→유저/세션 인덱스 = Redis 채널 refcount)와 Redis 수신→브로커 발행 relay다. `quote:`/`stream:`은 관심목록 인덱스로 대상 유저를 찾아 `convertAndSendToUser`, `post:`는 `/topic/rooms/{code}/posts`로 `convertAndSend`.
- SimpleBroker는 인스턴스별 인메모리지만, 게이트웨이 간 버스는 Redis Pub/Sub이 담당(broadcast-and-filter)하므로 외부 브로커(RabbitMQ relay)는 불필요하다.
- 틱 conflation(100~250ms)은 price-worker 책임이고, 게이트웨이는 전송 제한(`setSendTimeLimit`/`setSendBufferSizeLimit`)으로 느린 클라를 방어한다 — 버퍼 초과 세션은 강제 종료하고 클라가 재연결+REST 복구한다(§6·§7과 일관).
 
