# Alpha Talk — WebSocket(STOMP) API 명세 v0.8
**WS Gateway · 실시간 푸시 전용**

> **v0.7 → v0.8**: `digest{}`에 optional `marketAnalysis{}` 추가 — 하루 1건 생성되는 시장 매크로 브리핑(순환매·수급·해외 지표)을 전 종목 일일 브리핑에 동일하게 삽입([뉴스 파이프라인 명세](alphatalk_news_worker_spec.md) §4.3). 생성이 늦거나 실패하면 필드가 생략된다. 기존 필드 변경 없음 — 비파괴.
> **v0.6 → v0.7**: `opinion{}`에서 `ratingCode`·`previousRatingCode` **제거** — KIS 실계정 계측 결과 `invt_opnn_cls_code`는 등급 분류가 아니라 위치 값(현재 의견=2·직전 의견=3 고정)이라 정보가 없다([KIS 워커 명세](alphatalk_kis_worker_spec.md) §3.3·§9-7). 아직 발행 코드가 없어 기수신 클라이언트 영향도 없다. 필수 필드는 `brokerCode`·`rating`·`businessDate`.
> **v0.5 → v0.6**: `stream`의 `category=report`에 증권사 투자의견 subtype 추가 — optional `kind=opinion`·`opinion{}` 필드. STOMP 목적지·봉투·기존 필드는 변경 없음([KIS 워커 명세](alphatalk_kis_worker_spec.md) §3.3).
> **v0.4 → v0.5**: `stream` payload 확장(§4.3) — `sentiment`·`scope`·`sector`·`sources[]`(news) · `digest{}`(ai) **optional** 필드 추가([뉴스 파이프라인 명세](alphatalk_news_worker_spec.md) §3.5·§3.6·§4.2). 기존 필드 변경 없음 — 모르는 필드는 무시하면 된다(비파괴).
> **v0.3 → v0.4**: 게이트웨이 구현 스택을 WebFlux → **Spring MVC + STOMP 브로커**로 변경 (하단 구현 노트만 수정, 클라이언트 노출 프로토콜 §1~§9는 변경 없음)

---

## 1. 개요

이 문서는 클라이언트 ↔ **WS 게이트웨이**의 STOMP 프로토콜 계약이다. 목적지·프레임·페이로드·에러 처리를 여기서 정한다. 실시간 수신을 구현하거나 바꿀 때 클라·게이트웨이 양쪽이 이 문서를 기준으로 삼는다. 게이트웨이 코드 레벨 설계는 [ws_architecture.md](ws_architecture.md), 게이트웨이 ↔ 워커·메인서버의 Redis 계약은 [redis_contract.md](redis_contract.md) 몫이다.

설계 전제는 하나다. **DB가 진실의 원천이고 WS 푸시는 best-effort다.** 그래서 게이트웨이는 서버→클라 전달과 구독 제어만 하는 **푸시 전용 얇은 엣지**다. 끊기면 재연결 + REST로 복구한다(§6).

- **클라이언트가 콘텐츠를 보내는 SEND 프레임은 없다.** 글/댓글 작성·관심목록 편집·로그인·과거 조회는 전부 **메인서버 REST**가 담당한다(§8).
- 전달 데이터 3종: **① 주식 틱(현재가) · ② 소식(뉴스/리포트/투자의견/AI) · ③ 글/댓글 — 게시판(post·comment)**
- 프로토콜: **STOMP 1.2 over WebSocket**

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

- `wss://` URL 쿼리파라미터에 실린 토큰은 프록시·서버 로그에 남아 유출된다. 그래서 **토큰은 반드시 CONNECT 헤더로만 보낸다.**
- 게이트웨이가 JWT를 검증한다(메인서버와 공유하는 검증 모듈). 실패하면 `ERROR` 프레임 후 연결을 종료한다.
- 유효한 `CONNECTED` 이전의 모든 `SUBSCRIBE`는 거부한다.

### 2.2 하트비트 / 타임아웃

- 클라·서버 모두 10초마다 핑. 리버스 프록시의 유휴 타임아웃이 하트비트보다 짧으면 프록시가 멀쩡한 연결을 끊는다. 유휴 타임아웃은 **하트비트보다 길게**(예: 60초) 잡는다.
- 하트비트가 끊긴 세션은 half-open일 수 있다. 게이트웨이는 하트비트 미수신 시 세션을 정리하고 프레즌스 TTL을 만료시킨다.

---

## 3. 구독 모델 (Subscriptions)

구독은 두 종류다. **가벼운 건 서버가 관심목록으로 대신 구독, 무거운 건 클라가 보는 방만 구독.**

### 3.1 서버 해소 — 관심목록 전체 (클라는 큐 2개만 구독)

입장하면 게이트웨이가 그 유저의 관심목록을 조회해 **관심목록 N종목의 틱·소식을 아래 큐로 흘린다.** 클라는 종목별로 구독하지 않고 데이터 타입 큐 2개만 구독한다. 어느 종목인지는 페이로드의 `code`로 구분한다.

| 구독 목적지 | 데이터 | 범위 | 구독 주체 |
|---|---|---|---|
| `/user/queue/quote` | 현재가 틱 | 관심목록 N종목 | **서버** (입장 시 해소) |
| `/user/queue/stream` | 소식 | 관심목록 N종목 | **서버** (입장 시 해소) |

- 관심목록은 메인서버 REST로 바꾼다. 변경 시 메인서버가 **단일 채널 `watchlist:updated`(Pub/Sub)** 로 `{userId, added, removed}`를 발행한다. 모든 게이트웨이가 이 채널을 구독하다가 **자신이 들고 있는 세션이면 그 세션의 구독을 조정**한다(broadcast-and-filter — 유저별 채널을 만들지 않는다). *(간단 대안: 알림 없이 다음 재연결 시 재해소)*

### 3.2 클라 동적 — 보는 방 1개 (입장 시 구독, 퇴장 시 해제)

방(종목 상세 화면)에 들어갈 때 그 방의 토픽을 `SUBSCRIBE`하고 나갈 때 `UNSUBSCRIBE`한다. **구독 프레임 자체가 "방 입장" 신호**다. 게이트웨이는 그 시점에 해당 Redis 채널을 (아직 안 했으면) 구독해 relay한다.

| 구독 목적지 | 데이터 | 범위 |
|---|---|---|
| `/topic/rooms/{code}/posts` | 글/댓글(post·comment) | 보는 방 1개 |
| `/topic/rooms/{code}/trade` *(선택)* | 체결 | 보는 방 1개 |
| `/topic/rooms/{code}/depth` *(선택)* | 호가 | 보는 방 1개 |

> `trade`/`depth`는 호가창·체결 풀데이터로 무거우므로 보는 방에서만 받는다. 지금 단계에서 필수는 `post`이고 나머지는 확장이다.

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
| `eventId` | ULID. **stream·post는 필수**(순서·중복제거). quote/trade/depth는 선택(스냅샷성) |
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
  "occurredAt": 1719500000000,

  "sentiment": "POSITIVE | NEGATIVE | NEUTRAL",
  "scope": "STOCK | SECTOR | MARKET",
  "sector": { "code": "27", "name": "은행" },
  "sources": [ { "name": "한국경제", "url": "https://..." } ],
  "digest": {
    "date": "2026-07-16", "positives": [], "negatives": [], "sectorIssues": [], "marketIssues": [],
    "neutralCount": 0, "newsCount": 0,
    "marketAnalysis": {
      "summary": "…시장 종합 3줄…",
      "domestic": [ { "title": "반도체→2차전지 순환매", "line": "…" } ],
      "global": [ { "title": "미 10년물 4.1%로 하락", "line": "…", "sourceIds": ["s1"] } ],
      "sources": [ { "id": "s1", "title": "기사 제목", "url": "https://…", "publisher": "Reuters" } ],
      "asOf": "2026-07-16T17:40:00+09:00",
      "factDate": "2026-07-16",
      "degraded": false
    }
  },

  "kind": "opinion",
  "opinion": {
    "brokerCode": "00005",
    "brokerName": "미래에셋",
    "rating": "매수",
    "previousRating": "중립",
    "targetPrice": 95000,
    "businessDate": "20260727"
  }
}
```

- 빈 줄 아래 필드는 **전부 optional**이다.
  - `sentiment`·`scope`·`sector`·`sources`: 뉴스·공시·일반 리포트
  - `digest`: `category=ai` 일일 브리핑. `digest.marketAnalysis`는 그 안에서도 optional이다 — 하루 1건의 시장 브리핑을 전 종목에 동일 삽입하며(종목별 내용이 아니다), 생성 지연·실패 시 생략된다. `degraded=true`는 일부 입력(해외 리서치 등)이 빠진 채 생성됐다는 뜻이고, `global[]` 항목의 `sourceIds`가 `sources[]`의 `id`를 가리켜 수치별 검색 근거를 잇는다. 신선도 판단은 `date`가 아니라 `asOf`(생성 기준 시각)·`factDate`(국내 데이터 기준 거래일)로 한다 — 지연 생성 시 `asOf`가 늦고, 주말·휴장일엔 `factDate`가 지난 거래일이며, 국내 팩트 층이 빠진 산출물엔 `factDate`가 없다(optional)
  - `kind=opinion`·`opinion`: `category=report`인 증권사 투자의견
- 투자의견은 `summary`·`sourceUrl`·`sentiment`를 싣지 않는다. `occurredAt`은 최초 수집 시각이다. KIS가 제공한 영업일자는 `opinion.businessDate`에 원문 그대로 둔다.
- `opinion.brokerCode`는 KIS 회원사 마스터의 5자리 코드, `opinion.brokerName`은 KIS 응답의 회원사명이다. `rating`·`previousRating`은 회원사가 쓴 표기 그대로다(`매수`·`BUY`·`NotRated` 등 — 표준화하지 않는다).
- `opinion.brokerName`·`previousRating`·`targetPrice`는 원천 값이 없으면 `null`일 수 있다(목표가는 KIS가 `0`으로 주는 무의견도 `null`). `brokerCode`·`rating`·`businessDate`는 필수다.
- 뉴스 상세 구조·생성 규칙은 [뉴스 파이프라인 명세](alphatalk_news_worker_spec.md) §3.5·§3.6·§4.2, 투자의견 수집·멱등 규칙은 [KIS 워커 명세](alphatalk_kis_worker_spec.md) §3.3이 소유한다.

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

클라가 보내는 프레임은 **제어 프레임뿐**이다. 글/댓글 작성·관심목록 편집처럼 상태를 바꾸는 행위가 메인서버 REST 한 곳으로만 흐를 때 DB가 진실의 원천으로 유지된다(§1·§8). 그래서 WS로는 콘텐츠 `SEND`를 받지 않는다.

| 프레임 | 용도 |
|---|---|
| `CONNECT` | JWT 인증 + 하트비트 협상 |
| `SUBSCRIBE` | §3의 목적지 구독 (서버 해소 큐 2개 + 방 토픽) |
| `UNSUBSCRIBE` | 방 퇴장 시 해당 방 토픽 해제 |
| `DISCONNECT` | 정상 종료 |

---

## 6. 전달 의미론 (Delivery Semantics)

- **best-effort**: WS 전달 실패는 에러가 아니다. DB가 진실의 원천이고 재연결 + REST 복구로 보강한다(§1). 아래 규칙은 전부 이 전제에서 나온다.
- **틱 coalescing**: `quote`는 합쳐져서 온다(종목당 약 100~250ms 간격의 최신값). **델타가 아니라 항상 "최신 스냅샷"** 이므로 클라는 받은 값으로 덮어쓰면 된다. 중간 틱 누락은 정상이다.
- **순서 / 중복제거**: `stream`·`post`는 `eventId`(ULID) **오름차순**이 순서의 기준이다. 클라는 `eventId`로 중복을 제거하고 정렬한다.
- **복구**: 끊긴 동안 놓친 `stream`·`post`는 **메인서버 REST로 "마지막 eventId 이후"를 조회**해 메운다. WS는 과거 메시지를 재전송하지 않는다(live-only). **틱은 복구하지 않는다**(다음 틱이 대체).

---

## 7. 에러 & 재연결

- **인증 실패/만료**: `ERROR` 프레임(예: `message:unauthorized`) 후 연결 종료. 클라는 토큰 갱신 후 재연결한다.
- **비정상 종료**: 클라는 **지수 백오프 + 지터**로 재연결한다(동시 재접속 폭주 완화). 재연결 후: ① 서버가 관심목록 재해소 → ② 클라가 보던 방 재구독 → ③ 놓친 `stream`/`post`는 REST 복구.
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

> 아래는 **게이트웨이가 직접 구독하는 실시간 Pub/Sub 채널**만이다. 작업 큐(`queue:ingest`, Redis Streams)·자료구조·발행 순서·LLM 요약 파이프라인을 포함한 전체 규약은 [Redis 계약](redis_contract.md) 참조.
> ⚠️ 여기 `stream:{code}`는 **Pub/Sub 채널**이다. Redis Streams(데이터 구조)는 작업 큐 `queue:ingest` 하나뿐이며 게이트웨이는 그걸 보지 않는다.

| 클라 목적지 | Redis 채널 (구독 대상) | 발행 주체 |
|---|---|---|
| `/user/queue/quote` (해당 종목) | `quote:{code}` | price-worker |
| `/user/queue/stream` (해당 종목) | `stream:{code}` | llm-worker · batch-worker(투자의견) |
| `/topic/rooms/{code}/posts` | `post:{code}` | 메인서버(글 작성 시 발행) |
| `/topic/rooms/{code}/trade` *(선택)* | `trade:{code}` | price-worker |
| `/topic/rooms/{code}/depth` *(선택)* | `depth:{code}` | price-worker |

- 같은 종목을 보는 클라가 N명이어도 게이트웨이는 Redis 채널을 **한 번만 구독**하고 N명에게 fan-out한다(중복 제거).

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
- SimpleBroker는 인스턴스별 인메모리지만 게이트웨이 간 버스는 Redis Pub/Sub이 담당(broadcast-and-filter)하므로 외부 브로커(RabbitMQ relay)는 불필요하다.
- 틱 conflation(100~250ms)은 price-worker 책임이다. 게이트웨이는 전송 제한(`setSendTimeLimit`/`setSendBufferSizeLimit`)으로 느린 클라를 방어한다 — 버퍼 초과 세션은 강제 종료하고 클라가 재연결+REST 복구한다(§6·§7과 일관).
