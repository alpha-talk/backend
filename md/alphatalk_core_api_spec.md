# Alpha Talk — core-api REST API 명세 v0.5
**메인서버(core-api) · 동기 API 전용 · 담당: 민균**

> **v0.5 (2026-08-14)**: **quote 스냅샷에 신선도 검사 추가(§5)** — `price:{code}` 캐시는 TTL이 없어(Redis 계약 §3) 수요가 사라진 종목의 마지막 튜플이 무기한 남는데, 지금까지는 값이 있기만 하면 `delayed:false`로 반환해 **이전 거래일의 등락률이 실시간으로 위장**됐다(방 입장 화면이 낡은 값으로 굳는 실증 버그). `ts`의 KST 날짜가 오늘보다 과거인 캐시는 미스와 동일하게 일봉 폴백(`delayed:true`)으로 강등한다. 판정을 나이(초)가 아니라 거래일로 하는 이유: KIS 실시간은 체결 시에만 틱을 주고 worker-price의 틱 침묵 강등은 최초 무틱 종목에만 적용되므로, 유동성 낮은 종목은 장중에도 캐시가 오래 조용한 것이 정상이다 — 같은 날 안에서는 마지막 체결가가 곧 현재가다. 응답 스키마 불변.

> **v0.4 (2026-08-13)**: **증권사 투자의견 전역 알림 추가(§6)** — 관심목록과 무관하게 모든 유저에게 새 투자의견을 알린다. `GET /notifications/opinions`(최신 의견 피드)·`PUT /notifications/opinions/cursor`(전역 읽음 커서) 신설, 배지 응답에 `opinions` 필드 추가, read-all이 전역 커서도 전진. 원천은 worker-batch가 적재하는 `invest_opinion`(KIS 워커 명세 §4)이고 notification 모듈이 읽기 전용 매핑으로 조회 시점 집계한다(ADR A4 확장 — 유저별 알림 행 없음). 읽음 위치는 유저당 1행 `opinion_read_cursor`(DB 전용, Redis 미러 없음 — 배지 10초 캐시가 비용을 방어).

> **v0.3 (2026-08-06)**: 분봉의 **수집 창이 종목별로 다르다는 사실을 §8에 반영**. NXT 상장 종목은 08:00–20:00(최대 721봉)이지만 미상장 종목은 09:00–15:30(최대 391봉)이다 — KIS가 미상장 종목에 장외 세션 분봉을 주지 않는다(KIS 워커 명세 v0.5 §2.6). 응답 스키마와 버킷 그리드는 그대로고, 클라는 종목에 따라 장외 구간이 비는 것을 **정상**으로 다뤄야 한다.

> **v0.2 (2026-08-04)**: 차트 분봉 조회 추가(§8) — `period`에 `1m|5m|15m|30m|60m` 확장, 원본은 1분봉(`minute_candle`, KIS 워커 명세 v0.3 §2.6)이고 상위 분 단위는 조회 시 파생.

클라이언트가 메인서버(core-api)를 호출할 때 따르는 REST 계약이다. core-api를 구현하거나 클라이언트에서 이 API를 붙일 때 기준으로 삼는다. 실시간 푸시(STOMP) 프로토콜은 WS 명세(ws_api_spec.md), 서비스 간 Redis 계약은 redis_contract.md가 다룬다. 이 문서는 동기 REST만 정의한다.

---

## 0. 개요 & 원칙

- core-api는 **진실의 원천(DB)을 다루는 유일한 클라이언트 대면 동기 API**다. WS는 best-effort 통보일 뿐이다. 영속 상태 변경(글·관심목록·커서)과 과거 조회·복구는 전부 이 API 몫이다 (WS 명세 §8).
- **core-api는 KIS를 직접 호출하지 않는다** (ADR A7). 시세·지표는 워커가 적재한 DB/Redis만 읽는다.
- 모듈은 7개다: `auth` · `search` · `subscription` · `stream` · `notification` · `community` · `stockinfo`. 경계는 Spring Modulith로 강제한다.
- 이 문서의 응답 예시는 대표 케이스다. 필드 정의는 예시 + 필드 표로 확정한다. 전 바디의 기계가독 스키마(OpenAPI)는 구현과 함께 `springdoc`으로 생성해 이 문서와 상호 검증한다.

---

## 1. 공통 규약

### 1.1 기본

| 항목 | 값 |
|---|---|
| Base URL | `/api/v1` |
| 형식 | JSON(UTF-8), 시각은 **epoch ms** (WS 봉투 `ts`와 통일) |
| 종목 코드 | 6자리 문자열 (예: `"005930"`) — 선행 0 보존 위해 항상 문자열 |
| ID | 유저 `number`, 그 외(이벤트·글·댓글) **ULID 문자열** |
| 인증 | `Authorization: Bearer {accessToken}` — §1.2 |

### 1.2 인증 토큰 정책

- 액세스 토큰 30분 / 리프레시 토큰 14일. **리프레시 회전(rotation)**: `/auth/refresh` 시 새 쌍을 발급하고 구 리프레시는 즉시 무효화한다. 재사용을 감지하면 해당 유저의 전 토큰을 무효화한다.
- WS 게이트웨이는 같은 검증 모듈(서명 키 공유)로 CONNECT 헤더의 JWT를 검증한다 — 클라는 같은 액세스 토큰으로 WS도 인증한다.
- 토큰은 응답 바디로 전달한다(포트폴리오 단순화). XSS 대비는 프론트 책임 범위로 문서화한다. 로그에 토큰을 출력하지 않는다.

### 1.3 오류 포맷

```json
{ "error": { "code": "VALIDATION_FAILED", "message": "content는 1~2000자여야 합니다", "detail": { "field": "content" } } }
```

| HTTP | code | 상황 |
|---|---|---|
| 400 | `VALIDATION_FAILED` | 파라미터/바디 검증 실패 |
| 401 | `UNAUTHORIZED` / `TOKEN_EXPIRED` | 미인증 / 액세스 만료(클라: refresh 후 재시도) |
| 403 | `FORBIDDEN` | 타인 리소스 수정 등 |
| 404 | `NOT_FOUND` | 없는 종목/글 |
| 409 | `CONFLICT` / `DUPLICATE` | 중복 이메일, 이미 구독 등 |
| 422 | `LIMIT_EXCEEDED` | 관심목록 한도 초과 등 |
| 429 | `RATE_LIMITED` | §1.5 — `Retry-After` 헤더 포함 |
| 500 | `INTERNAL` | 서버 오류 |

### 1.4 커서 페이지네이션 (공통 규약)

ULID 사전순이 곧 시간순이라는 성질을 이용한 **양방향 커서**다. 스트림·글·알림 목록에 공통 적용한다. WS 푸시는 전달을 보장하지 않으므로(§0) 놓친 구간 복구도 이 커서 규약이 감당한다.

| 파라미터 | 의미 |
|---|---|
| `cursor` | 기준 eventId/ULID (미지정 시 최신부터). 서버가 내려준 **대문자 ULID**를 그대로 돌려준다 — 소문자·다른 형식은 400 |
| `direction` | `before`(과거로, 기본) / `after`(cursor 필수, cursor 이후 → **재접속 복구용**) |
| `limit` | 기본 50, 최대 100 |

응답 공통 `pageInfo`:

```json
{ "items": [ ... ], "pageInfo": { "oldest": "01J8...", "newest": "01J9...", "hasMoreBefore": true, "hasMoreAfter": false } }
```

- `before`: `cursor`보다 작은 ID를 내림차순 limit건. `after`: 큰 ID를 **오름차순** limit건(복구는 오래된 것부터 재생).
- 클라는 `eventId` 기준으로 중복을 제거한다. WS 수신분과 REST 복구분이 겹칠 수 있고 이는 정상이다.

### 1.5 레이트리밋 (쓰기 계열, FR-19)

| 대상 | 한도 | 키 |
|---|---|---|
| 글 작성 | 5회/분 | userId |
| 댓글 작성 | 10회/분 | userId |
| 공감 토글 | 60회/분 | userId |
| 로그인 시도 | 10회/분 | IP+email |

구현: Redis 고정 윈도 카운터(`INCR`+`EXPIRE` Lua 원자화). 초과 시 429 + `Retry-After`. Redis 장애 시에는 요청을 막지 않는다(fail-open — 유량 제한이 가용성보다 우선하지 않음). 로그인 키의 IP는 `remoteAddr` 기준이므로 프록시/LB 뒤 배포 시 `server.forward-headers-strategy` 설정이 전제다(M6 배포 체크리스트).

- **적용 지점은 API 엣지다**: 유량 제한은 유스케이스 로직이 아니라 횡단 관심사이므로 컨트롤러 메서드의 `@RateLimited` 선언으로 걸고 애스펙트가 강제한다 — 서비스 계층에는 유량 코드가 없다. 키는 기본이 인증 유저(`userId`)이고, 로그인처럼 비인증 요청은 애노테이션의 SpEL 식으로 조합한다(IP+email). 위 표의 한도·윈도는 API 계약이므로 애노테이션 상수로 고정하고 환경별 프로퍼티로 두지 않는다.
- 제한은 **시도 기준**이라 인증 실패(로그인 401)와 멱등 재생(§1.6)도 카운트한다. 429는 창이 지나면 자연 해소되고 `Retry-After`가 대기 시간을 알려준다.

### 1.6 멱등성

- 글/댓글 POST는 `Idempotency-Key` 헤더(선택, ULID)를 지원한다: 같은 키로 재요청하면 최초 성공 응답을 재반환한다. 구현은 **DB 원장**(`idempotency_record`)이다 — 본문 검증 통과 후 글·댓글 INSERT와 **같은 트랜잭션**에서 키를 조건부 INSERT(`ON CONFLICT DO NOTHING`)로 선점하고 응답을 함께 영속한다. 커밋과 키 기록이 원자적이라 커밋 직후 프로세스 종료·순차 재시도·병렬 중복(선점 대기 후 재생) 모두에서 중복 생성이 없다. 같은 키를 다른 요청 종류에 쓰면 409. 재생도 하나의 요청이므로 §1.5 유량을 소비한다. 원장 보존 정리는 배치 몫(M6).
- 공감/구독은 PUT/DELETE 의미론이라 자연 멱등이다. 공감 등록·읽음 커서 생성은 조건부 INSERT(`ON CONFLICT DO NOTHING`)로 동시 요청에서도 정확히 한 번만 반영된다.

---

## 2. auth 모듈

계정 생성과 세션 유지 흐름을 받친다 — 가입·로그인·토큰 갱신·로그아웃. 여기서 발급한 토큰이 REST와 WS CONNECT 인증의 원천이다(§1.2).

| 메서드 | 경로 | 인증 | 설명 |
|---|---|---|---|
| POST | `/auth/signup` | — | 이메일 가입 |
| POST | `/auth/login` | — | 토큰 쌍 발급 |
| POST | `/auth/refresh` | — | 리프레시 회전 |
| POST | `/auth/logout` | ✓ | 리프레시 무효화 |
| GET | `/users/me` | ✓ | 내 프로필 |

**POST /auth/signup** — req `{ "email": "a@b.c", "password": "...", "nickname": "민균" }` → 201 `{ "userId": 123 }`
검증: 비밀번호 8자+영/숫자 조합, 닉네임 2~12자. 중복 이메일/닉네임은 409 `DUPLICATE`.

**POST /auth/login** — req `{ "email", "password" }` → 200
```json
{ "accessToken": "eyJ...", "refreshToken": "eyJ...", "accessExpiresIn": 1800 }
```

**POST /auth/refresh** — req `{ "refreshToken" }` → 200 새 토큰 쌍(위와 동일 형태). 재사용 감지 시 401 + 전 세션 무효화.

**GET /users/me** → 200 `{ "userId": 123, "email": "a@b.c", "nickname": "민균", "createdAt": 1719... }`

---

## 3. search 모듈

유저가 종목 방을 찾아 들어가는 입구다 — 종목명/코드 검색·자동완성 (FR-02).

**GET /stocks/search?q={질의}&limit=10** (인증 ✓)

```json
{ "items": [ { "code": "005930", "name": "삼성전자", "market": "KOSPI" },
             { "code": "005935", "name": "삼성전자우", "market": "KOSPI" } ] }
```

- 매칭: 코드 prefix OR 이름 부분일치(`ILIKE` + `pg_trgm` GIN 인덱스). 정렬: 코드 prefix 일치 우선 → 이름 일치 → **규모 내림차순** → 코드.
- 규모 기준(M5부터): **최신 `valuation_daily.market_cap` 내림차순**. 아직 밸류에이션이 미적재인 종목은 `shares_outstanding`(발행주식수) 근사로 폴백한다(nulls last → 폴백 정렬). 정렬 기준은 응답에 드러나지 않으므로 **API 계약은 그대로**다.
- `q`는 1자 이상. 데이터 원천은 batch-worker의 `stock_master`이고 `is_active=true`만 조회한다. Phase 3에서 OpenSearch(형태소/초성)로 승격하되 **API 계약은 불변**.

---

## 4. subscription 모듈 (관심목록)

관심목록이 곧 "내 방 목록"이다. 방 목록 조회(FR-03)와 구독·해지 흐름을 받친다.

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/watchlist?include=price,unread` | 내 방 목록 (FR-03) |
| PUT | `/watchlist/{code}` | 구독 (멱등) |
| DELETE | `/watchlist/{code}` | 구독 해지 (멱등) |

**GET /watchlist?include=price,unread** → 200

```json
{ "items": [ { "code": "005930", "name": "삼성전자", "market": "KOSPI", "subscribedAt": 1719...,
               "price": { "price": 71200, "changeRate": 0.99, "ts": 1719..., "delayed": false },
               "unreadCount": 12 } ] }
```

- `include=price` → Redis `price:{code}` 조회. 미스면 최신 일봉 종가에 `"delayed": true`를 붙인다.
- `include=unread` → notification 모듈에 집계를 위임한다 (§6). 미지정 시 해당 필드를 생략해 응답을 가볍게 유지한다.

> 구현 현황: 목록·구독·해지와 아래 부수효과는 동작한다. `include`는 시세를 가진 stream 모듈(§5)과 미읽음을 가진 notification 모듈(§6)이 붙는 시점에 함께 연다.

**PUT /watchlist/{code}** → 201(신규)/200(기존). 새로 담을 종목이 없거나 상장폐지됐으면 404, 한도 100개를 넘기면 422, 6자리 숫자가 아닌 코드는 400이다. 이미 담긴 종목은 이후 상장폐지됐거나 한도를 채웠어도 200 — 현재 상태를 다시 요청하는 PUT의 멱등성을 유지한다.

**DELETE /watchlist/{code}** → 204. 담은 적 없는 종목이어도 204다.

**한도 검사는 사용자 단위로 직렬화한다.** 99개를 가진 사용자가 서로 다른 두 종목을 동시에 PUT하면, 두 요청 모두 "99개"를 읽고 통과해 101개가 된다. `(user_id, code)` 기본키는 같은 종목의 중복만 막을 뿐 서로 다른 종목끼리의 경쟁은 막지 못한다. 그래서 구독·해지 트랜잭션은 auth 모듈의 `UserAccountLock` 포트로 사용자 엔티티를 `PESSIMISTIC_WRITE` 잠근 뒤 상태 확인과 변경을 끝낸다. subscription은 `users` 테이블이나 auth의 JPA 엔티티를 직접 알지 않는다. 잠글 사용자가 이미 사라졌으면 유효하지 않은 인증 주체이므로 401을 반환한다. 수락된 요청은 멱등 재요청이라도 커밋 전에 사용자별 `watchlist_rev`를 1 올리고 변경 후 전체 스냅샷을 함께 돌려준다 — 잠금이 트랜잭션을 직렬화하므로 **rev 순서가 곧 커밋 순서**다.

**부수효과 (Redis 계약 §1.1·§3)**: 관심목록의 진실은 core-api의 DB지만, 게이트웨이는 CONNECT 때 Redis Set `watchlist:{userId}`만 읽는다. DB 변경과 rev 증가는 서비스 트랜잭션에서 끝내고, **Redis는 트랜잭션과 잠금 밖(커밋 후)에서만 호출한다**(코딩 컨벤션 §6 — DB 트랜잭션 안에서 느린 외부 I/O 금지. Redis 지연이 DB 커넥션·잠금 점유로 전이되지 않는다). 커밋 후 동기화는 (rev, 전체 스냅샷, 이 요청의 변경)을 Lua 스크립트 하나로 원자 반영한다.

1. 미러의 `watchlist:rev:{userId}`보다 새 rev일 때만 Set `watchlist:{userId}`를 스냅샷으로 통째 교체한다 — 오래된 동기화는 폐기되어 최신 상태를 과거로 덮지 못한다
2. 같은 스크립트 안에서 미러 전이 diff에 이 요청의 변경을 합쳐 `PUBLISH watchlist:updated` `{ "userId": 123, "added": ["005930"], "removed": [], "ts": ... }`한다 — 반영과 발행이 원자적이고 스크립트는 Redis에서 직렬화되므로 **발행 순서도 rev 순서와 일치**한다. 폐기된 동기화는 발행도 생략한다(더 새로운 동기화가 그 상태 전이를 이미 발행했다)

이미 접속한 세션은 발행된 diff를 멱등 적용해 구독과 **수요 카운트**를 조정하고, 발행 직후 재접속한 세션도 같은 상태의 Set을 해소한다.

**동기화는 멱등 재요청에도 매번 반복한다.** 멱등 재요청도 rev를 올리므로 동기화가 폐기되지 않고, 미러가 이미 맞아도 이 요청의 diff를 재발행해 접속 중인 게이트웨이까지 복구한다. DB 커밋 뒤 동기화에서 실패하면 요청은 500을 반환하지만 DB 변경은 유지된다 — 클라는 같은 PUT/DELETE를 재요청한다. 최초 PUT이 500 뒤 재요청되면 DB에는 이미 행이 있으므로 재요청의 정상 응답은 200이다.

미러가 통째로 사라져도(Redis 초기화 등) 동기화가 전체 스냅샷을 반영하므로 **그 사용자의 다음 요청 한 번으로 미러 전체가 복구된다**. 사용자 활동 없이 전 사용자를 일괄 재구축하는 경로는 여전히 없다.

---

## 5. stream 모듈 (방의 통합 스트림)

방 화면의 본체다 — 통합 스트림의 과거 조회·놓친 구간 복구(FR-04·05·06)와 입장 직후 시세 스냅샷(FR-08)을 받친다.

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/rooms/{code}/stream` | 통합 스트림 조회·복구 (FR-04·05·06) |
| GET | `/rooms/{code}/quote` | 입장 스냅샷 (FR-08) |

**GET /rooms/{code}/stream?cursor=&direction=before&limit=50&types=news,disclosure,report,ai,post,alert**

```json
{ "items": [
    { "eventId": "01J9Z8...", "code": "005930", "type": "NEWS", "occurredAt": 1719...,
      "source": "hankyung",
      "payload": { "title": "삼성전자 2분기 실적 발표", "summary": "① ... ② ... ③ ...", "sourceUrl": "https://..." } },
    { "eventId": "01J9Z7...", "code": "005930", "type": "POST", "occurredAt": 1719...,
      "source": "user",
      "payload": { "postId": "01J9Z7...", "kind": "post", "author": { "id": 123, "nickname": "민균" },
                    "preview": "이번 실적은...", "likeCount": 3, "commentCount": 1 } }
  ],
  "pageInfo": { "oldest": "01J9Z7...", "newest": "01J9Z8...", "hasMoreBefore": true, "hasMoreAfter": false } }
```

- **없는 종목·상장폐지 종목의 방은 404**다. 정상 종목인데 이벤트가 아직 없으면 빈 목록으로 200을 준다 — 클라가 "그런 방은 없다"와 "아직 소식이 없다"를 구분해야 빈 화면에 무엇을 띄울지 정할 수 있다. 종목 존재 확인은 search가 노출한 `StockCatalog`를 쓴다.
- `types` 미지정 시 전체 타입을 반환한다. `payload`는 타입별 JSON이며 타입 매핑은 기획안 §2.3을 따른다.
- **재접속 복구**: 클라가 마지막 수신 `eventId`로 `direction=after`를 호출하면 놓친 `stream`/`post`를 오름차순으로 메운다 (WS 명세 §6). `hasMoreAfter=true`면 반복 호출한다.
- 인덱스: `(code, event_id DESC)` — p95 300ms 목표(NFR-02).

**GET /rooms/{code}/quote** → 200

```json
{ "code": "005930", "price": 71200, "prevClose": 70500, "change": 700, "changeRate": 0.99,
  "open": 70600, "high": 71500, "low": 70400, "volume": 1234567, "ts": 1719..., "delayed": false }
```

- Redis `price:{code}` Hash를 그대로 매핑한다. 미스(비수요 종목·장전)나 Redis 조회 장애면 최신 일봉 기반에 `"delayed": true`. 필수 필드가 없거나 숫자 형식이 깨진 Hash도 일봉으로 폴백하되 종목코드와 필드명을 warn 로그로 남긴다.
- **신선도 검사(v0.5)**: 캐시에 값이 있어도 `ts`의 KST 날짜가 **오늘보다 과거**면 미스와 동일하게 일봉 폴백한다(낡음은 정상 조건이라 로그를 남기지 않는다). 이전 세션의 튜플은 `prevClose`·`changeRate` 기준이 어긋나 실시간으로 위장되면 안 되지만, **같은 날의 튜플은 나이와 무관하게 유효하다** — 체결이 없던 종목의 마지막 체결가가 곧 현재가이고, 장 마감 후에도 당일 종가를 `delayed:false`로 준다. 수요가 끊긴 뒤 같은 날 안에 남은 캐시는 시세가 수 시간 뒤처질 수 있으나, 방 quote 구독(WS 명세 v0.9 §3.2)이 입장 즉시 수요를 되살려 수 초 내 라이브로 덮인다.
- 일봉 폴백의 `ts`는 해당 거래일 정규장 마감 시각(15:30 KST)이다.
- 실시간도 일봉도 없으면 404다. 이때 없는 종목이면 "존재하지 않는 종목", 정상 종목이면 "시세를 찾을 수 없음"으로 메시지를 나눈다. 존재 확인은 둘 다 없을 때만 하므로 정상 경로에는 질의가 늘지 않는다.
- 상장폐지 종목이라도 남은 일봉이 있으면 200(`delayed: true`)이다. quote는 시세 스냅샷이지 방 존재 판정이 아니다 — 방 존재 판정(없는·상장폐지 종목 404)은 stream 조회(§5 상단)가 담당한다.
- 필드는 WS `quote` data(§WS 4.2)와 동일 명세다 — 클라가 스냅샷→라이브 덮어쓰기를 같은 모델로 처리한다.

---

## 6. notification 모듈 (알림 인박스 · fan-out-on-read)

방 밖의 유저에게 "안 읽은 소식"을 보여주는 흐름을 받친다 — 미읽음 배지(FR-12)·알림 목록·읽음 커서(FR-13).

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/notifications/badge` | 종목별/전체 미읽음 수 (FR-12) |
| GET | `/notifications` | 미읽음 이벤트 통합 목록 |
| PUT | `/rooms/{code}/cursor` | 읽음 커서 전진 (FR-13) |
| POST | `/notifications/read-all` | 모두 읽음 |
| GET | `/notifications/opinions` | 증권사 투자의견 전역 피드 (관심목록 무관, v0.4) |
| PUT | `/notifications/opinions/cursor` | 투자의견 전역 읽음 커서 전진 (v0.4) |

**동작 원리 (ADR A4)**: 쓰기 시점에 유저별 알림 행을 만들지 않는다. `cursor:{userId}:{code}`(Redis, DB `read_cursor` 미러) **이후의 StreamEvent를 조회 시점에 집계**한다.

**GET /notifications/badge** → 200 `{ "total": 27, "byCode": { "005930": 12, "000660": 15 }, "opinions": 3 }`

- 관심 종목마다 `count(event_id > cursor)`를 센다. 종목당 상한 99로 캡(`LIMIT 100` 카운트)해 비용을 고정한다. 결과는 10초 Redis 캐시(`badge:{userId}` — 메인서버 전용 키, Redis 계약 §3 주석). 커서 전진·모두 읽음 시 캐시를 지워 읽음 처리 직후의 배지가 캐시 신선도에 묶이지 않게 한다. 미읽음이 0인 종목은 `byCode`에서 생략한다.
- 커서가 없는 종목(구독 직후 등)은 전부 미읽음으로 센다. 커서 조회는 Redis가 fast path고, 미스·장애 시 DB `read_cursor` 미러에서 읽어 Redis에 되채운다(기획안 §5.4).

**GET /notifications?types=&cursor=&limit=30** → 스트림과 동일한 item 형태에 `code`별 혼합, `eventId` 내림차순. 항목 클릭 시 클라는 `/rooms/{code}` 화면에서 해당 `eventId`로 점프한다.

- 응답 봉투는 §1.4 공통 `pageInfo`와 동일하다. `limit` 기본 30·최대 100. 과거 페이지는 `cursor`(내림차순 `before` 의미)로 넘긴다.

**PUT /rooms/{code}/cursor** — req `{ "lastEventId": "01J9Z8..." }` → 204

- 현재 커서보다 **작은 값(역행)은 무시**한다. DB upsert 후 Redis SET(write-through — DB가 진실, Redis는 fast path). 방 열람 중 주기적/이탈 시 호출한다. `lastEventId`는 ULID 형식을 검증하고, 없는 종목은 404.

**POST /notifications/read-all** → 204 — 관심 종목 각각의 커서를 해당 종목 최신 `eventId`로 옮기고, **투자의견 전역 커서도 최신 의견 `eventId`로 옮긴다**(v0.4).

### 6.1 증권사 투자의견 전역 알림 (v0.4)

새 증권사 투자의견은 **관심목록과 무관하게 모든 유저에게** 알린다. 클라는 `opinions > 0`이면 "새로운 증권사 종목 의견이 나왔어요" 안내를 띄우고, 피드 화면에서 최신 의견을 보여준다.

- **원천**: worker-batch `invest_opinion_sync`가 적재하는 `invest_opinion` 테이블(insert-only, KIS 워커 명세 §3.3·§4). `eventId`는 발행 파이프라인이 부여한 `stream_event_id`(ULID, 시간순)를 그대로 쓴다 — 아직 이벤트가 바인딩되지 않은 행(`stream_event_id IS NULL`)은 피드에 노출하지 않는다. `stream_event`의 `type=REPORT`는 뉴스 파이프라인도 쓸 수 있어 식별자로 삼지 않는다.
- **배지 `opinions`**: 전역 커서 이후의 의견 수. `byCode`/`total`(관심 종목 스코프)과 별도 필드이며 `total`에 합산하지 않는다 — 전체 미읽음은 `total + opinions`. 종목당 캡과 동일하게 99로 캡(`LIMIT 100` 카운트)하고 `badge:{userId}` 10초 캐시에 함께 실린다. **커서가 없는 유저(신규 가입·기능 롤아웃 직후)는 전체 이력이 아니라 최근 24시간(`collected_at` 기준)의 의견만 센다** — 방 커서의 "커서 없음 = 전부 미읽음"과 달리 전역 피드는 이력 전체가 새 알림으로 쏟아지는 것을 막아야 하고, 조회 경로에 커서 초기화 쓰기를 만들지 않기 위해 시간 하한으로 대신한다. 관심 종목에 담긴 종목의 의견은 `byCode`(REPORT 타입)와 `opinions` 양쪽에 잡힐 수 있다 — 서로 다른 화면(방 알림 vs 전역 피드)의 카운트라 중복 합산 문제로 보지 않는다.
- **GET /notifications/opinions?cursor=&limit=30** → 200

  ```json
  {
    "items": [
      { "eventId": "01J9Z8...", "code": "005930", "businessDate": "20260813",
        "brokerCode": "00016", "brokerName": "한국투자증권", "rating": "매수",
        "previousRating": "중립", "targetPrice": 92000, "collectedAt": 1755072000000 }
    ],
    "pageInfo": { "oldest": "01J9Z8...", "newest": "01J9Z8...", "hasMoreBefore": false, "hasMoreAfter": false }
  }
  ```

  읽음 여부와 무관하게 **최신 의견을 `eventId` 내림차순**으로 준다(읽음 커서는 배지 카운트만 제어) — "최신 의견 보여주기"가 목적이라 미읽음 필터를 걸지 않는다. `limit` 기본 30·최대 100, 과거 페이지는 `cursor`(`before` 의미), 봉투는 §1.4 `pageInfo`. `brokerName`·`previousRating`·`targetPrice`는 원천이 비면 null.
- **PUT /notifications/opinions/cursor** — req `{ "lastEventId": "01J9Z8..." }` → 204. 역행 무시·ULID 검증은 방 커서와 동일. 저장은 유저당 1행 `opinion_read_cursor`(DB 전용 — Redis 미러 없음, 커서 읽기는 배지 계산 시 1회뿐이고 10초 캐시 뒤에 있다).

---

## 7. community 모듈 (글·댓글·공감)

방 안의 유저 대화를 받친다 — 글 작성·조회(FR-09·10)와 댓글·공감·신고(FR-18).

| 메서드 | 경로 | 설명 |
|---|---|---|
| POST | `/rooms/{code}/posts` | 글 작성 (FR-09·10) |
| GET | `/rooms/{code}/posts` | 방 글 목록 (커서) |
| GET | `/posts/{postId}` | 글 상세(+댓글 1페이지) |
| PATCH | `/posts/{postId}` | 수정 (작성자) |
| DELETE | `/posts/{postId}` | 소프트 삭제 (작성자) |
| POST | `/posts/{postId}/comments` | 댓글(1뎁스) |
| GET | `/posts/{postId}/comments` | 댓글 목록 (커서) |
| DELETE | `/comments/{commentId}` | 댓글 삭제 |
| PUT / DELETE | `/posts/{postId}/like` | 공감 등록/해제 (멱등) |
| POST | `/posts/{postId}/report` | 신고 (FR-18) |

**POST /rooms/{code}/posts** — req

```json
{ "title": "2분기 실적 감상", "content": "본문 ... (1~2000자)", "quotedEventId": "01J9Z8..." }
```

→ 201 `{ "postId": "01JA0...", "eventId": "01JA0..." }`

**더블라이트 순서 (ADR A6)** — 한 트랜잭션: ① `post` INSERT ② `stream_event`(type=POST, `event_id`=postId 재사용) INSERT → 커밋 → ③ `@TransactionalEventListener(AFTER_COMMIT)`로 `PUBLISH post:{code}` (봉투는 Redis 계약 §1.2). 발행에 실패해도 무시한다 — 클라가 REST 복구로 보강한다(§1.4). `quotedEventId`는 같은 방의 실존 이벤트인지 검증한다.

- 검증: `title` 1~100자, `content` 1~2000자, 댓글 `content` 1~1000자, 신고 `detail` 500자 이하.
- `stream_event`의 POST payload는 §5 예시 형태(`postId`·`kind`·`author`·`preview`(본문 100자)·`likeCount`·`commentCount`)로 **작성 시점 스냅샷**을 저장한다. 이후 공감·댓글 수 변동은 payload에 재반영하지 않는다 — 최신 수치는 글 상세/목록 REST가 진실이다.
- 댓글 발행 봉투: `eventId`=댓글 ULID, `data.kind="comment"`, `data.postId`=댓글 자신의 ULID, `data.parentId`=부모 글 ULID (WS 명세 §4.4의 `parentId` 해석 — 글은 `parentId: null`).
- 방 글 목록(`GET /rooms/{code}/posts`)은 §1.4 커서 규약(`cursor`·`direction`·`limit` 기본 50)을 따르고 item은 `{ postId, code, author, title, preview, likeCount, commentCount, createdAt }`이다. 소프트 삭제된 글은 목록에서 제외한다(삭제 흔적 표시는 스트림의 `deleted` 마킹 몫).
- 댓글 목록(`GET /posts/{postId}/comments?cursor=&direction=&limit=50`)은 §1.4 파라미터를 쓰되 스레드 관행에 맞춰 **기본 방향이 `after`(cursor 미지정 시 가장 오래된 댓글부터 오름차순)**다. `direction=before`는 내림차순 과거 조회. item은 상세 응답의 comments.items와 같고 `commentId` 오름차순이 시간순이다. 51번째 이후 댓글과 놓친 `kind=comment` 푸시(WS 명세 §6)는 이 API의 `cursor=마지막 commentId`로 복구한다. 글 상세의 `comments`는 이 API의 첫 페이지(기본 방향, 50건)와 동일하다.
- PATCH 요청은 `{ "title?", "content?" }` 부분 수정이고 응답은 GET 상세와 같은 형태다. 생략한 필드는 **DB 현재값 기준(coalesce)으로 유지**된다 — 서로 다른 필드를 동시에 수정해도 늦은 쪽이 상대 필드를 옛 값으로 되돌리지 않는다. 삭제된 글의 수정·댓글·공감·신고는 404.

**GET /posts/{postId}** → 200

```json
{ "postId": "01JA0...", "code": "005930", "author": { "id": 123, "nickname": "민균" },
  "title": "...", "content": "...", "likeCount": 3, "likedByMe": true, "commentCount": 2,
  "quoted": { "eventId": "01J9Z8...", "type": "NEWS", "payload": { "title": "...", "sourceUrl": "..." } },
  "comments": { "items": [ { "commentId": "01JA1...", "author": {...}, "content": "...", "createdAt": ... } ],
                 "pageInfo": { ... } },
  "createdAt": 1719..., "updatedAt": null, "deleted": false }
```

- **삭제 정책**: 소프트 삭제다. 스트림에서는 해당 StreamEvent payload에 `"deleted": true`를 마킹한다(재발행 없음 — 클라가 목록에서 "삭제된 글"로 표시). 삭제된 글의 상세는 `deleted: true`에 `title`/`content`를 비워 반환한다(댓글 스레드는 유지). 댓글 작성 시에도 `post:{code}`에 `kind=comment`를 발행한다(같은 봉투).
- 공감: `post_like` upsert + `like_count` 원자 증감. 응답은 204(등록·해제 동일, 멱등). 신고: `{ "reason": "SPAM|ABUSE|MANIPULATION|ETC", "detail?" }` → 201 `{ "reportId": "01JA..." }`. 신고 상태(접수/처리)는 운영 도구 범위다.

---

## 8. stockinfo 모듈 (종목 정보·지표·차트)

방의 정보 탭을 받친다 — 종목 개요·차트(FR-15)·밸류에이션(FR-14)·재무·수급. 원천은 전부 워커가 적재한 테이블이고 core-api는 읽기만 한다(§0).

| 메서드 | 경로 | 원천 (워커) | 설명 |
|---|---|---|---|
| GET | `/stocks/{code}` | batch: `stock_master` | 종목 개요 |
| GET | `/stocks/{code}/candles` | price/batch: `daily_candle`·`minute_candle` | OHLCV (FR-15) |
| GET | `/stocks/{code}/valuation` | batch: `valuation_daily` | PER·PBR 등 (FR-14) |
| GET | `/stocks/{code}/financials` | batch(OpenDART): `financial_summary` | 재무 요약 |
| GET | `/stocks/{code}/investors` | batch: `investor_flow_daily` | 수급 |

**GET /stocks/{code}** → `{ "code", "name", "market", "sector", "sharesOutstanding", "listedAt", "updatedAt" }`

- `sector`는 `sector` 테이블을 조인한 업종 **이름**(미분류면 null), `listedAt`은 `yyyyMMdd` 문자열(null 허용), `updatedAt`은 epoch ms. 상장폐지(`is_active=false`)·미존재 종목은 이 모듈 전 엔드포인트에서 404.

**GET /stocks/{code}/candles?period=D|W|M|1m|5m|15m|30m|60m&count=100&to=20260707** → 200

```json
{ "period": "D", "items": [ { "date": "20260707", "open": 70600, "high": 71500, "low": 70400,
                               "close": 71200, "volume": 12345678, "value": 876543210000 } ],
  "pageInfo": { "hasMoreBefore": true, "nextTo": "20260706" } }
```

- 저장은 **일봉만**(수정주가) 한다. `W`/`M`은 조회 시 일봉을 집계한다(ADR A8: 주=ISO주, 월=역월; open=첫날 시가, close=마지막 종가, high/low=극값, volume=합). `to` 이전 `count`건은 내림차순이 아니라 **오름차순 반환**(차트 라이브러리 관행).
- 미적재 과거 구간은 있는 만큼 반환하고 `hasMoreBefore:false`를 준다(백필은 batch 잡).
- `W`/`M` 버킷의 `date`는 버킷 안 **마지막 거래일**이고 `value`도 합산한다. `count` 기본 100·최대 500. 집계용 일봉 조회는 `count × 버킷당 최대 일수(주 7·월 31)+1`로 상한을 고정하고, 상한에 걸려 잘렸을 수 있는 가장 오래된 버킷은 버린 뒤 `hasMoreBefore:true`로 알린다 — 부분 버킷을 완전한 봉처럼 주지 않기 위해서다.
- **과거 페이지 커서는 `pageInfo.nextTo`다**: `hasMoreBefore=true`면 다음 페이지를 `to=nextTo`로 요청한다. `nextTo`는 가장 오래된 버킷의 **시작일 하루 전**(주=ISO주 월요일−1, 월=1일−1, 일=당일−1)이라 같은 버킷이 다음 페이지에서 부분 재집계되지 않는다. 클라가 `date`(마지막 거래일)−1로 직접 계산하면 W/M에서 같은 주·월이 중복되므로 반드시 `nextTo`를 쓴다. `hasMoreBefore=false`면 `nextTo`는 null.
- **분봉**: `period=1m|5m|15m|30m|60m`. 저장은 **1분봉만**(`minute_candle` — 원시가, 보존 30 달력일, 하루 최대 721봉(08:00~20:00 양끝 포함 — 종목별 수집 창은 아래 참조), 조회·수요된 종목만 쌓임. 워커 명세 v0.5 §2.6) 하고 상위 분 단위는 조회 시 1분봉을 집계한다(D→W/M과 같은 사다리, open/close/high/low/volume/value 규칙 동일). 버킷은 **08:00 기준 고정 그리드**고 일 경계를 넘지 않는다. 그리드는 종목과 무관하게 08:00에서 시작하며, 09:00 정규장 시작은 5/15/30/60분 모든 단위에서 버킷 경계에 정렬된다. 세션 사이 공백(08:50–09:00 등)은 봉이 없어 버킷도 생기지 않는다 — 차트는 거래가 있었던 구간만 이어 그린다. 20:00 마감 행은 5분 이상 단위에서 그날 마지막 그리드 버킷에 합산되고, `1m`에서는 독립 봉이다 — 08:00 그리드에서 20:00은 새 버킷을 열어 봉 하나짜리 꼬리를 만들기 때문이다. NXT 미상장 종목의 마감 행 15:30은 이 예외가 필요 없다(08:00 그리드에서 5/15/30분 단위의 버킷 시작에 정렬되고 60분 단위에서는 15:00 버킷에 들어간다). items에 `"time": "HHmm"`(버킷 시작 시각)이 추가되고 `date`는 해당 거래일이다. D/W/M 응답에는 `time`이 없다.
- **오늘 구간의 신선화**: 분봉 조회는 테이블을 읽기 전에 worker-price의 내부 신선화 API를 호출한다(워커 명세 §2.6 — 멱등 트리거, 응답에 데이터 없음). 완료 또는 **타임아웃**(연결 0.5s + 응답 1.0s) 후 `minute_candle`을 읽는다. 같은 종목의 동시 조회는 트리거를 하나로 합치고, 최근 1초 안에 이미 트리거했으면 건너뛴다 — 워커의 60s 신선 임계 안에서 무의미한 왕복으로 요청 스레드를 점유하지 않는다 — 타임아웃·워커 다운이면 저장분만 반환한다(stale-while-revalidate, 클라 재조회로 수렴). 이 엔드포인트의 분봉 조회는 신선화 대기 때문에 p95 300ms(NFR-02)의 **명시 예외**다(상한은 트리거 타임아웃 1.5s + 조회). 데이터는 언제나 테이블에서만 읽는다 — core-api가 KIS를 직접 호출하지 않는다.
- 분봉의 `to`·`pageInfo.nextTo`는 `yyyyMMddHHmm`이다. `nextTo`는 가장 오래된 버킷 **시작 1분 전**. 집계용 1분봉 조회 상한과 잘렸을 수 있는 가장 오래된 버킷 폐기 규칙은 W/M과 동일하다(버킷당 최대 1분봉 수 = 단위 분수). `count` 기본 100·최대 500도 동일.
- **수집 창은 종목별로 다르다**: NXT 상장 종목은 08:00–20:00(하루 최대 721봉), 미상장 종목은 **09:00–15:30**(최대 391봉)이다 — KIS가 미상장 종목에 장외 세션 분봉을 주지 않는다(워커 명세 §2.6). 어느 쪽인지는 응답에 노출하지 않는다. 클라는 **장외 구간이 비어 있는 것을 정상으로 다뤄야 하고**, 종목별로 첫 봉이 08:00일 수도 09:00일 수도 있다고 가정한다. 상장 여부는 시간에 따라 바뀌므로 클라가 종목별로 캐싱·분기하지 않는다.
- 분봉 데이터가 없는 구간(수집 시작 전·보존 30일 초과)은 있는 만큼 반환하고 `hasMoreBefore:false`를 준다. 콜드 종목의 직전 7영업일은 조회가 신선화를 트리거할 때 워커가 **비동기로** 백필한다(워커 명세 v0.8 §2.6) — 첫 조회 응답은 백필을 기다리지 않으므로 직후 과거 구간이 비어 있는 것은 오류가 아니며, 과거·당일 구간 모두 재조회에서 채워진다.
- 분봉은 **원시가**라 액면분할 등의 직후 일봉(수정주가)과 어긋날 수 있다. 워커가 감지 시 해당 종목 분봉을 삭제·재백필한다(워커 명세 §2.6) — API는 그 사이의 불일치를 보정하지 않는다.
- 진행 중인 현재 분봉은 서버가 만들지 않는다 — 클라가 `/rooms/{code}/quote` 스냅샷과 WS `quote` 라이브를 마지막 봉 위에 얹는다(§5 quote와 동일 모델). 확정 분봉의 지연 상한은 신선화 임계 60s다. NXT 상장 종목은 장외 세션(08:00–08:50 · 15:30–20:00)에도 봉이 쌓여 시세 핀과 차트가 같은 구간을 가리키지만, **미상장 종목은 장외에 봉이 없어 실시간 시세만 움직인다** — 이 구간에서는 WS `quote` 오버레이가 마지막 확정 봉(15:30) 위에 얹힌다.

**GET /stocks/{code}/valuation** → `{ "per": 12.3, "pbr": 1.1, "eps": 5800, "bps": 65000, "marketCap": 4250000, "asOf": "20260706" }` (marketCap 단위 억원 — 프론트 합의)

**GET /stocks/{code}/financials?years=3** → 200

```json
{ "annual": [ { "period": "2025", "revenue": 3020000, "operatingProfit": 350000, "netIncome": 280000,
                "assets": ..., "liabilities": ..., "equity": ..., "source": "DART", "asOf": "20260401" } ],
  "quarterly": [ { "period": "2026Q1", ... } ] }
```

**GET /stocks/{code}/investors?days=20** → `{ "items": [ { "date": "20260706", "individual": -12000, "foreign": 8000, "institution": 4000 } ] }` (순매수, 단위 백만원 — 워커 명세와 합의)

모든 지표 응답에 `asOf` 필수(기획안 데이터 신선도 요구).

- **단위 규약**: 워커는 금액을 원 단위로 적재하고(KIS 워커 명세 §4) API 단위 변환은 core-api 몫이다 — `marketCap`과 재무 금액(`revenue`·`operatingProfit` 등)은 **억원**(1e8로 내림 나눗셈), 수급은 적재 단위 그대로 백만원.
- 재무의 `period`는 연간(`reprt_code=11011`)이 `"2025"`, 분기가 `"2026Q1"`(11013=Q1·11012=Q2·11014=Q3)이다. `years` 기본 3·최대 10 — 최신 `years`개 연도의 연간·분기 행을 준다. `fs_div`는 워커가 연결(CFS) 우선으로 한 행만 적재하므로 응답에 드러내지 않는다. `asOf`는 공시 시각(`disclosed_at`)의 KST 날짜다.
- `investors`는 최신 영업일부터 내림차순, `days` 기본 20·최대 250. 데이터가 없으면 `financials`/`investors`는 빈 배열로 200, 단일 객체인 `valuation`은 404다.

---

## 9. 모듈 간 의존 & 이벤트 (Modulith 경계)

```
community ──(StreamEventAppender 포트)──► stream   # POST 이벤트 기록
subscription ──(WatchlistBroadcaster 포트)──► watchlist:{userId} 미러 + watchlist:updated
subscription/stream ──(StockCatalog 포트)──► search   # 종목 존재 확인·이름 조회
notification ──(읽기)──► stream(이벤트 조회) + subscription(관심목록) + invest_opinion 읽기 매핑(§6.1)
stream/stockinfo ──(읽기)──► Redis price:{code} / 워커 적재 테이블
auth ◄── 전 모듈 (SecurityContext)
```

- `stream_event` 테이블의 논리 소유자는 **stream 모듈**이다. community는 직접 INSERT하지 않고 노출된 `StreamEventAppender`를 호출한다(경계 테스트로 강제). worker-llm과 worker-batch(투자의견)는 별도 프로세스로 같은 테이블에 INSERT한다. 스키마는 `db-migrations` 모듈(Liquibase)이 단일 관리하고 외부 생산자는 `source_key` 멱등 계약을 지킨다.
- **워커 적재 테이블(읽기 전용)의 매핑 소유권**: `stock_master`·`daily_candle`·`valuation_daily`·`financial_summary`·`investor_flow_daily`처럼 워커가 쓰고 core-api는 읽기만 하는 테이블은 core-api 안에 단독 소유 모듈을 두지 않는다 — 읽는 모듈(search·stream·stockinfo)이 각자 `@Immutable` 읽기 전용 매핑을 갖는다(엔티티 이름만 구분). 단일 소유는 DDL의 `db-migrations`뿐이다. 단, **"이 종목이 실재하는가"라는 공용 질문은 search의 `StockCatalog` 포트로 일원화**한다(subscription·stream·notification·community가 사용) — 존재 판정 로직이 모듈마다 갈라지는 것을 막기 위한 유스케이스 포트이며, stockinfo처럼 판정이 아니라 개요·지표 자체가 요구인 모듈은 자기 읽기 매핑을 쓴다. core-api가 쓰는 테이블(users·post·stream_event 등)은 기존대로 소유 모듈의 포트로만 접근한다.
- 채널명·봉투는 `:contracts` 상수만 사용한다(문자열 하드코딩 금지).

## 10. 보안 체크리스트

bcrypt(cost 10+) · JWT HS256(단일 키 공유, 게이트웨이 동일 모듈) · 토큰/앱키 로그 마스킹 · CORS 화이트리스트 · 입력 검증(Bean Validation) · 소유자 검증(글/댓글 수정·삭제) · 커서 등 ULID 형식 검증(정규식) · SQL 파라미터 바인딩만.

## 11. 부록 — core-api 소유 ERD 스케치

```
users(id BIGSERIAL PK, email UQ, password_hash, nickname UQ, created_at)
refresh_tokens(id, user_id FK, token_hash, expires_at, rotated_from NULL)
watchlist(user_id, code, created_at, PK(user_id, code))
stream_event(event_id CHAR(26) PK, code, type, occurred_at, source, source_key TEXT NULL, payload JSONB, created_at)
  -- UNIQUE(source_key) WHERE source_key IS NOT NULL (외부 워커 자연키 멱등)
  -- PARTITION BY RANGE (created_at) 월 단위 · INDEX (code, event_id DESC) · INDEX (code, type, event_id DESC)
post(id CHAR(26) PK, code, author_id FK, title, content, quoted_event_id NULL,
     like_count INT, comment_count INT, created_at, updated_at, deleted_at NULL)
comment(id CHAR(26) PK, post_id FK, author_id FK, content, created_at, deleted_at NULL)
post_like(post_id, user_id, created_at, PK(post_id, user_id))
report(id, target_type, target_id, reporter_id, reason, detail, status, created_at)
read_cursor(user_id, code, last_event_id, updated_at, PK(user_id, code))   -- Redis 미러
opinion_read_cursor(user_id PK, last_event_id, updated_at)   -- 투자의견 전역 커서(§6.1, DB 전용)
idempotency_record(user_id, idem_key CHAR(26), action, response JSONB NULL, created_at,
     PK(user_id, idem_key))   -- §1.6 멱등 원장 (글·댓글 커밋과 동일 트랜잭션)
-- 워커 소유 테이블(stock_master, daily_candle, valuation_daily, investor_flow_daily,
-- financial_summary 등)은 「KIS 수집 워커 명세」 §4 참조. 마이그레이션은 db-migrations 모듈(Liquibase) 단일 관리.
```

### 전체 엔드포인트 요약 (33개)

| 모듈 | 엔드포인트 |
|---|---|
| auth (5) | POST signup·login·refresh·logout, GET /users/me |
| search (1) | GET /stocks/search |
| subscription (3) | GET /watchlist, PUT·DELETE /watchlist/{code} |
| stream (2) | GET /rooms/{code}/stream, GET /rooms/{code}/quote |
| notification (6) | GET badge, GET /notifications, PUT /rooms/{code}/cursor, POST read-all, GET /notifications/opinions, PUT /notifications/opinions/cursor |
| community (11) | posts(작성·목록·상세·수정·삭제=5), comments(POST·GET·DELETE=3), like(PUT·DELETE=2), report(1) |
| stockinfo (5) | GET /stocks/{code} + candles·valuation·financials·investors |

---

*core-api REST API 명세 v0.4 — WS 명세 v0.8·Redis 계약 v0.21·KIS 워커 명세 v0.8과 정합. 봉투/채널 문자열은 `:contracts`가 원천.*
