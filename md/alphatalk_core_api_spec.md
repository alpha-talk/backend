# Alpha Talk — core-api REST API 명세 v0.1
**메인서버(core-api) · 동기 API 전용 · 담당: 민균**

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

구현: Redis 고정 윈도 카운터(`INCR`+`EXPIRE`). 초과 시 429 + `Retry-After`.

### 1.6 멱등성

- 글/댓글 POST는 `Idempotency-Key` 헤더(선택, ULID)를 지원한다: 10분 내 같은 키로 재요청하면 최초 응답을 재반환한다(Redis 캐시).
- 공감/구독은 PUT/DELETE 의미론이라 자연 멱등이다.

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
- 규모 기준은 원래 시가총액이지만 `stock_master`에 가격이 없어 지금은 **`shares_outstanding`(발행주식수)로 근사**한다. 발행주식수는 시총과 다르므로 저가·다주식 종목이 고가 우량주보다 앞설 수 있다. `valuation_daily.market_cap`이 들어오는 M5에서 시총으로 교체한다 — 정렬 기준은 응답에 드러나지 않으므로 그때도 **API 계약은 그대로**다.
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

**동작 원리 (ADR A4)**: 쓰기 시점에 유저별 알림 행을 만들지 않는다. `cursor:{userId}:{code}`(Redis, DB `read_cursor` 미러) **이후의 StreamEvent를 조회 시점에 집계**한다.

**GET /notifications/badge** → 200 `{ "total": 27, "byCode": { "005930": 12, "000660": 15 } }`

- 관심 종목마다 `count(event_id > cursor)`를 센다. 종목당 상한 99로 캡(`LIMIT 100` 카운트)해 비용을 고정한다. 결과는 10초 Redis 캐시(`badge:{userId}` — 메인서버 전용 키, Redis 계약 §3 주석). 커서 전진·모두 읽음 시 캐시를 지워 읽음 처리 직후의 배지가 캐시 신선도에 묶이지 않게 한다. 미읽음이 0인 종목은 `byCode`에서 생략한다.
- 커서가 없는 종목(구독 직후 등)은 전부 미읽음으로 센다. 커서 조회는 Redis가 fast path고, 미스·장애 시 DB `read_cursor` 미러에서 읽어 Redis에 되채운다(기획안 §5.4).

**GET /notifications?types=&cursor=&limit=30** → 스트림과 동일한 item 형태에 `code`별 혼합, `eventId` 내림차순. 항목 클릭 시 클라는 `/rooms/{code}` 화면에서 해당 `eventId`로 점프한다.

- 응답 봉투는 §1.4 공통 `pageInfo`와 동일하다. `limit` 기본 30·최대 100. 과거 페이지는 `cursor`(내림차순 `before` 의미)로 넘긴다.

**PUT /rooms/{code}/cursor** — req `{ "lastEventId": "01J9Z8..." }` → 204

- 현재 커서보다 **작은 값(역행)은 무시**한다. DB upsert 후 Redis SET(write-through — DB가 진실, Redis는 fast path). 방 열람 중 주기적/이탈 시 호출한다. `lastEventId`는 ULID 형식을 검증하고, 없는 종목은 404.

**POST /notifications/read-all** → 204 — 관심 종목 각각의 커서를 해당 종목 최신 `eventId`로 옮긴다.

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
- PATCH 요청은 `{ "title?", "content?" }` 부분 수정이고 응답은 GET 상세와 같은 형태다. 삭제된 글의 수정·댓글·공감·신고는 404.

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
| GET | `/stocks/{code}/candles` | price/batch: `daily_candle` | OHLCV (FR-15) |
| GET | `/stocks/{code}/valuation` | batch: `valuation_daily` | PER·PBR 등 (FR-14) |
| GET | `/stocks/{code}/financials` | batch(OpenDART): `financial_summary` | 재무 요약 |
| GET | `/stocks/{code}/investors` | batch: `investor_flow_daily` | 수급 |

**GET /stocks/{code}** → `{ "code", "name", "market", "sector", "sharesOutstanding", "listedAt", "updatedAt" }`

- `sector`는 `sector` 테이블을 조인한 업종 **이름**(미분류면 null), `listedAt`은 `yyyyMMdd` 문자열(null 허용), `updatedAt`은 epoch ms. 상장폐지(`is_active=false`)·미존재 종목은 이 모듈 전 엔드포인트에서 404.

**GET /stocks/{code}/candles?period=D|W|M&count=100&to=20260707** → 200

```json
{ "period": "D", "items": [ { "date": "20260707", "open": 70600, "high": 71500, "low": 70400,
                               "close": 71200, "volume": 12345678, "value": 876543210000 } ],
  "pageInfo": { "hasMoreBefore": true } }
```

- 저장은 **일봉만**(수정주가) 한다. `W`/`M`은 조회 시 일봉을 집계한다(ADR A8: 주=ISO주, 월=역월; open=첫날 시가, close=마지막 종가, high/low=극값, volume=합). `to` 이전 `count`건은 내림차순이 아니라 **오름차순 반환**(차트 라이브러리 관행).
- 미적재 과거 구간은 있는 만큼 반환하고 `hasMoreBefore:false`를 준다(백필은 batch 잡).
- `W`/`M` 버킷의 `date`는 버킷 안 **마지막 거래일**이고 `value`도 합산한다. `count` 기본 100·최대 500. 집계용 일봉 조회는 `count × 버킷당 최대 일수(주 7·월 31)+1`로 상한을 고정하고, 상한에 걸려 잘렸을 수 있는 가장 오래된 버킷은 버린 뒤 `hasMoreBefore:true`로 알린다 — 부분 버킷을 완전한 봉처럼 주지 않기 위해서다.

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
notification ──(읽기)──► stream(이벤트 조회) + subscription(관심목록)
stream/stockinfo ──(읽기)──► Redis price:{code} / 워커 적재 테이블
auth ◄── 전 모듈 (SecurityContext)
```

- `stock_master`를 읽는 **JPA 매핑은 search 모듈이 단독 소유**한다. 관심목록도 스트림도 "이 종목이 실재하는가"를 물어야 하는데, 모듈마다 같은 테이블을 각자 매핑하면 매핑이 갈라진다. search가 `StockCatalog`(존재 확인·이름/시장 조회)를 노출하고 나머지는 이 포트만 쓴다. 검색 질의도 같은 엔티티 위의 JPQL(`ilike`·정렬 case 식)로 구현한다.
- `stream_event` 테이블의 논리 소유자는 **stream 모듈**이다. community는 직접 INSERT하지 않고 노출된 `StreamEventAppender`를 호출한다(경계 테스트로 강제). worker-llm과 worker-batch(투자의견)는 별도 프로세스로 같은 테이블에 INSERT한다. 스키마는 `db-migrations` 모듈(Liquibase)이 단일 관리하고 외부 생산자는 `source_key` 멱등 계약을 지킨다.
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
-- 워커 소유 테이블(stock_master, daily_candle, valuation_daily, investor_flow_daily,
-- financial_summary 등)은 「KIS 수집 워커 명세」 §4 참조. 마이그레이션은 db-migrations 모듈(Liquibase) 단일 관리.
```

### 전체 엔드포인트 요약 (22개)

| 모듈 | 엔드포인트 |
|---|---|
| auth (5) | POST signup·login·refresh·logout, GET /users/me |
| search (1) | GET /stocks/search |
| subscription (3) | GET /watchlist, PUT·DELETE /watchlist/{code} |
| stream (2) | GET /rooms/{code}/stream, GET /rooms/{code}/quote |
| notification (4) | GET badge, GET /notifications, PUT /rooms/{code}/cursor, POST read-all |
| community (9) | posts CRUD(4)+목록, comments(2), like(PUT/DELETE=1), report |
| stockinfo (5) | GET /stocks/{code} + candles·valuation·financials·investors |

---

*core-api REST API 명세 v0.1 — WS 명세 v0.3·Redis 계약 v0.1과 정합. 봉투/채널 문자열은 `:contracts`가 원천.*
