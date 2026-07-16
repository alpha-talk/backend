# Alpha Talk — core-api REST API 명세 v0.1
**메인서버(core-api) · 동기 API 전용 · 담당: 민균**
 
---

## 0. 개요 & 원칙

- core-api는 **진실의 원천(DB)을 다루는 유일한 클라이언트 대면 동기 API**다. WS는 best-effort 통보이며, 모든 영속 상태 변경(글·관심목록·커서)과 과거 조회·복구는 이 API가 담당한다 (WS 명세 §8).
- **core-api는 KIS를 직접 호출하지 않는다** (ADR A7). 시세·지표는 워커가 적재한 DB/Redis만 읽는다.
- 7개 모듈: `auth` · `search` · `subscription` · `stream` · `notification` · `community` · `stockinfo`. 경계는 Spring Modulith로 강제.
- 이 문서의 응답 예시는 대표 케이스다. 필드 정의는 예시 + 필드 표로 확정하며, 전 바디의 기계가독 스키마(OpenAPI)는 구현과 함께 `springdoc`으로 생성해 이 문서와 상호 검증한다.
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

- 액세스 토큰 30분 / 리프레시 토큰 14일, **리프레시 회전(rotation)**: `/auth/refresh` 시 새 쌍 발급, 구 리프레시 즉시 무효화. 재사용 감지 시 해당 유저 전 토큰 무효화.
- WS 게이트웨이는 같은 검증 모듈(서명 키 공유)로 CONNECT 헤더의 JWT를 검증한다.
- 토큰은 응답 바디로 전달(포트폴리오 단순화). XSS 대비는 프론트 책임 범위로 문서화. 로그에 토큰 출력 금지.
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

ULID 사전순 = 시간순임을 이용한 **양방향 커서**. 스트림·글·알림 목록에 공통 적용.

| 파라미터 | 의미 |
|---|---|
| `cursor` | 기준 eventId/ULID (미지정 시 최신부터) |
| `direction` | `before`(과거로, 기본) / `after`(cursor 이후 → **재접속 복구용**) |
| `limit` | 기본 50, 최대 100 |

응답 공통 `pageInfo`:

```json
{ "items": [ ... ], "pageInfo": { "oldest": "01J8...", "newest": "01J9...", "hasMoreBefore": true, "hasMoreAfter": false } }
```

- `before`: `cursor`보다 작은 ID를 내림차순 limit건. `after`: 큰 ID를 **오름차순** limit건(복구는 오래된 것부터 재생).
- 클라는 `eventId` 기준 중복 제거(WS 수신분과 REST 복구분이 겹칠 수 있음 — 정상).
### 1.5 레이트리밋 (쓰기 계열, FR-19)

| 대상 | 한도 | 키 |
|---|---|---|
| 글 작성 | 5회/분 | userId |
| 댓글 작성 | 10회/분 | userId |
| 공감 토글 | 60회/분 | userId |
| 로그인 시도 | 10회/분 | IP+email |

구현: Redis 고정 윈도 카운터(`INCR`+`EXPIRE`). 초과 시 429 + `Retry-After`.

### 1.6 멱등성

- 글/댓글 POST는 `Idempotency-Key` 헤더(선택, ULID) 지원: 10분 내 동일 키 재요청 시 최초 응답 재반환(Redis 캐시).
- 공감/구독은 PUT/DELETE 의미론으로 자연 멱등.
---

## 2. auth 모듈

| 메서드 | 경로 | 인증 | 설명 |
|---|---|---|---|
| POST | `/auth/signup` | — | 이메일 가입 |
| POST | `/auth/login` | — | 토큰 쌍 발급 |
| POST | `/auth/refresh` | — | 리프레시 회전 |
| POST | `/auth/logout` | ✓ | 리프레시 무효화 |
| GET | `/users/me` | ✓ | 내 프로필 |

**POST /auth/signup** — req `{ "email": "a@b.c", "password": "...", "nickname": "민균" }` → 201 `{ "userId": 123 }`
검증: 비밀번호 8자+영/숫자 조합, 닉네임 2~12자. 중복 이메일/닉네임 409 `DUPLICATE`.

**POST /auth/login** — req `{ "email", "password" }` → 200
```json
{ "accessToken": "eyJ...", "refreshToken": "eyJ...", "accessExpiresIn": 1800 }
```

**POST /auth/refresh** — req `{ "refreshToken" }` → 200 새 토큰 쌍(위와 동일 형태). 재사용 감지 시 401 + 전 세션 무효화.

**GET /users/me** → 200 `{ "userId": 123, "email": "a@b.c", "nickname": "민균", "createdAt": 1719... }`
 
---

## 3. search 모듈

**GET /stocks/search?q={질의}&limit=10** (인증 ✓) — 종목명/코드 검색·자동완성 (FR-02)

```json
{ "items": [ { "code": "005930", "name": "삼성전자", "market": "KOSPI" },
             { "code": "005935", "name": "삼성전자우", "market": "KOSPI" } ] }
```

- 매칭: 코드 prefix OR 이름 부분일치(`ILIKE` + `pg_trgm` GIN 인덱스). 정렬: prefix 일치 우선 → 시총 내림차순.
- `q` 1자 이상. 데이터 원천은 batch-worker의 `stock_master`. Phase 3에서 OpenSearch(형태소/초성)로 승격하되 **API 계약은 불변**.
---

## 4. subscription 모듈 (관심목록)

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

- `include=price` → Redis `price:{code}` 조회, 미스 시 최신 일봉 종가 + `"delayed": true`.
- `include=unread` → notification 모듈 집계 위임 (§5). 미지정 시 해당 필드 생략(가볍게).
  **PUT /watchlist/{code}** → 201(신규)/200(기존). 한도 100개 초과 시 422. 없는 종목 404.
  **DELETE /watchlist/{code}** → 204.

**부수효과(Redis 계약 §1.1)**: 변경 커밋 후 `PUBLISH watchlist:updated` `{ "userId": 123, "added": ["005930"], "removed": [], "ts": ... }` → 게이트웨이가 접속 세션의 서버 해소 구독과 **수요 카운트**를 조정한다.
 
---

## 5. stream 모듈 (방의 통합 스트림)

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

- `types` 미지정 시 전체. `payload`는 §(기획안 2.3) 타입 매핑을 따르는 타입별 JSON.
- **재접속 복구**: 클라가 마지막 수신 `eventId`로 `direction=after` 호출 → 놓친 `stream`/`post`를 오름차순으로 메움 (WS 명세 §6). `hasMoreAfter=true`면 반복 호출.
- 인덱스: `(code, event_id DESC)` — p95 300ms 목표(NFR-02).
  **GET /rooms/{code}/quote** → 200

```json
{ "code": "005930", "price": 71200, "prevClose": 70500, "change": 700, "changeRate": 0.99,
  "open": 70600, "high": 71500, "low": 70400, "volume": 1234567, "ts": 1719..., "delayed": false }
```

- Redis `price:{code}` Hash를 그대로 매핑. 미스(비수요 종목·장전) 시 최신 일봉 기반 + `"delayed": true`.
- 필드는 WS `quote` data(§WS 4.2)와 동일 명세 — 클라가 스냅샷→라이브 덮어쓰기를 동일 모델로 처리.
---

## 6. notification 모듈 (알림 인박스 · fan-out-on-read)

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/notifications/badge` | 종목별/전체 미읽음 수 (FR-12) |
| GET | `/notifications` | 미읽음 이벤트 통합 목록 |
| PUT | `/rooms/{code}/cursor` | 읽음 커서 전진 (FR-13) |
| POST | `/notifications/read-all` | 모두 읽음 |

**동작 원리 (ADR A4)**: 유저별 알림 행을 만들지 않는다. `cursor:{userId}:{code}`(Redis, DB `read_cursor` 미러) **이후의 StreamEvent를 조회 시점에 집계**한다.

**GET /notifications/badge** → 200 `{ "total": 27, "byCode": { "005930": 12, "000660": 15 } }`
- 각 관심 종목에 대해 `count(event_id > cursor)` — 종목당 상한 99로 캡(`LIMIT 100` 카운트)해 비용 고정. 결과 10초 Redis 캐시.
  **GET /notifications?types=&cursor=&limit=30** → 스트림과 동일 item 형태 + `code`별 혼합, `eventId` 내림차순. 항목 클릭 시 클라는 `/rooms/{code}` 화면에서 해당 `eventId`로 점프.

**PUT /rooms/{code}/cursor** — req `{ "lastEventId": "01J9Z8..." }` → 204
- 현재 커서보다 **작은 값(역행)은 무시**. Redis SET + DB upsert(write-through). 방 열람 중 주기적/이탈 시 호출.
  **POST /notifications/read-all** → 204 — 관심 종목 각각 커서를 해당 종목 최신 `eventId`로.

---

## 7. community 모듈 (글·댓글·공감)

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

**더블라이트 순서 (ADR A6)** — 한 트랜잭션: ① `post` INSERT ② `stream_event`(type=POST, `event_id`=postId 재사용) INSERT → 커밋 → ③ `@TransactionalEventListener(AFTER_COMMIT)`로 `PUBLISH post:{code}` (봉투는 Redis 계약 §1.2). 발행 실패는 무시(클라 REST 복구로 보강). `quotedEventId`는 같은 방의 실존 이벤트인지 검증.

**GET /posts/{postId}** → 200

```json
{ "postId": "01JA0...", "code": "005930", "author": { "id": 123, "nickname": "민균" },
  "title": "...", "content": "...", "likeCount": 3, "likedByMe": true, "commentCount": 2,
  "quoted": { "eventId": "01J9Z8...", "type": "NEWS", "payload": { "title": "...", "sourceUrl": "..." } },
  "comments": { "items": [ { "commentId": "01JA1...", "author": {...}, "content": "...", "createdAt": ... } ],
                 "pageInfo": { ... } },
  "createdAt": 1719..., "updatedAt": null, "deleted": false }
```

- **삭제 정책**: 소프트 삭제. 스트림에서는 해당 StreamEvent payload에 `"deleted": true` 마킹(재발행 없음 — 클라가 목록에서 "삭제된 글"로 표시). 댓글 작성 시에도 `post:{code}`에 `kind=comment` 발행(같은 봉투).
- 공감: `post_like` upsert + `like_count` 원자 증감. 신고: `{ "reason": "SPAM|ABUSE|MANIPULATION|ETC", "detail?" }` → 201, 상태(접수/처리)는 운영 도구 범위.
---

## 8. stockinfo 모듈 (종목 정보·지표·차트)

| 메서드 | 경로 | 원천 (워커) | 설명 |
|---|---|---|---|
| GET | `/stocks/{code}` | batch: `stock_master` | 종목 개요 |
| GET | `/stocks/{code}/candles` | price/batch: `daily_candle` | OHLCV (FR-15) |
| GET | `/stocks/{code}/valuation` | batch: `valuation_daily` | PER·PBR 등 (FR-14) |
| GET | `/stocks/{code}/financials` | batch(OpenDART): `financial_summary` | 재무 요약 |
| GET | `/stocks/{code}/investors` | batch: `investor_flow_daily` | 수급 |

**GET /stocks/{code}** → `{ "code", "name", "market", "sector", "sharesOutstanding", "listedAt", "updatedAt" }`

**GET /stocks/{code}/candles?period=D|W|M&count=100&to=20260707** → 200

```json
{ "period": "D", "items": [ { "date": "20260707", "open": 70600, "high": 71500, "low": 70400,
                               "close": 71200, "volume": 12345678, "value": 876543210000 } ],
  "pageInfo": { "hasMoreBefore": true } }
```

- 저장은 **일봉만**(수정주가). `W`/`M`은 조회 시 일봉 집계(ADR A8: 주=ISO주, 월=역월; open=첫날 시가, close=마지막 종가, high/low=극값, volume=합). `to` 이전 `count`건 내림차순 아님 — **오름차순 반환**(차트 라이브러리 관행).
- 미적재 과거 구간은 있는 만큼 반환 + `hasMoreBefore:false` (백필은 batch 잡).
  **GET /stocks/{code}/valuation** → `{ "per": 12.3, "pbr": 1.1, "eps": 5800, "bps": 65000, "marketCap": 4250000, "asOf": "20260706" }` (marketCap 단위 억원 — 프론트 합의)

**GET /stocks/{code}/financials?years=3** → 200

```json
{ "annual": [ { "period": "2025", "revenue": 3020000, "operatingProfit": 350000, "netIncome": 280000,
                "assets": ..., "liabilities": ..., "equity": ..., "source": "DART", "asOf": "20260401" } ],
  "quarterly": [ { "period": "2026Q1", ... } ] }
```

**GET /stocks/{code}/investors?days=20** → `{ "items": [ { "date": "20260706", "individual": -12000, "foreign": 8000, "institution": 4000 } ] }` (순매수, 단위 백만원 — 워커 명세와 합의)

모든 지표 응답에 `asOf` 필수(기획안 데이터 신선도 요구).
 
---

## 9. 모듈 간 의존 & 이벤트 (Modulith 경계)

```
community ──(StreamEventAppender 포트)──► stream   # POST 이벤트 기록
subscription ──(RedisPublisher)──► watchlist:updated
notification ──(읽기)──► stream(이벤트 조회) + subscription(관심목록)
stream/stockinfo ──(읽기)──► Redis price:{code} / 워커 적재 테이블
auth ◄── 전 모듈 (SecurityContext)
```

- `stream_event` 테이블 소유자는 **stream 모듈**. community는 직접 INSERT하지 않고 노출된 `StreamEventAppender`를 호출(경계 테스트로 강제). worker-llm은 별도 프로세스로 같은 테이블에 INSERT — 스키마는 Flyway가 단일 관리.
- 채널명·봉투는 `:contracts` 상수만 사용(문자열 하드코딩 금지).
## 10. 보안 체크리스트

bcrypt(cost 10+) · JWT HS256(단일 키 공유, 게이트웨이 동일 모듈) · 토큰/앱키 로그 마스킹 · CORS 화이트리스트 · 입력 검증(Bean Validation) · 소유자 검증(글/댓글 수정·삭제) · 커서 등 ULID 형식 검증(정규식) · SQL 파라미터 바인딩만.

## 11. 부록 — core-api 소유 ERD 스케치

```
users(id BIGSERIAL PK, email UQ, password_hash, nickname UQ, created_at)
refresh_tokens(id, user_id FK, token_hash, expires_at, rotated_from NULL)
watchlist(user_id, code, created_at, PK(user_id, code))
stream_event(event_id CHAR(26) PK, code, type, occurred_at, source, payload JSONB, created_at)
  -- PARTITION BY RANGE (created_at) 월 단위 · INDEX (code, event_id DESC) · INDEX (code, type, event_id DESC)
post(id CHAR(26) PK, code, author_id FK, title, content, quoted_event_id NULL,
     like_count INT, comment_count INT, created_at, updated_at, deleted_at NULL)
comment(id CHAR(26) PK, post_id FK, author_id FK, content, created_at, deleted_at NULL)
post_like(post_id, user_id, created_at, PK(post_id, user_id))
report(id, target_type, target_id, reporter_id, reason, detail, status, created_at)
read_cursor(user_id, code, last_event_id, updated_at, PK(user_id, code))   -- Redis 미러
-- 워커 소유 테이블(stock_master, daily_candle, valuation_daily, investor_flow_daily,
-- financial_summary 등)은 「KIS 수집 워커 명세」 §4 참조. Flyway는 저장소 단일 관리.
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
