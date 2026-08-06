# Alpha Talk — Redis 계약 (`:contracts`) v0.14

> v0.14 (2026-08-06): 일일 다이제스트 잡 적재를 Redis 단일 실행으로 직렬화한다. `seen:ingest:digest:{code}:{date}`가 없을 때 `XADD queue:ingest`를 먼저 성공시키고 마커를 기록한다. XADD 전 마커를 남겨 프로세스 종료 시 잡을 유실하는 순서는 금지하고, 마커 기록 실패로 생길 수 있는 중복은 llm-worker의 `sourceId` 멱등과 DB 유니크 인덱스가 흡수한다. 기동 보충이 Redis 장애로 불완전하면 최신 예정 실행을 주기적으로 재조정한다(뉴스 워커 명세 §4.1).

> v0.13 (2026-08-04): worker-price 내부 키 2종 추가 — 분봉 신선화의 종목별 인스턴스 간 single-flight 락 `lock:minute-refresh:{code}`(SET NX PX)와 완주 워터마크 `minute:through:{code}:{date}`(TTL 2일). 둘 다 [KIS 워커 명세](alphatalk_kis_worker_spec.md) §2.6이 소유하고 worker-price 전용이며 다른 서버는 접근하지 않는다. `:contracts`의 `Keys.minuteRefreshLock`·`Keys.minuteRefreshWatermark` 생성 함수 사용. 아울러 §1.3의 `rate:kis-rest:{keyId}` 토큰 버킷이 구현됐다 — 시각은 Lua 안에서 Redis `TIME`으로 읽어 인스턴스 시계 오차가 합산 한도를 깨지 않게 한다(**Redis 5+ effects replication 전제**).

게이트웨이 · 워커(price/batch/ingest/llm) · 메인서버가 공유하는 Redis 키/채널/스트림 규약이다. 서버끼리는 코드로 의존하지 않고 이 계약으로만 통신하므로, 채널명·키·봉투 스키마·소유권은 이 문서가 서비스 간 단일 진실이다. 새 채널·키가 필요하면 코드보다 먼저 여기에 합의 내용을 반영한다. 클라이언트 쪽 계약은 별도 문서 몫이다 — 게이트웨이↔클라 STOMP는 WS API 명세, 메인서버↔클라 REST는 core-api 명세가 다룬다.

> v0.12 (2026-08-04): 수요 해시 **쓰기 방식을 mutation별 `HINCRBY`에서 게이트웨이 인메모리 스냅샷 전체 재기록(Lua DEL+HSET 원자)으로 정정** — PR 리뷰 반영. 게이트웨이는 주기(5s)와 0↔1 전이 트리거마다 DemandRegistry 스냅샷을 통째로 기록하므로, mutation 유실로 인한 refcount 드리프트가 다음 주기에 자가 치유되고 인메모리 락 안에서 Redis I/O를 하지 않는다. worker-price가 보는 계약(해시 스키마·TTL·`demand:updated` 전이 발행·`gw:alive` 게이팅)은 v0.11과 동일.
> v0.11 (2026-08-04): [KIS 워커 명세](alphatalk_kis_worker_spec.md) §2.1의 **수요(demand) 신호 계약 병합** — 게이트웨이가 `demand:quote:{gwId}`·`demand:room:{gwId}` 해시에 종목별 refcount를 `HINCRBY ±1`로 유지하고, 종목 참조수 0↔1 전이 시에만 `demand:updated`를 발행한다. `gw:alive:{gwId}`(TTL 15s) 하트비트로 살아있는 게이트웨이를 식별하고, 수요 해시는 하트비트가 TTL 60s로 연장해 죽은 게이트웨이의 수요가 자가 소멸한다. worker-price는 `demand:updated` 수신 시 즉시 + 60초 주기로 전체 리컨실(alive gw 합산)한다. §1.3·§3·§4 참조.
> v0.10 (2026-08-03): `cursor:{userId}:{code}`에 **TTL 1일** 부여 — DB `read_cursor`가 진실이고 Redis는 미러 캐시이므로, 미러 쓰기 실패·커밋 직후 종료로 stale해진 키가 영구히 DB 폴백을 가리는 문제를 TTL 만료로 자가 치유한다. 만료 후 조회는 DB에서 읽어 재적재(TTL 갱신)한다.
> v0.9 (2026-08-02): 메인서버 전용 키에 `badge:{userId}`(미읽음 배지 집계 캐시, TTL 10s) 추가 — 서비스 간 계약이 아니며 게이트웨이·워커는 접근하지 않는다. `cursor:{userId}:{code}` 쓰기 주체를 core-api notification 모듈로 확정(전진 전용 — 역행 값은 Lua 비교로 폐기, 기록 값은 DB `read_cursor`의 최종 커서). `idem:{userId}:{key}` Redis 캐시는 **제거** — 멱등 응답은 DB 원장 `idempotency_record`가 커밋과 동일 트랜잭션으로 영속한다(core-api 명세 §1.6). 메인서버 전용 키(`rl:*`·`badge:*`)도 `:contracts`의 `Keys` 생성 함수로 고정한다.
> v0.8 (2026-07-31): `watchlist:rev:{userId}` 키 추가 — 메인서버 전용, 미러 동기화의 최신성 판정(rev)에 사용하며 게이트웨이는 읽지 않는다. 메인서버의 미러 교체와 `watchlist:updated` 발행은 하나의 Lua 스크립트에서 원자적으로 수행되어, 오래된 동기화는 폐기되고 발행 순서가 rev(커밋) 순서를 따른다.
> v0.7 (2026-07-27): 증권사 투자의견 직접 발행 반영([KIS 워커 명세](alphatalk_kis_worker_spec.md) §3.3) — **batch-worker를 `stream:{code}` 공동 발행자로 추가**. DB에는 멱등 저장하고 같은 `eventId`로 Pub/Sub 통보를 재시도한다. 실시간 전달은 best-effort이며 REST가 복구를 담당한다. price·batch가 공유하는 KIS 계정의 합산 유량은 `rate:kis-rest:{keyId}`로 제한한다.
> v0.6 (2026-07-26): 수집 측 텍스트 매칭 완전 제거 — 종목명 사전·매크로 키워드 매핑 코드를 걷어내고 `codes`는 소스가 아는 후보(네이버 쿼리·DART 등)만 싣는다. `macroHint`는 예약 필드로 유지하되 수집기가 더 이상 적재하지 않는다(소비 측은 원래 미사용).
> v0.5 (2026-07-24): 전량 LLM 판정 전환 — `queue:ingest`의 `codes`를 `macroHint` 없이도 공란 허용(수집 측 종목 매칭 게이트 제거). 관련 종목 판정은 llm-worker LLM 전담([뉴스 워커 명세](alphatalk_news_worker_spec.md) §2.4). `digest` 엔트리만 `codes` 1개 필수 유지(§2.3).
> v0.4 (2026-07-23): llm-worker 원문 fetch의 인스턴스 간 호스트별 요청 간격을 위한 `rate:article-fetch:{host}` 키 추가. PEL 회수 설명을 실제 구현인 `XPENDING` + `XCLAIM`으로 정정.
> v0.3 (2026-07-22): 뉴스 파이프라인 반영([뉴스 워커 명세](alphatalk_news_worker_spec.md)) — `queue:ingest`에 `type="digest"`(§2.3)·`macroHint` 필드·`codes` 공란 허용, poison 격리 `queue:ingest:dlq`, `lock:cluster:{code}` 키 추가.
> v0.2 (2026-07-16): `watchlist:{userId}` 미러 키 명문화(ws `RedisWatchlistResolver`·core-api 쓰기 반영), 메인서버 전용 키(`rl:*`·`idem:*`) 주석 추가.
 
---

## 0. 가장 먼저 — 세 가지 메커니즘 구분 (혼동 방지)

Redis를 세 가지 용도로 쓴다. **이름이 비슷해도 메커니즘이 다르다.** 특히 "Streams"와 채널명 "stream"이 헷갈리므로 여기서 못 박는다.

**핵심 경계**: "**처리해야 할 일(work)**"은 Streams로 안전 분배하고 "**이미 처리된 결과의 실시간 통보**"는 Pub/Sub으로 브로드캐스트한다. 일은 유실하면 안 되므로 ACK·재시도가 있는 큐를 쓴다. 통보는 놓쳐도 클라가 REST로 복구하므로 휘발 채널로 충분하다.

| 메커니즘 | 무엇 | 키/채널 패턴 | 보장 | 누가 봄 |
|---|---|---|---|---|
| **Pub/Sub** | 실시간 1→N 브로드캐스트 | `quote:{code}` · `stream:{code}` · `post:{code}` · `watchlist:updated` · `demand:updated` | 구독한 **전원이 사본** · 휘발 · ACK 없음 · best-effort | 게이트웨이가 구독 (`demand:updated`만 반대로 worker-price가 구독) |
| **Redis Streams** | 신뢰성 **작업 큐** | `queue:ingest` (+DLQ `queue:ingest:dlq`) | **경쟁 소비**(한 건=한 워커) · ACK · 재시도(PEL) | 워커끼리만 |
| **자료구조** | 상태/캐시 | `price:{code}` · `presence:{userId}` · `cursor:*` · `seen:ingest:*` · `lock:cluster:*` · `rate:article-fetch:*` · `rate:kis-rest:*` · `demand:*:{gwId}` · `gw:alive:{gwId}` | 영속(메모리) · TTL | 워커/메인/게이트웨이 |

> ⚠️ **`stream:{code}`는 Pub/Sub 채널이다 — Redis Stream(데이터 구조)이 아니다.**
> 이 시스템에서 진짜 Redis Stream은 **`queue:ingest`(와 그 DLQ `queue:ingest:dlq`)뿐**이다.
> *(이름 충돌이 계속 헷갈리면 `stream:{code}` → `feed:{code}` 리네임을 권장. 이 문서는 일단 `stream`을 유지한다.)*

---

## 1. Pub/Sub 채널 — 실시간 (게이트웨이 업스트림)

게이트웨이가 구독해 클라로 fan-out하는 채널이다. 모두 **best-effort 브로드캐스트**다. 유실을 감수할 수 있는 이유는 영속이 필요한 것을 발행자가 **발행 전에 이미 DB에 저장**해 두기 때문이다(persist-then-publish). 놓친 클라는 재접속 후 REST로 복구한다.

### 1.1 채널 목록

| 채널 | 발행자 | 구독자 | 범위 | 영속화 | 비고 |
|---|---|---|---|---|---|
| `quote:{code}` | price-worker | 게이트웨이 | 종목 | ✗ (휘발) | 100~250ms conflation된 최신가 |
| `stream:{code}` | llm-worker · **batch-worker**(투자의견 한정, v0.7 — KIS 워커 명세 §3.3) | 게이트웨이 | 종목 | ✓ (발행 전 저장) | 소식(뉴스/리포트/AI/투자의견) · `eventId` ULID |
| `post:{code}` | 메인서버 | 게이트웨이 | 종목 | ✓ (발행 전 저장) | 글/댓글 · `eventId` ULID |
| `watchlist:updated` | 메인서버 | **모든** 게이트웨이 | 전역(단일 채널) | — | 관심목록 변경 통보 → 게이트웨이가 세션 구독 조정 |
| `trade:{code}` *(선택)* | price-worker | 게이트웨이 | 종목 | ✗ | 체결 · 보는 방만 |
| `depth:{code}` *(선택)* | price-worker | 게이트웨이 | 종목 | ✗ | 호가 · 보는 방만 |

- `{code}` = 종목코드(예: `005930`). **유저로 키하지 않는다** — 채널 수는 종목 수(~2,600)로 고정되고 유저 수와 무관하다.
- 같은 종목을 N명이 봐도 게이트웨이는 채널을 **한 번만 구독**하고 N명에게 fan-out한다(중복 제거).
- 유저별 채널을 만들면 채널이 폭증하므로 `watchlist:updated`만 **단일 전역 채널 + broadcast-and-filter**다. 발행 빈도가 낮아 전원 수신·필터로 충분하다.
- 게이트웨이는 `watchlist:updated`의 diff를 멱등 적용한다. 이미 들어 있는 코드의 `added`와 들어 있지 않은 코드의 `removed`를 허용하며, 이런 중복 diff는 세션 구독이나 수요 refcount를 중복 변경하지 않는다.

### 1.2 메시지 스키마 (Pub/Sub payload)

게이트웨이가 받아 거의 그대로 클라로 relay하는 공통 봉투다(클라 측 스키마는 WS API 명세 §4와 동일).

```json
{ "type": "quote|stream|post|trade|depth", "code": "005930", "eventId": "01J...", "ts": 1719600000000, "data": { } }
```

- `eventId`: ULID. **stream·post는 필수**(순서·중복제거). quote/trade/depth는 선택(스냅샷).
- 타입별 `data`는 WS API 명세 §4.2~4.5와 동일.
  `watchlist:updated` payload (예외 — 봉투 아님):
```json
{ "userId": 123, "added": ["005930"], "removed": ["000660"], "ts": 1719600000000 }
```

### 1.3 수요 신호 채널 `demand:updated` — 게이트웨이 → worker-price (v0.11)

§1.1과 방향이 반대다: **게이트웨이가 발행하고 worker-price가 구독한다.** WS 구독 용량이 유한하므로 worker-price는 전 종목이 아니라 수요가 있는 종목만 KIS에 구독한다. 수요 = ⋃(접속 중 유저의 관심목록) ∪ (입장 중인 방) — 이를 모두 아는 쪽은 게이트웨이라서 카운트는 게이트웨이가 유지하고(§3의 `demand:*:{gwId}` 해시) worker-price는 소비만 한다.

```json
{ "kind": "quote|room", "code": "005930", "active": true, "ts": 1719600000000 }
```

- 게이트웨이는 종목 참조수가 **0↔1 전이할 때만** 발행한다(모든 증감마다 발행하지 않는다).
- best-effort다. worker-price는 이 메시지를 **리컨실 트리거**로만 쓰고, 목표 종목 집합은 항상 §3의 해시 합산으로 재계산한다(payload의 `active`를 단독 신뢰해 즉시 해제하지 않는다). 유실은 60초 주기 전체 리컨실이 자기치유한다.
- 증감 시점: CONNECT 시 관심목록 해소분 +1씩 / DISCONNECT −1씩 / `watchlist:updated` 반영 시 ± / 방 토픽 SUBSCRIBE·UNSUBSCRIBE 시 room ±.

---

## 2. Redis Streams — 작업 큐 `queue:ingest` (워커 간)

**소식(뉴스/리포트) 수집·LLM 가공 파이프라인의 입력 큐다.** 이 경로는 외부 API·LLM 타임아웃으로 처리가 실패할 수 있어 재시도가 필요하고 워커도 여러 대로 늘려 경쟁 소비해야 한다. 그래서 Pub/Sub이 아니라 Streams를 쓴다.

| 항목 | 값 |
|---|---|
| 키 | `queue:ingest` |
| 적재 | `XADD` (생산자: **ingest-worker**) |
| 소비 | `XREADGROUP` (소비자 그룹 **`g:llm`**, 멤버: **llm-worker** ×N) |
| 완료 | `XACK queue:ingest g:llm <id>` |
| 장애 회수 | `XPENDING`으로 idle 임계 초과 엔트리를 찾고 `XCLAIM`으로 다른 워커가 회수 |
| 트림 | `XADD ... MAXLEN ~ N` 또는 주기적 `XTRIM` (이미 처리된 건은 DB에 있으므로 큐는 유한 보관) |
| poison 격리 | delivery count > 5 엔트리는 llm-worker가 `queue:ingest:dlq`로 XADD 후 원큐 XACK — DLQ는 소비자 없음(수동 점검 + 알람) |

### 2.1 큐 엔트리 스키마 (ingest-worker가 XADD)

Streams 필드는 문자열이다. 한 엔트리 = "가공해야 할 원본 소식 1건".

| 필드 | 예 | 설명 |
|---|---|---|
| `source` | `"naver"` `"hankyung"` `"dart"` | 출처 |
| `sourceId` | `"a1b2c3"` | 출처 고유 ID — **중복 제거 키** |
| `type` | `"news"` `"report"` `"disclosure"` `"digest"` | 원본 종류 — `digest`는 일일 브리핑 잡(§2.3) |
| `codes` | `"005930,000660"` | 영향 종목 **후보**(콤마구분, 다중 가능). **공란 허용** — 소스가 종목을 아는 경우(네이버 쿼리·DART)만 싣고 텍스트 사전 매칭은 하지 않는다(v0.6). 관련 종목 확정·발견은 llm-worker LLM이 전담(뉴스 워커 명세 §2.4·§3.6). `digest` 타입만 1개 필수(§2.3) |
| `title` | `"..."` | 원문 제목 |
| `url` | `"https://..."` | 원문 링크 |
| `body` | `"..."` | 원문 본문/발췌(선택) |
| `fetchedAt` | `1719500000000` | 수집 시각(epoch ms) |
| `macroHint` *(예약)* | `"금리"` | v0.6부터 수집기가 적재하지 않는 예약 필드 — 소비 측 파서는 하위 호환으로 계속 허용한다 |

### 2.2 llm-worker 처리 순서 (★ persist → publish → ack)

소비자가 한 엔트리를 처리하는 순서는 안전성의 핵심이다.

```
1. XREADGROUP 으로 엔트리 1건 수신 (g:llm — 한 건은 한 워커만)
2. 멱등 체크: sourceId 기준 (seen:ingest:{sourceId} 또는 upsert로 흡수)
3. LLM 호출: 요약(summary) + 분류(category) → StreamEvent 생성 (eventId = ULID)
4. ① Postgres 저장        (persist — 진실의 원천)
5. ② PUBLISH stream:{code} (영향 종목 each — 실시간 통보, 실패해도 REST 복구)
6. ③ XACK queue:ingest g:llm <id>   (저장+발행 성공 후에만 완료 확정)
```

- **순서 불변식**: 저장(4) → 발행(5) → ACK(6). 4~5 사이/5~6 사이에서 워커가 죽으면 **XACK가 안 됐으니 엔트리는 PEL에 남아 재처리**된다 → 유실 없음.
- 재처리되므로 **멱등 필수**: `sourceId`로 upsert(같은 소식 두 번 처리해도 StreamEvent 중복 생성 안 됨).
- `codes`가 다중이면 StreamEvent를 종목별로 저장하고 `stream:{code}`를 **각 종목에 발행**(한 엔트리 → 여러 채널 fan-out).
- **순서 의미**: 큐 처리는 워커 간 동시 진행이라 순서 비결정적이다. 방 안에서의 최종 순서는 **처리 시점에 부여한 `eventId`(ULID) 오름차순**으로 잡는다(큐 순서가 아님).
> `post`(글/댓글)는 즉시 처리라 작업 큐가 없다 — 메인서버가 저장 후 바로 `post:{code}` 발행. `quote`(틱)도 큐 없이 price-worker가 바로 `quote:{code}` 발행. **Streams를 타는 건 소식(ingest→llm) 경로뿐이다.**

### 2.3 `type="digest"` 엔트리 — 일일 브리핑 잡

ingest-worker 스케줄러(싱글턴)가 매일 18:00 KST에 적재하고 같은 그룹 `g:llm`이 경쟁 소비한다 — 스케줄은 싱글턴, 실행은 ×N(리더 선출 불요).

적재는 `seen:ingest:{sourceId}` 확인 → `XADD queue:ingest` → 마커 기록을 Redis 단일 실행으로 직렬화한다. XADD보다 마커를 먼저 기록하지 않는다. XADD 실패에는 마커가 남지 않아 재조정할 수 있고, 마커 기록 실패 뒤 중복 XADD는 소비 측 멱등으로 흡수한다. 이 단일 실행은 `seen:ingest:*`와 `queue:ingest` 두 키를 한 스크립트에서 만지므로 **단일 Redis(비클러스터) 전제**다 — 클러스터 전환 시 CROSSSLOT으로 깨지며, 해시 태그로 같은 슬롯을 보장하거나 2단계 적재로 되돌리고 유실 창을 다시 검토해야 한다.

| 필드 | 값 |
|---|---|
| `type` | `"digest"` |
| `sourceId` | `digest:{code}:{yyyy-MM-dd}` — 멱등 키(종목·일자당 브리핑 1건) |
| `codes` | 대상 종목 1개 |
| `source` | `"scheduler"` — `title`/`url`/`body` 공란 |

처리 순서·불변식은 §2.2와 동일(persist → publish → ack). 생성물은 `type=AI` StreamEvent(뉴스 워커 명세 §4).

> 증권사 **투자의견**은 이 큐를 타지 않는다 — batch-worker가 `stream_event` 저장 후 `stream:{code}`를 직접 발행한다(§1.1 공동 발행자, v0.7). DB 멱등 저장·같은 `eventId` 재시도·REST 복구 상세는 [KIS 워커 명세](alphatalk_kis_worker_spec.md) §3.3이 소유.
 
---

## 3. 자료구조 — 상태/캐시

게이트웨이의 실시간 계약은 아니지만 서비스들이 공유하는 상태 키다.

| 키 | 타입 | 용도 | 쓰기 | 읽기 | TTL |
|---|---|---|---|---|---|
| `price:{code}` | Hash/String | 현재가 last-value 캐시(스냅샷) | price-worker | 메인서버(REST 현재가) | 갱신/없음 |
| `presence:{userId}` | Set(+멤버 TTL) | 접속 세션 집합(멀티디바이스) | 게이트웨이 | 게이트웨이 | 하트비트로 갱신 |
| `cursor:{userId}:{code}` | String | 마지막 읽은 `eventId`(읽음 위치) — **fast path 미러**, 진실은 DB `read_cursor` | 메인서버(REST) | 메인서버 | **1일** — 쓰기·재적재 시 갱신. 미러 SET 실패로 stale해져도 TTL 만료 후 DB에서 재적재되어 자가 치유 |
| `watchlist:{userId}` | Set | 관심목록 미러 — 게이트웨이 CONNECT 시 해소용 (진실은 메인서버 DB) | 메인서버 | 게이트웨이 | 없음 |
| `watchlist:rev:{userId}` | String | 미러 최신성 판정 rev — 이보다 새 rev의 동기화만 미러를 교체·발행 (메인서버 전용) | 메인서버 | 메인서버 | 없음 |
| `seen:ingest:{sourceId}` | String | 적재 중복 제거 마커 — 기사 sourceId뿐 아니라 일일 다이제스트 잡(`digest:{code}:{date}`)도 같은 마커로 하루 1회를 고정한다(뉴스 워커 명세 §4.1) | ingest/llm-worker | ingest/llm-worker | 며칠 |
| `lock:cluster:{code}` | String (`SET NX PX 3000`) | 뉴스 클러스터 판정 직렬화 락(뉴스 워커 명세 §3.3) | llm-worker | llm-worker | 3초 |
| `rate:article-fetch:{host}` | String (`SET PX`) | robots.txt·원문 fetch의 호스트별 다음 요청 간격을 llm-worker 인스턴스 간 직렬화 | llm-worker | llm-worker | 요청 간격(기본 1초) |
| `rate:kis-rest:{keyId}` | Hash(token bucket) | 같은 KIS 계정을 쓰는 price·batch 프로세스의 일반 REST 합산 유량 제한 | price/batch-worker | price/batch-worker | 마지막 소비 후 2분 |
| `lock:minute-refresh:{code}` | String (`SET NX PX`) | 분봉 신선화의 종목별 인스턴스 간 single-flight 락(KIS 워커 명세 §2.6) — 미획득 인스턴스는 no-op | worker-price | worker-price | 페치 데드라인+여유 (기본 90s·일 확정 시 200s) |
| `minute:through:{code}:{date}` | String (`HHmm`) | 그 종목·일자를 몇 시까지 조회 완료했는지(완주 워터마크, §2.6) — 인스턴스 간 공유해 재기동·리더 전환 후 전 구간 재조회를 막는다 | worker-price | worker-price | **2일** |
| `demand:quote:{gwId}` | Hash `{code: refCount}` | 접속 세션의 관심목록 기준 종목 참조 수(유저 단위) — 주기·전이 트리거마다 스냅샷 전체 재기록(v0.12) | 게이트웨이 | worker-price | **60s** — 재기록이 연장 |
| `demand:room:{gwId}` | Hash `{code: refCount}` | 방 토픽 구독(입장) 기준 참조 수(구독 단위) — trade/depth·우선순위 판단 | 게이트웨이 | worker-price | **60s** — 재기록이 연장 |
| `gw:alive:{gwId}` | String | 살아있는 게이트웨이 식별(하트비트 5s 주기 갱신). worker-price는 리컨실 때 alive gw의 수요만 합산 | 게이트웨이 | worker-price | **15s** |

- **`{gwId}`는 게이트웨이 부팅마다 새로 발급**한다(인메모리 수요 인덱스가 0에서 재구축되는 것과 정합). 이전 부팅의 해시는 하트비트가 끊겨 TTL로 자가 소멸하고, worker-price는 `gw:alive` 없는 gwId를 합산에서 제외하므로 TTL 만료 전에도 무해하다. graceful shutdown 시 게이트웨이는 자기 키 3개를 즉시 DEL한다.
- **현재가 스냅샷**은 REST(메인서버가 `price:{code}` 읽기)로 준다. 게이트웨이는 `quote:{code}` 라이브만 relay하고 캐시를 직접 읽지 않는다(얇은 엣지 유지).
- 봉(OHLCV)은 KIS에서 받아 캐시하되, 권위 있는 가격 저장소로 쓰지 않는다(틱은 영속화 안 함).
- 메인서버 **전용** 키(서비스 간 계약 아님 — 게이트웨이·워커는 접근 금지): `rl:{action}:{key}:{windowIndex}`(레이트리밋 고정 윈도 카운터, TTL=윈도), `badge:{userId}`(미읽음 배지 집계 캐시, TTL 10초 — 커서 전진·모두 읽음 시 DEL). 둘 다 `:contracts` `Keys` 생성 함수 사용. 멱등 응답은 Redis가 아니라 DB 원장(`idempotency_record`)이다 — core-api 명세 §1.6.

---

## 4. 생산자 / 소비자 매트릭스

| 키 · 채널 | price-worker | batch-worker | ingest-worker | llm-worker | 메인서버 | 게이트웨이 |
|---|---|---|---|---|---|---|
| `quote:{code}` (P/S) | **PUBLISH** | — | — | — | — | **SUBSCRIBE** |
| `stream:{code}` (P/S) | — | **PUBLISH**(투자의견) | — | **PUBLISH** | — | **SUBSCRIBE** |
| `post:{code}` (P/S) | — | — | — | — | **PUBLISH** | **SUBSCRIBE** |
| `watchlist:updated` (P/S) | — | — | — | — | **PUBLISH** | **SUBSCRIBE** |
| `trade/depth:{code}` (P/S) | **PUBLISH** | — | — | — | — | **SUBSCRIBE** |
| `queue:ingest` (Stream) | — | — | **XADD** | **XREADGROUP/XACK** (`g:llm`) | — | — |
| `queue:ingest:dlq` (Stream) | — | — | — | **XADD** (poison 격리) | — | — |
| `price:{code}` (자료구조) | **WRITE** | — | — | — | READ | — |
| `presence:{userId}` (자료구조) | — | — | — | — | — | **WRITE/READ** |
| `cursor:{userId}:{code}` | — | — | — | — | **WRITE/READ** | — |
| `watchlist:{userId}` (자료구조) | — | — | — | — | **WRITE** | **READ** |
| `watchlist:rev:{userId}` (자료구조) | — | — | — | — | **WRITE/READ** | — |
| `seen:ingest:{sourceId}` | — | — | WRITE | WRITE/READ | — | — |
| `lock:cluster:{code}` | — | — | — | **WRITE/READ** | — | — |
| `rate:article-fetch:{host}` | — | — | — | **WRITE/READ** | — | — |
| `rate:kis-rest:{keyId}` | **WRITE/READ** | **WRITE/READ** | — | — | — | — |
| `demand:updated` (P/S) | **SUBSCRIBE** | — | — | — | — | **PUBLISH** |
| `demand:quote/room:{gwId}` (자료구조) | READ | — | — | — | — | **WRITE** |
| `gw:alive:{gwId}` (자료구조) | READ | — | — | — | — | **WRITE** |

게이트웨이는 **Pub/Sub SUBSCRIBE만** 한다(+프레즌스·수요 쓰기, `demand:updated` 발행). Streams·DB 쓰기는 만지지 않는다.
 
---

## 5. 전체 파이프라인 — 데이터 타입별 (Streams는 어디에?)

Streams를 타는 경로는 뉴스 기반 소식(stream) 하나뿐이다. 틱·글·투자의견은 큐를 거치지 않는다.

```
틱(quote) ─ 큐 없음:
  KIS WS → price-worker → [자료구조] price:{code} 캐시 갱신
                        → PUBLISH [P/S] quote:{code} → 게이트웨이 → /user/queue/quote
 
뉴스 기반 소식(stream) ─ Streams 작업 큐 사용:
  뉴스/리포트 → ingest-worker → XADD [STREAM] queue:ingest
                                     │ XREADGROUP (g:llm)
                                     ▼
                                  llm-worker  ── LLM 요약·분류 → StreamEvent(ULID)
                                     ├─ ① 저장 [DB]
                                     ├─ ② PUBLISH [P/S] stream:{code} → 게이트웨이 → /user/queue/stream
                                     └─ ③ XACK
글(post) ─ 큐 없음:
  클라 →(REST)→ 메인서버 ── ① 저장 [DB]
                          └─ ② PUBLISH [P/S] post:{code} → 게이트웨이 → /topic/rooms/{code}/posts
투자의견(opinion) ─ 큐 없음 (v0.7):
  KIS REST → batch-worker ── ① 저장 [DB] invest_opinion + stream_event
                           └─ ② PUBLISH [P/S] stream:{code} → 게이트웨이 → /user/queue/stream
                              (실패 시 published_at 미마킹 → 다음 회차에 같은 eventId로 재시도)
```

- `[STREAM]`은 `queue:ingest` 한 곳뿐. `[P/S]`(`quote/stream/post:{code}`)는 전부 Pub/Sub.
- 뉴스 기반 소식은 **Streams(입력 큐)를 통과한 뒤 Pub/Sub(`stream:{code}`)으로 빠져나간다.** 투자의견은 worker-batch가 DB에 영속한 뒤 Pub/Sub으로 직접 발행한다. Streams는 작업 큐, Pub/Sub은 브로드캐스트 채널 — 다른 메커니즘이다.

---

## 6. 멱등성 · 순서 · 복구 규약

순서는 `eventId`가 잡는다. 중복은 멱등 키가 막고 유실 복구는 REST 몫이다.

- **순서**: 방 안의 모든 이벤트(stream·post)는 **`eventId`(ULID) 오름차순**. 클라는 `eventId`로 정렬·중복제거. 틱(quote)은 순서 무의미(최신 스냅샷).
- **멱등성**:
    - 수집: `sourceId`로 중복 제거(같은 뉴스 재처리 흡수).
    - 저장: StreamEvent/post는 자연키 upsert(재시도/중복 발행에도 1건).
    - 투자의견: `source_key=opinion:{code}:{businessDate}:{brokerCode}:{contentHash}` 부분 유니크. 충돌 시 기존 `eventId`를 읽어 재사용한다.
- **복구**: 끊긴 동안 놓친 stream·post는 **메인서버 REST로 `cursor`(마지막 eventId) 이후 조회**. WS는 재전송하지 않는다. **틱은 복구 안 함**(다음 틱이 대체).
- **persist → publish → ack** 순서 불변식(§2.2): 저장이 진실, 발행은 best-effort 통보, ACK는 둘 성공 후. → 워커 장애 시 PEL 재처리로 무유실, 멱등으로 무중복.
- **투자의견 전달**: DB 저장 후 같은 `eventId`로 PUBLISH를 at-least-once 시도한다. `published_at`은 Redis 명령 수락 여부만 나타내며 클라이언트 전달을 보장하지 않는다. 중복은 `eventId`로 제거하고 유실은 REST로 복구한다.

---

## 7. 합의 필요 항목 (서비스 간)

게이트웨이·워커·메인서버가 똑같이 알아야 하는 계약 지점과 역할 분담이다.

| 항목 | 게이트웨이 | 워커 | 메인서버 |
|---|---|---|---|
| 채널명 `quote/stream/post:{code}` | 구독 | quote=price, stream=llm·batch(투자의견) 발행 | post 발행 |
| `queue:ingest` 엔트리 스키마(§2.1·§2.3) | — | ingest=생산, llm=소비 | — |
| 봉투/`eventId`(ULID) 규약 | 파싱 | 생성 | 생성 |
| `watchlist:updated` payload | 구독·세션조정 | — | 발행 |
| `demand:*:{gwId}`·`gw:alive:{gwId}`·`demand:updated` (§1.3) | refcount 쓰기·전이 발행·하트비트 | price=합산·구독 | — |
| `cursor` 의미(읽음 위치) | — | — | 쓰기/복구 |

- 이 표의 모든 문자열·DTO는 Gradle `:contracts` 서브프로젝트에 상수/레코드로 박아 세 앱이 공유한다.

---

## 부록 — LLM 요약 트레이스 (시퀀스)

```
ingest-worker:  XADD queue:ingest  source=hankyung sourceId=a1b2 codes=005930 type=news title=... url=...
llm-worker:     XREADGROUP GROUP g:llm c1 COUNT 1 STREAMS queue:ingest >
                  → entry(a1b2)
                  → seen:ingest:a1b2 없음 → 진행
                  → LLM: summary="삼성전자 ... 요약" category="news"
                  → StreamEvent{ eventId=01J..., code=005930, category=news, summary=..., ... }
                  → ① INSERT ... ON CONFLICT(sourceId) DO NOTHING   (persist)
                  → ② PUBLISH stream:005930  {type:stream,code:005930,eventId:01J...,data:{...}}
                  → ③ XACK queue:ingest g:llm <id>
게이트웨이:      (stream:005930 구독 중) → /user/queue/stream 으로 relay → 관심목록에 005930 가진 클라들
```
