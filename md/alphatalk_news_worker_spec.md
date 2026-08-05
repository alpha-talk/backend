# Alpha Talk — 뉴스 파이프라인 명세 v0.1
**worker-ingest · worker-llm · 담당: 민균**

worker-ingest·worker-llm을 구현하거나 리뷰하기 전에 여는 문서다. 뉴스를 수집해 관련 종목 방으로 배달하고, 같은 사건을 다룬 여러 언론사 기사를 하나로 묶고, 매일 종목별 호재·악재 브리핑을 만드는 파이프라인의 설계 기준을 정한다. 서비스 사이의 계약 자체는 [redis_contract.md](redis_contract.md) §2(`queue:ingest`)와 [ws_api_spec.md](ws_api_spec.md) §4.3(stream payload)이 소유하고, 이 문서는 그 계약 위에서 두 워커가 무엇을 어떤 순서로 하는지를 다룬다. 이 설계가 요구한 계약 확장은 **각 계약 문서에 반영 완료**(redis_contract v0.7 · ws_api_spec v0.6 · KIS 워커 명세 v0.2) — 내역은 §6.

---

## 0. 개요 & 책임 경계

파이프라인은 세 가지 요구에서 출발했다.

| 요구 | 대응 | 근거 FR |
|---|---|---|
| ① 신문사 크롤링 → 관련 종목 방 배달 | worker-ingest 수집·종목 매핑 → `queue:ingest` → worker-llm 요약 → `stream:{code}` | FR-11 (M1) |
| ② 유사 내용의 타 언론사 기사 묶기 | worker-llm 임베딩 클러스터링 — 클러스터당 스트림 이벤트 1건 | FR-04/05 노이즈 감소 |
| ③ 매일 호재·악재 AI 요약 | 일일 다이제스트 — `queue:ingest`에 digest 잡 적재 → worker-llm 생성 | 기획안 AI_SUMMARY(P3)의 뉴스 기반 조기 구체화 |

수집과 가공은 서버를 나누고, 둘은 `queue:ingest` 하나로만 잇는다. 큐를 사이에 두면 스케줄은 싱글턴(ingest), 실행은 무상태 ×N(llm)으로 갈라져 리더 선출이 필요 없다.

| 워커 | 책임 | 비책임 |
|---|---|---|
| **worker-ingest** | 소스 폴링(RSS·네이버 검색 API), 정규화·exact 중복 제거, `XADD queue:ingest`, 일일 다이제스트 잡 적재(스케줄러) — 종목 후보는 소스가 아는 것만 전달(텍스트 매칭 없음, §2.4) | LLM 호출, DB 쓰기(읽기는 허용), Pub/Sub 발행 |
| **worker-llm** | `XREADGROUP g:llm` 소비, 본문 확보, 클러스터 판정, LLM 요약·감성 분류, `stream_event` persist, `PUBLISH stream:{code}`, XACK, 일일 다이제스트 생성 | 수집·폴링, 스케줄(무상태 ×N 유지) |

**핵심 설계 결정** (근거는 각 절):

1. **클러스터링은 worker-llm에서, 임베딩 + pgvector로** — 수집 시점엔 비교 대상이 없고, 임베딩·LLM 접근이 llm 쪽에 응집돼 있다. (§3.3)
2. **클러스터당 `stream_event` 1건/종목** — 후속 편입 기사는 payload jsonb 병합만 하고 재발행하지 않는다(스트림 중복 노출 방지, core-api 명세 §12 노트 9와 동일 패턴). (§3.5)
3. **감성(호재/악재)은 (클러스터, 종목) 단위** — 같은 기사가 A사엔 호재·B사엔 악재일 수 있다. (§3.4)
4. **일일 다이제스트도 `queue:ingest`를 탄다** — ingest 스케줄러(싱글턴)가 `type=digest` 엔트리를 적재하고 llm-worker가 경쟁 소비. 신뢰성 장치(PEL·ACK·멱등)를 그대로 재사용하고 llm-worker의 무상태 ×N을 유지한다. (§4)
5. **본문은 LLM 입력으로만 쓰고, 저장은 발췌(200자)+원문 링크** — 기획안 §6 저작권 방침(요약+원문 링크) 준수. (§2.3)
6. **매크로·섹터 뉴스는 scope 사다리로** — LLM이 STOCK/SECTOR/MARKET 판정. SECTOR는 업종 구성 종목으로 해소해 fan-out(섹터 이슈 표기), MARKET은 방 fan-out 없이 일일 다이제스트로만. (§3.6)

---

## 1. 전체 파이프라인

두 워커 사이에 직접 호출은 없다. 모든 연결은 `queue:ingest` 한 곳을 지나고, 기사는 수집 → 큐 → 가공 → 발행 한 방향으로만 흐른다.

```
[RSS 피드들]  [네이버 뉴스검색 API]                        (매일 18:00 KST)
      │              │                                        │
      ▼              ▼                                        ▼
┌─────────────────────────────┐                    ┌──────────────────────┐
│ worker-ingest               │                    │ ingest 스케줄러       │
│  폴링 → 정규화 → seen 체크   │                    │  당일 뉴스 있는 종목별 │
│  → 종목 후보(소스 부여만)    │                    │  digest 잡 생성       │
└──────────┬──────────────────┘                    └──────────┬───────────┘
           │ XADD (type=news)                                 │ XADD (type=digest)
           ▼                                                  ▼
                    [STREAM] queue:ingest  ── XREADGROUP g:llm ──►  worker-llm ×N
                                                                      │
                    ┌─────────────────────────────────────────────────┤
                    ▼ type=news                                       ▼ type=digest
        본문 확보 → 임베딩 → 클러스터 판정                 당일 클러스터 요약·감성 취합
           ├─ 신규 클러스터: LLM 요약+감성                  → LLM 브리핑(호재/악재/종합)
           │    → ① stream_event INSERT                     → ① stream_event(type=AI) upsert
           │    → ② PUBLISH stream:{code}                   → ② PUBLISH stream:{code}
           │    → ③ XACK                                    → ③ XACK
           └─ 기존 클러스터 편입: payload 병합(재발행 없음) → ① 병합 ③ XACK
```

persist → publish → ack 순서 불변식(Redis 계약 §2.2)은 모든 분기에서 유지한다. 편입 분기는 publish만 생략한다(발행은 원래 best-effort).

---

## 2. worker-ingest — 수집

worker-ingest는 외부 소스를 큐 엔트리로 바꾸는 일만 한다. 관련성 판정도 요약도 하지 않고, 판단이 필요한 것은 전부 worker-llm으로 넘긴다.

### 2.1 소스 전략

소스마다 저작권 부담과 종목 매핑 비용이 달라 셋을 다르게 다룬다.

| 소스 | 방식 | 종목 매핑 | 비고 |
|---|---|---|---|
| 언론사 RSS (경제·사회 섹션) | 주기 폴링(5분), `If-Modified-Since`/ETag 활용 | 없음 — 관련성·종목 판정은 LLM(§2.4) | 제목+링크+요약만 제공 — 저작권 안전 |
| 네이버 뉴스 검색 API | 종목명 쿼리, 주기 폴링(10분) | 쿼리 자체가 종목 — 매핑 공짜 | 공식 오픈 API, **25,000건/일** 한도 |
| OpenDART 공시 *(후속)* | 목록 API 폴링 | 공시 자체에 종목 포함 | `type=disclosure`, 동일 경로 재사용 |

- RSS 설정은 기본 `application.yml`이 소유해 실행 프로필과 무관하게 같은 목록을 쓴다. 피드의 `id`는 섹션별 로그·메트릭 식별자이고, `source`는 언론사 단위 표시값이자 `sourceId` 네임스페이스다. 같은 언론사의 여러 섹션에 같은 기사가 걸려도 하나만 남긴다.
- MVP RSS는 연합뉴스·한국경제·매일경제·동아일보·경향신문·한겨레·조선일보·뉴시스·서울경제의 경제·산업·금융·증권·사회 섹션을 쓰고 정치 섹션은 수집하지 않는다.
- 피드별 ETag와 Last-Modified는 해당 `NewsSource` 인스턴스가 메모리에 들고 있다. `304 Not Modified`는 빈 결과로 처리하며, 재기동 후 첫 요청은 조건 없이 보낸다.
- 소스는 `NewsSource` 포트 뒤의 어댑터로 추가한다 — 소스가 늘어도 파이프라인 코드는 그대로여야 한다.
- **크롤링 준법**: robots.txt 준수, 식별 가능한 User-Agent, 사이트별 요청 간격 제한. robots.txt와 본문 요청은 `rate:article-fetch:{host}` TTL 게이트로 모든 llm-worker 인스턴스가 기본 1초 간격을 공유한다. 본문 페이지 fetch는 worker-llm이 신규 클러스터를 요약하기 직전에만 한다(수집 단계 대량 fetch 금지).
- **폴링 병렬화**: 소스별 fetch·처리는 고정 크기 스레드 풀(`fetch-concurrency`, 기본 4)에서 소스 단위 태스크로 병렬 실행한다. 느리거나 죽은 소스(타임아웃 최대 ~15s) 하나가 전체 폴링 주기를 끌지 않게 하려는 것이다. 같은 소스 안의 기사 처리는 순차라 사이트별 요청 예절은 그대로 지켜지고, 통계는 소스별로 모은 뒤 합산한다(공유 가변 상태 없음).
- 네이버 API 예산: 쿼리 대상 종목 × 폴링 횟수가 한도를 넘지 않게 설계한다. 시드 41종목 × 6회/시간 × 24h ≈ 5,900건/일로 여유. 전 종목(~2,600) 확장 시 수요 기반 선별이 필요하다(§10 오픈 이슈).

### 2.2 폴링 대상 종목

MVP 폴링 대상은 **설정 파일의 시드 종목 목록**이다(worker-price의 41종목과 같은 세트). 이 목록은 네이버 검색 API 쿼리에만 쓴다 — RSS는 종목과 무관하게 전체를 수집하므로 목록의 영향을 받지 않는다. 확장 후보는 `watchlist` distinct 코드 합집합 ∪ 거래대금 상위 N(`daily_candle`)이며, 워커가 core-api 테이블을 읽는 것은 core-api가 워커 테이블을 읽는 것과 대칭이라 허용한다.

### 2.3 정규화 · exact 중복 제거

중복 제거는 두 층으로 나눈다. 이 단계는 **같은 기사를 다시 받은 경우만** 거른다. 같은 사건을 다룬 다른 언론사 기사는 sourceId가 다르므로 그대로 통과하고, 묶는 일은 worker-llm의 클러스터링(§3.3) 몫이다.

| 단계 | 규칙 |
|---|---|
| URL 정규화 | 추적 파라미터(utm_* 등) 제거, 스킴·호스트 소문자화 |
| `sourceId` | `{source}:{기사 고유 ID}` — 고유 ID 없으면 정규화 URL의 SHA-256 앞 16자 |
| exact 중복 | `SETNX seen:ingest:{sourceId}` (TTL 7일) 실패 시 skip — 같은 기사 재수집 흡수 |
| 저장 범위 | 제목 + 리드 발췌(≤200자) + 원문 URL만 큐에 싣는다. 전문(全文)은 싣지도 저장하지도 않는다 |

### 2.4 종목 후보 매핑

관련성·종목 판정은 LLM(2차)이 전담한다. 수집 단계는 소스가 이미 아는 종목만 힌트로 붙이고, 한 종목도 못 붙인 기사라도 버리지 않는다.

| 단계 | 방법 | 담당 |
|---|---|---|
| 1차 | 소스가 종목을 알면 그대로(네이버 쿼리, DART 공시) — **텍스트 매칭 없음** | ingest |
| 2차 | LLM 요약 시 관련 종목 확정 — 후보 오탐 제거 및 후보 외 종목 발견(발견 종목은 `stock_master` 존재 검증 후 채택) | llm |

- **v0.6 결정: 수집 측 텍스트 매칭(종목명 사전·매크로 키워드)을 코드 레벨에서 제거했다.** 종목명 사전은 전 종목 등록·별칭 관리 부담에 비해 이득이 없고(판정은 어차피 LLM 전담), 매크로 `macroHint`는 소비 측이 쓴 적이 없다. `stock_alias` 테이블(§5)은 예약으로만 남긴다.
- 1차는 **후보 힌트일 뿐 게이트가 아니다** — 후보 0건이어도 **전량 적재**한다(`codes` 공란 허용, Redis 계약 v0.7 §2.1). 종목명이 한 번도 안 나오는 사회 기사도 정책→수혜 종목 같은 간접 영향을 가지므로, 수집 단계에서 걸러버리면 그런 기사를 통째로 잃는다. 후보에 없던 종목은 `stock_master` 존재 검증 후 채택하고(환각 코드 차단), LLM이 기사 전체를 `marketRelevant=false`로 판정하면 후보·scope와 무관하게 IRRELEVANT로 버린다(§3.4). 전량 적재의 비용 방어선은 클러스터링이다 — 같은 사건이면 LLM 호출 1회로 끝난다(§3.3).
- 후보가 빈 기사의 클러스터 판정 직렬화는 단일 `lock:cluster:macro` 락으로 수렴한다. 단일 인스턴스 운용(로컬)에선 무해하나, 운영 ×N 확장으로 병렬성이 필요해지면 이 결정을 재평가한다(§7).
- `codes` 필드는 큐 스키마(Redis 계약 §2.1) 그대로 콤마 구분 다중이다.

### 2.5 큐 적재

Redis 계약 §2.1 스키마를 그대로 쓴다: `source` · `sourceId` · `type` · `codes` · `title` · `url` · `body`(발췌) · `fetchedAt`. `XADD queue:ingest MAXLEN ~ 10000`.

---

## 3. worker-llm — 가공

worker-llm은 큐 엔트리를 방에 뜨는 이벤트로 바꾼다. 인스턴스는 상태를 들고 있지 않아 ×N으로 늘려도 되고, 중복·유실 방어는 스트림 그룹(PEL·ACK)과 DB 자연키 upsert가 맡는다.

### 3.1 소비 루프

인스턴스 여럿이 컨슈머 그룹 `g:llm` 하나로 경쟁 소비한다.

- `XREADGROUP GROUP g:llm {consumerName} COUNT {batch} BLOCK 5000 STREAMS queue:ingest >` — consumerName은 PID@호스트명, batch 기본 8.
- 유휴 회수: 주기적으로 `XPENDING`에서 min-idle-time을 넘긴 엔트리를 찾고 `XCLAIM queue:ingest g:llm {me} min-idle-time ...`으로 인계한다.
- **poison 엔트리**: delivery count > 5면 `queue:ingest:dlq`로 XADD 후 원큐 XACK + 알람 메트릭. (⚠️ §6 증보)

### 3.2 type=news 처리 순서

순서는 아래로 고정한다. XACK는 언제나 맨 뒤다.

```
1. 멱등 체크와 클러스터 판정(§3.3): 제목 + 큐 발췌 300자를 임베딩하고 신규 생성 or 기존 편입, news_article INSERT(발췌만). 기존 sourceId는 해당 클러스터를 그대로 사용하고 누락된 후보 종목만 복구
2-a. [요약 완료 클러스터 편입] 대응 stream_event.payload에 sources 병합(재발행 없음) → XACK. 단, 편입 기사가 새 종목을 추가하면 그 종목에만 신규 INSERT+PUBLISH
2-b. [신규·중단 클러스터] 요약 token lease 선점. 다른 워커가 소유 중이면 PEL에 남겨 재시도
3. 본문 확보: 허용 호스트 원문 URL fetch → 본문 추출(jsoup). SSRF 가드(http/https 강제·사설/루프백/링크로컬/메타데이터 대역 차단·리다이렉트 홉별 재검증)·robots.txt·호스트별 요청 간격 준수. 실패·차단 시 큐 발췌만으로 진행
4. LLM 요약·감성(§3.4) → ① stream_event INSERT(종목별) → ② PUBLISH stream:{code} 각 종목 → ③ token 일치 조건으로 클러스터 완료 → ④ XACK
     단, 편입 기사가 새 종목을 추가하면 그 종목에만 신규 INSERT+PUBLISH
```

중간 단계에서 죽으면 XACK 전이므로 PEL이 재처리한다. 각 쓰기는 자연키 upsert(`sourceId`, `(cluster_id, code)`)라 재처리해도 중복이 생기지 않는다 — FR-11 DoD(동일 sourceId 중복 요약 0건)를 이 구조가 보장한다.

### 3.3 클러스터링 — "비슷한 기사 하나로 묶기"

같은 사건을 다룬 기사 N건을 클러스터 1건으로 접는다. 그래야 LLM 호출이 사건당 1회로 줄고(§3.4), 같은 소식이 방 스트림에 여러 번 뜨지 않는다.

| 항목 | 값 | 근거 |
|---|---|---|
| 비교 대상 | 종목 교집합이 있는 최근 **72시간** 클러스터. `codes`가 빈 기사는 SECTOR·MARKET, 아직 종목 후보가 없는 미완료 클러스터, 또는 후보 없이 들어온 기사를 이미 포함한 클러스터끼리 비교 | LLM이 뒤늦게 종목을 발견해 STOCK이 된 클러스터에도 같은 빈 후보 기사가 재합류하게 하면서 다른 종목 기사와의 오합류를 제한 |
| 검색 | pgvector `embedding <=> :v` 최근접 5건 조회 | PG가 이미 있고 마이그레이션 단일 관리 원칙(`db-migrations`)에 부합 — 별도 벡터 스토어 불요 |
| 판정 | 코사인 유사도 ≥ **0.85** → 최고 유사 클러스터에 편입, 미만 → 신규 클러스터 | 임계값은 실데이터로 튜닝(§10) |
| 지름길 | 정규화 제목 해시(언론사명·[속보]·특수문자 제거)가 기존 기사와 일치하면 임베딩 생략하고 그 클러스터에 편입 | 통신사 전재 기사(제목 거의 동일)가 다수 — 임베딩 호출 절약 |
| 동시성 | 판정~INSERT 구간을 `lock:cluster:{primaryCode}` 분산락(SET NX PX 3000, Lua compare-and-delete 해제)으로 직렬화. **임베딩·LLM 호출은 락 밖**. 요약은 `summarizing_token`·`summarizing_at` CAS lease로 한 워커만 소유한다. LLM 응답 뒤 lease 갱신 UPDATE로 클러스터 행을 잠그고 링크·이벤트·최종 상태를 한 DB 트랜잭션에 저장하므로, 만료 재선점 뒤 이전 토큰의 부분 결과도 남지 않는다. `news_cluster_stock.stream_event_id` 원자 클레임도 이벤트 중복을 막는다 | llm-worker ×N이 동일 사건 기사를 동시 처리하면 클러스터·요약·이벤트가 중복 생성됨 |

클러스터 상태는 `NEW`(생성 직후) → `SUMMARIZING`(토큰 lease 선점) → `SUMMARIZED`(요약·발행 완료)로 간다. 전부 무관하면 `IRRELEVANT`로 끝난다. 요약 중 워커가 죽으면 lease 만료 뒤 다른 워커가 새 토큰으로 재선점하며, 이전 토큰은 결과를 저장할 수 없다.

### 3.4 LLM 요약 · 호재/악재 분류

한 클러스터당 LLM 1회 호출로 끝낸다(묶인 기사 N건이어도 1회 — 클러스터링이 곧 비용 절감). 구조화 출력(tool use)으로 스키마를 강제한다.

**입력**: 대표 기사 본문(확보 시) + 클러스터 내 기사 제목들 + 후보 종목 목록(코드·종목명)
**출력 스키마**:

```json
{
  "summary": "3줄 요약 (각 줄 ≤ 80자)",
  "marketRelevant": true,
  "scope": "STOCK | SECTOR | MARKET",
  "stocks": [
    { "code": "005930", "relevant": true, "sentiment": "POSITIVE|NEGATIVE|NEUTRAL", "confidence": 0.0~1.0, "reason": "한 줄" }
  ],
  "sectors": [
    { "sectorCode": "27", "sentiment": "POSITIVE|NEGATIVE|NEUTRAL", "impact": "HIGH|MEDIUM|LOW", "reason": "한 줄" }
  ]
}
```

- `marketRelevant=false`이면 `scope`·종목·섹터 결과를 보지 않고 클러스터를 `IRRELEVANT`로 마킹해 발행 없이 XACK한다. `MARKET`은 증시 전체에 관련된 기사만 뜻한다.
- `marketRelevant=true`에서 `scope=STOCK`이면 `sectors`는 무시하고, `SECTOR`/`MARKET` 처리는 §3.6을 따른다. 프롬프트에는 §5 `sector` 목록을 섹터 후보로 제시한다(자유 서술이 아니라 코드 선택).
- `relevant=false`인 종목 후보는 제외한다(후보 오탐 제거). STOCK/SECTOR 판정인데 채택할 종목·섹터가 없으면 방어적으로 `IRRELEVANT` 처리한다.
- confidence < 0.6이면 sentiment를 NEUTRAL로 강등한다 — 애매한 건을 호재/악재로 단정하지 않는다.
- 운영 `anthropic` provider의 기본 모델은 클러스터 요약 **claude-haiku-4-5**(건수 많음·단순), 일일 다이제스트 **claude-sonnet-5**(하루 종목당 1회·종합 판단)다. `LlmClient` 포트 뒤라 교체는 자유롭다.
- 로컬은 `claude-cli`(기본) 또는 `codex-cli` provider로 로그인된 개인 구독을 쓰며 **단일 worker-llm 인스턴스 운용만 지원**한다. 두 CLI 모두 단발성 비대화형 실행·JSON Schema 강제·세션 비영속·2분 타임아웃이고, 자식 프로세스에서 API 키 환경변수를 제거해 구독 인증과 API 과금이 섞이지 않게 한다. Claude는 도구를 전부 끄고 safe mode로, Codex는 빈 임시 작업공간과 read-only sandbox에서 실행한다. local 프로파일은 `consumer-batch=1`로 한 번에 PEL에 한 건만 선점하고, 기동 시 CLI timeout이 `claim-idle`보다 짧은지 검증한다. worker-llm ×N 운용은 운영 `anthropic` provider에만 적용한다.
- 비용 추정: 시드 41종목 기준 일 ~500기사 → ~150클러스터 × ~2K tokens(Haiku) + 41다이제스트 × ~3K tokens(Sonnet) — 월 수 달러 수준.
- 워커 내 재시도는 백오프 1회까지다. 그 이상은 PEL 재처리에 맡긴다(이중 재시도 루프 금지).

### 3.5 발행 정책

발행은 클러스터당 종목별 이벤트 1건으로 끝난다. 뒤늦게 편입되는 기사는 기존 이벤트의 payload만 병합하고 다시 발행하지 않는다 — 같은 소식이 방 스트림에 두 번 뜨는 것을 막기 위해서다.

| 상황 | stream_event | PUBLISH |
|---|---|---|
| 신규 클러스터 | 종목별 INSERT (`type=NEWS`, `event_id`=ULID) | `stream:{code}` 각 종목 |
| 기존 클러스터 편입 | 해당 이벤트 `payload` jsonb 병합(`sources` 추가) | 없음 (재발행 금지) |
| 편입인데 새 종목 등장 | 새 종목만 INSERT | 새 종목만 |

stream payload (ws_api_spec §4.3 확장 — ⚠️ §6 증보):

```json
{
  "category": "news",
  "title": "삼성전자, 3나노 대규모 수주",
  "summary": "…3줄…",
  "sentiment": "POSITIVE",
  "sourceUrl": "https://…(대표 기사)",
  "sources": [ { "name": "한국경제", "url": "…" }, { "name": "매일경제", "url": "…" } ],
  "occurredAt": 1719500000000
}
```

`stream_event.payload`는 JSONB라 core-api 스키마를 건드리지 않고 그대로 조회에 노출된다(`GET /rooms/{code}/stream`).

**수집 type의 관통(정형화)**: 큐 `type`(news/report/disclosure)은 `:contracts`의 `StreamCategory` 매핑을 타고 발행까지 관통한다 — `IngestType → StreamCategory(payload, eventType)` → payload `category`와 `stream_event.type`이 함께 결정된다(digest→ai). 클러스터는 **첫 기사 type으로 카테고리를 보유**하며(V6 `news_cluster.category`), 이후 편입 기사 type이 달라도 유지한다. 새 소식 종류를 추가할 때는 `IngestType`·`StreamCategory`에 값을 더하는 것으로 끝난다 — 워커 코드에 카테고리 리터럴을 두지 않는다.

### 3.6 섹터·매크로 뉴스 — scope 사다리

금리 인상·환율 급변·업종 규제처럼 특정 기업 언급 없이 업종 전반에 작용하는 뉴스는 종목 후보가 붙지 않아, 어느 방에 배달할지가 별도 문제가 된다. LLM 판정 `scope`(§3.4)에 따라 세 갈래로 처리한다.

| scope | 판정 예 | 배달 | 근거 |
|---|---|---|---|
| `STOCK` | 개별 기업 수주·실적·공시 | 현행 경로(§3.5) | — |
| `SECTOR` | 기준금리 인상 → 은행·증권·건설 | 섹터를 구성 종목으로 해소해 종목별 stream_event INSERT + PUBLISH. payload에 `scope`·`sector` 표기 | 섹터 방이 없으므로("종목 하나=방 하나") 구성 종목 방이 유일한 노출면 — 방에서 "섹터 이슈" 배지로 구분 |
| `MARKET` | 코스피 전체 급락, 거시 지표 | 방 fan-out **없음** — 일일 다이제스트 '시장 이슈'로만 반영(§4.2) | 전 방 동보(~2,600방)는 노이즈·비용만 크고 종목 방의 정보가치가 없다 |

- **섹터 축은 KSIC 업종이다(v0.7 변경)**: 기존 `sector`·`stock_master.sector_code`를 **그대로 쓰되 내용을 KSIC로 교체**했다(KIS 워커 명세 §4). 축을 둘로 늘리지 않는다 — 종목당 유효 업종이 하나인 현재 요구에서는 별도 membership 테이블이 필요 없고, core-api는 `sector.name`만 읽으므로 이름이 KSIC 세부 업종명으로 바뀌어도 API 형태가 깨지지 않는다. KIS 마스터의 업종 필드는 대분류 18종(`제조` 하나에 557종목)뿐이라 fan-out 대상이 되지 못했다 — 중분류 필드도 28종에 그쳐 한계가 같다. `sector`에는 KSIC 전 계층(2~5자리)을 `level`·`parent_code`·`version`과 함께 보유하고, 라우팅에 쓰는 유효 코드만 `stock_master.sector_code`에 배정한다. 배정은 **소분류(3자리)에서 시작해 상한을 넘는 그룹만 한 단계씩 세세분류(5자리)까지 내린다**. 실측(2026-08, 활성 2,604종목): 177그룹 · 중앙값 6 · 최대 95 · 상한 초과 0.
- **섹터 해소**: `stock_master.sector_code`로 활성 구성 종목을 조회해 합집합·중복 제거 후 배달한다. **커버리지 화이트리스트는 제거했다** — 무차별 배달의 방어선은 배치가 유지하는 그룹 크기이고, worker-llm의 `fanout-cap`(기본 100)은 그 불변식이 깨졌을 때만 작동하는 이중 안전장치다. **상한 초과를 MARKET으로 바꾸지 않는다** — `MARKET`은 시장 전체 기사라는 뜻이고 fan-out 규모와 무관하다. 초과 시 scope는 `SECTOR`로 두고 실시간 발행만 억제해 일일 다이제스트에만 반영한다(`sector.fanout.suppressed` 카운터).
- **직접 관련 종목**도 SECTOR scope 클러스터에서는 `scope=SECTOR` + 자기 섹터(`sectorOf`)를 payload에 표기한다(클라 배지 일관성).
- **섹터 스키마는 N6 선행 조건**: `sector`·`stock_master.sector_code` 조회 예외는 삼키지 않고 전파해 PEL이 재시도하게 한다(빈 결과를 성공으로 오인해 영구 미발행되는 것 방지). 업종 목록이 **빈 결과인 것도 예외로 취급**한다 — worker-batch `industry_sync`가 적재하지 않았으면 뉴스 처리가 진행되지 않는 게 정상이다(fail-closed).
- **DART 신고 업종이 뉴스 맥락과 어긋나는 종목은 수동 보정한다**: `alphatalk.batch.dart.group-overrides`. 예로 삼성전자의 신고 업종은 `264 통신 및 방송장비`라 반도체 그룹에 들어가지 않는다 — `sector_code`만 `261`로 덮고 `stock_master.dart_induty_code`에 DART 원본을 보존해 추적 가능하게 둔다. 손으로 관리하는 예외라 **시총 상위·뉴스 빈출 종목으로 짧게 유지**하고, 늘어나면 축 자체를 재검토한다.
- **종목당 업종은 하나다**: 겸업(반도체+통신장비 등)을 표현하지 못한다. 복수 업종이 실제 요구가 되면 `stock_master.sector_code` 대신 `(code, sector_code)` membership 테이블을 추가한다.
- **우선주는 업종이 없다**: OpenDART는 보통주에만 회사코드를 부여해 우선주 113종목(2026-08 기준)은 `sector_code`가 비어 섹터 뉴스를 받지 못한다. 보통주 업종 승계는 후속 과제다.
- **업종 후보 제시는 아직 전량 열거다**: 프롬프트에 실제 사용 중인 업종(2026-08 기준 177개)을 모두 싣는다. KSIC 세세분류(1,196개)까지 라우팅을 넓히려면 열거로는 감당되지 않으므로, 기사 임베딩으로 업종 Top-K를 뽑아 후보 10~20개만 제시하는 구조가 선행돼야 한다 — 후속 과제(§7).
- **이번 단계의 수용 기준은 그룹 해상도다**: `KIS 대분류 18개 → 적응형 KSIC 177그룹`까지가 범위다. LLM이 `26410` 같은 **세세분류를 직접 골라 그 5자리 업종에만 배달**하는 leaf 라우팅은 위 Top-K 검색이 들어온 뒤의 목표다 — 지금은 `26410`도 정상 상황에서는 `264`로 접힌다.
- **감성은 섹터별로 반대일 수 있다** — 금리 인상은 은행 POSITIVE·건설 NEGATIVE. `news_cluster_sector`에 섹터 단위로 저장하고, fan-out된 각 stream_event에는 **그 종목이 속한 섹터의 감성**을 싣는다.
- **노이즈 가드 2중**: ① 매크로 기사는 물량이 많지만 클러스터링(§3.3)이 선행 방어선이다 — 금리 기사 수십 건도 1클러스터 1이벤트. ② `impact=LOW` 판정은 실시간 fan-out 없이 다이제스트에만 반영한다(방 스트림은 HIGH·MEDIUM만).
- SECTOR 이벤트 payload 예:

```json
{ "category": "news", "scope": "SECTOR", "sector": { "code": "27", "name": "은행" },
  "title": "한은, 기준금리 25bp 인상", "summary": "…", "sentiment": "POSITIVE", "sources": [ … ], "occurredAt": … }
```

> 증권사 **투자의견**은 이 파이프라인을 타지 않는다 — worker-batch가 `stream_event` 저장 후 `stream:{code}`를 직접 발행한다(llm-worker와 공동 생산자, [KIS 워커 명세](alphatalk_kis_worker_spec.md) §3.3 · Redis 계약 v0.7). 라우팅은 같지만 payload는 WS v0.6의 `kind=opinion`·`opinion{}` 하위 호환 확장을 쓴다. `queue:ingest`·llm-worker는 관여하지 않는다.

---

## 4. 일일 다이제스트 — 호재/악재 브리핑

다이제스트는 하루치 클러스터 요약을 종목당 브리핑 1건으로 접는다. 이미 요약해 둔 것을 취합하는 단계라 원문을 다시 읽지 않는다.

### 4.1 트리거와 멱등

다이제스트도 뉴스와 같은 큐를 탄다. 그래야 PEL·ACK·멱등 같은 신뢰성 장치를 새로 만들지 않고 그대로 쓴다(§0 결정 4).

- **ingest 스케줄러**가 매일 **18:00 KST**(장 마감 후)에 **시드 종목 전체**를 대상으로 `XADD queue:ingest type=digest codes={code} sourceId=digest:{code}:{yyyy-MM-dd}`를 실행한다. ingest는 DB를 보지 않아 어떤 종목에 클러스터가 쌓였는지 모르므로 잡을 전 종목에 적재하고, 윈도 `[전일 18:00, 당일 18:00)`에 소식(STOCK·SECTOR)이 없는 종목 잡은 llm-worker가 브리핑 없이 ACK한다(시장 이슈만으로는 브리핑을 만들지 않는다).
- llm-worker가 같은 그룹(`g:llm`)으로 경쟁 소비한다 — 스케줄은 싱글턴(ingest), 실행은 ×N(llm)으로 갈라져 리더 선출이 필요 없다.
- 멱등 키 `digest:{code}:{date}` → `stream_event` upsert. 재처리·중복 적재에도 브리핑은 하루 1건이고, 동시 처리 경쟁은 `stream_event (code, digest date)` 부분 유니크 인덱스(V3)가 최종 차단한다.

### 4.2 생성

**입력**: 윈도 내 해당 종목 클러스터들의 (요약 3줄, sentiment, confidence, 기사 수) 목록 — 원문 재조회 없음(이미 요약된 것의 취합).
**출력** (`type=AI`, `category="ai"`):

```json
{
  "category": "ai",
  "title": "삼성전자 데일리 브리핑 (7/16)",
  "summary": "…종합 3줄…",
  "digest": {
    "date": "2026-07-16",
    "positives": [ { "title": "3나노 수주", "line": "한 줄 요지", "eventId": "01J…" } ],
    "negatives": [ { "title": "…", "line": "…", "eventId": "01J…" } ],
    "sectorIssues": [ { "title": "기준금리 25bp 인상", "line": "은행업 이자이익 개선 기대", "sentiment": "POSITIVE", "eventId": "01J…" } ],
    "marketIssues": [ { "title": "코스피 외국인 순매도 지속", "line": "…" } ],
    "neutralCount": 4,
    "newsCount": 12
  },
  "occurredAt": 1752654000000
}
```

- **입력에 세 층을 모두 취합한다**: 종목 직접 클러스터(STOCK) + 그 종목 섹터의 SECTOR 클러스터(impact LOW 포함 — 실시간에서 걸렀어도 여기엔 반영) + MARKET 클러스터. 단 `positives/negatives`에는 종목 직접 뉴스만 담고, 섹터·시장 요인은 `sectorIssues`/`marketIssues` 버킷으로 분리한다 — 전 종목 브리핑에 같은 매크로 문구가 반복돼 종목 고유 정보가 희석되는 것을 막는다.
- `eventId` 참조 덕분에 클라가 브리핑에서 원 뉴스 이벤트로 점프한다(MARKET 클러스터는 방 이벤트가 없으므로 eventId 없이 제목·한 줄만).
- 프롬프트에 면책을 고정한다: 투자 판단의 근거가 아니라 정보 요약임을 명시(기획안 FR-17 면책 방침과 같은 기조). 클라 노출 문구는 클라 몫이다.
- persist → publish(`stream:{code}`) → XACK — 뉴스와 같은 불변식이다.

---

## 5. DB 스키마 (Liquibase 마이그레이션 — `db-migrations` 모듈 소유, core-api는 읽기 전용)

테이블은 클러스터(사건) · 기사 · (클러스터, 종목) · (클러스터, 섹터) 네 축이다. 감성이 종목·섹터 쪽 테이블에 붙는 이유는 같은 사건이라도 대상마다 호재·악재가 갈리기 때문이다(§0 결정 3, §3.6).

```sql
CREATE EXTENSION IF NOT EXISTS vector;

news_cluster(
  id CHAR(26) PK,                -- ULID
  rep_title TEXT, summary TEXT NULL, scope TEXT NULL,
  category TEXT,                 -- news | report | disclosure
  status TEXT,                   -- NEW | SUMMARIZING | SUMMARIZED | IRRELEVANT
  summarizing_at TIMESTAMPTZ NULL,  -- 요약 lease(§3.3 동시성) — CAS로 단일 워커 선점
  summarizing_token TEXT NULL,      -- 만료 재선점 후 이전 소유자의 저장을 막는 fencing token
  first_published_at, last_article_at, article_count INT, created_at
)
news_article(
  id BIGSERIAL PK,
  source TEXT, source_id TEXT UNIQUE,   -- 멱등 키
  url TEXT, title TEXT, excerpt VARCHAR(200),   -- 전문 저장 금지
  title_hash CHAR(16),                  -- 전재 기사 지름길(§3.3)
  embedding vector(1024) NULL,
  candidate_codes_empty BOOLEAN,        -- 수집 시 종목 후보가 없었던 기사인지 여부
  published_at, fetched_at, cluster_id CHAR(26) FK NULL
)
news_cluster_stock(
  cluster_id FK, code CHAR(6),
  sentiment TEXT, confidence NUMERIC(3,2),
  rejected BOOLEAN NULL,               -- NULL=미평가, false=채택, true=LLM 기각
  stream_event_id CHAR(26) NULL,        -- 편입 시 payload 병합 대상
  PK(cluster_id, code)
)
stock_alias(code CHAR(6), alias TEXT, PK(code, alias))   -- 예약: 수집 측 사전 매핑 제거(§2.4 v0.6)로 현재 미적재·미조회
news_cluster_sector(
  cluster_id FK, sector_code FK,
  sentiment TEXT, confidence NUMERIC(3,2), impact TEXT,   -- HIGH | MEDIUM | LOW
  PK(cluster_id, sector_code)
)
-- news_cluster_sector.sector_code는 sector.code(KSIC 코드)를 참조한다
-- sector·stock_master·dart_corp_map은 KIS 워커 명세 §4 소유(반영됨) — 여기서 재정의하지 않는다

-- INDEX news_article (embedding vector_cosine_ops) ivfflat · (title_hash) · (published_at)
-- INDEX news_cluster (last_article_at) — 72h 창 후보 조회
```

Liquibase 마이그레이션(`db-migrations` 모듈, `news/` changelog — Flyway V1~V7에서 이관): `0001`(news_* + stock_alias) · `0002`(stream_event) · `0003`(다이제스트 부분 유니크 인덱스) · `0004`(news_cluster 요약 lease·fencing token) · `0005`(종목 verdict 기각 상태와 기존 완료 행 백필) · `0006`(클러스터 category와 허용값 제약) · `0007`(기사 수집 시 빈 후보 경계). worker-llm은 `db-migrations` 의존만으로 기동 시 changelog를 적용한다. 투자의견용 nullable `source_key`와 부분 유니크 인덱스는 core-api stream 모듈이 논리 소유하고, worker-batch 착수 시 `db-migrations`에 후속 changeSet으로 추가한다.

**Flyway → Liquibase 전환 정책** — 전환은 운영 DB가 생기기 전에 끝냈으므로 baseline(`changelog-sync`) 절차를 두지 않는다. `flyway_schema_history`만 있는 기존 로컬 DB는 지원하지 않는다 — `docker compose down -v`로 리셋 후 재기동이 유일한 경로다(Liquibase가 `0001`부터 재실행을 시도해 기동에 실패하는 것이 의도된 fail-closed다). 리셋 불가한 공유 DB가 전환 전에 생겼다면 그때는 해당 DB에 한해 수동 `changelog-sync`로 이력을 등록한다.

**changeSet 식별자 불변식** — Liquibase 변경셋 식별자는 `filepath::id::author`라 **파일 경로도 식별자의 일부**다. 적용 이력이 생긴 changeSet 파일은 이동·개명하지 않는다(경로 영구 보존). `db-migrations` 안의 디렉토리는 논리적 소유자를 표시할 뿐이며, 소유가 바뀌어도 파일은 제자리에 두고 문서로만 경계를 옮긴다.

**⚠️ `stream_event` 소유 경계** — `stream_event`의 **논리적 소유자는 core-api의 stream 모듈**이다(core-api 명세 §11, `StreamEventAppender` 경유 INSERT). worker-llm과 worker-batch(투자의견)는 이 테이블에 INSERT하는 별도 프로세스다. 마이그레이션 파일은 `db-migrations`가 단일 소유하므로 서버 간 DDL 충돌은 없다:
- `0002`(stream_event DDL)는 논리적으로 core-api 소유 — 단 changeSet 파일은 위 식별자 불변식에 따라 `news/`에 영구 보존하고, core-api 착수 시 문서·주석으로만 소유 경계를 표시한다.
- `0003`(다이제스트 유니크 인덱스)은 뉴스 파이프라인 고유 제약이므로 논리적 소유자가 worker-llm이다.

⚠️ pgvector는 PostgreSQL 확장이다 — docker-compose 이미지 `pgvector/pgvector:pg16`, Testcontainers도 같은 이미지를 쓴다. `vector(1024)` 차원은 임베딩 제공자 확정 시(§10) 함께 확정한다.

---

## 6. 계약 반영 내역

이 설계가 요구한 계약 확장은 **각 계약 문서와 `:contracts` 코드에 반영 완료**다(문서 먼저 원칙).

| 대상 문서 | 항목 | 상태 |
|---|---|---|
| redis_contract **v0.4** §2.1·§2.3 | `type="digest"` + digest 엔트리 규약(`sourceId=digest:{code}:{date}`) | ✅ 반영 |
| redis_contract v0.4 §2.1 | `macroHint` 필드(선택) + `codes` 공란 허용 | ✅ 반영 |
| redis_contract **v0.5** §2.1 | `codes` 공란 전면 허용(`macroHint` 불요) — 수집 게이트 제거, 전량 LLM 판정 전환 | ✅ 반영 |
| redis_contract **v0.6** §2.1 | 수집 측 텍스트 매칭 코드 제거 — `codes`=소스 부여 후보만, `macroHint` 예약 필드화(미적재) | ✅ 반영 |
| redis_contract v0.4 §2·§4 | `queue:ingest:dlq` — poison 격리(delivery count > 5) | ✅ 반영 |
| redis_contract v0.4 §3·§4 | `lock:cluster:{code}` — 클러스터 판정 직렬화 락(TTL 3s) | ✅ 반영 |
| redis_contract v0.4 §3·§4 | `rate:article-fetch:{host}` — llm-worker 인스턴스 간 원문 요청 간격 | ✅ 반영 |
| ws_api_spec **v0.5** §4.3 | `sentiment`·`scope`·`sector{}`·`sources[]`(news) · `digest{}`(ai) — 전부 optional, 비파괴 | ✅ 반영 |
| KIS 워커 명세 §4 | `sector`(KSIC 계층)·`stock_master.sector_code`·`dart_induty_code`·`dart_corp_map`(OpenDART 기업개황 → KSIC 업종축, `industry_sync` 적재) | ✅ 반영 (v0.7) |
| :contracts | `Queues`·`Keys.seenIngest/clusterLock`·`IngestQueueEntry`·`StreamData` v0.5 확장 | ✅ 반영 (N0) |
| :contracts | `StreamCategory` + `IngestType.streamCategory()` — 수집 type→발행 category·이벤트 type 관통 매핑 | ✅ 반영 |
| redis_contract **v0.7** §1.1 | `stream:{code}` 공동 발행자 batch-worker(투자의견 직접 발행 — KIS 명세 §3.3, llm-worker 비관여) | ✅ 반영 |

---

## 7. 모듈 구조 & 포트 (저장소 DIP 컨벤션)

```
worker-ingest/
├─ scheduler/   IngestPoller(소스 폴링 오케스트레이션) · DigestTrigger(§4.1)
├─ source/      NewsSource(포트) · RssNewsSource · NaverSearchNewsSource
├─ dedup/       SeenMarker(포트) · RedisSeenMarker
└─ queue/       IngestQueue(포트) · RedisIngestQueue(XADD)

worker-llm/
├─ consume/     IngestConsumer(XREADGROUP 루프 · XPENDING/XCLAIM · DLQ 격리)
├─ article/     ArticleFetcher(포트) · ArticleRequestGate(포트) · JsoupArticleFetcher(본문 추출) · RedisArticleRequestGate(호스트별 요청 간격)
├─ cluster/     EmbeddingClient·ClusterLock·ClusterStore(포트) · ClusterAssigner(판정·락) · JdbcClusterStore(pgvector)
├─ enrich/      LlmClient·TransactionRunner(포트) · ClusterSummarizer(§3.4) · NewsProcessor(§3.2) · DigestProcessor(§4)
├─ persist/     StreamEventStore(포트) · JdbcStreamEventStore(upsert·payload 병합)
├─ sector/      SectorDirectory(포트) · JpaSectorDirectory(sector·stock_master 조회)
└─ publish/     StreamPublisher(포트) · RedisStreamPublisher
```

- 서버 → 서버 의존 금지 — worker-ingest는 `:contracts`에만, worker-llm은 `:contracts`·`:db-migrations`에 의존한다. JWT가 필요 없으므로 `:auth-jwt`는 의존하지 않는다.
- 포트는 도메인 패키지에 구현과 함께 둔다(`port/` 패키지로 몰지 않음). 도메인 클래스는 Lettuce·HTTP 클라이언트·LLM SDK를 직접 import하지 않는다 — `ClusterAssigner`·`ClusterSummarizer` 전이 로직이 인프라 없이 단위 테스트되어야 한다.

### 7.1 데이터 접근 — JPA 우선과 raw SQL 예외

[coding_convention.md](coding_convention.md) §7에 따라 worker-llm은 Spring Data JPA를 기본으로 쓰고, 아래 두 어댑터만 raw SQL 예외로 남긴다. 예외 근거는 여기가 단일 소유이며, 새 쿼리를 추가할 때도 먼저 JPA/JPQL로 표현되는지 확인한다.

| 어댑터 | 방식 | 근거 |
|---|---|---|
| `JpaSectorDirectory` (`sector`·`stock_master` 조회) | Spring Data JPA 파생 쿼리 | 엔티티 중심 단순 조회 — PostgreSQL 전용 기능 불필요 |
| `JdbcClusterStore` | native SQL | pgvector 거리 연산자(`<=>`)·`CAST(... AS vector)` 최근접 검색, `ON CONFLICT DO NOTHING/DO UPDATE` 업서트, `GREATEST` 부분 갱신, 상태 전이 CAS(`claimSummarize`·`markSummarized`) 조건부 UPDATE의 갱신 행 수 판정 |
| `JdbcStreamEventStore` | native SQL | `jsonb` 캐스팅·`jsonb_set` 부분 갱신·`payload -> 'digest' ->> 'date'` 경로 조회, 멱등 삽입 `ON CONFLICT DO NOTHING` |

- 두 예외 어댑터도 모든 입력값을 named parameter로 바인딩한다 — 문자열 연결로 값을 넣지 않는다. 동적으로 조립하는 부분은 종목 후보 유무에 따른 필터 **절 선택**뿐이고, 값은 항상 파라미터로 간다.
- JPA는 스키마를 소유하지 않는다. `ddl-auto=validate`로 엔티티 매핑과 `:db-migrations` Liquibase 스키마의 정합성만 검증한다(`stock_master.code`는 `CHAR(6)`이므로 엔티티에서 `@JdbcTypeCode(SqlTypes.CHAR)`로 맞춘다).
- JPA 트랜잭션 매니저 아래에서 native SQL 어댑터도 같은 커넥션에 참여한다 — §3.2 persist→publish→ack의 롤백 계약은 통합 테스트가 고정한다.

## 8. 설정 · 메트릭

provider는 명시 설정이고 자동 fallback이 없다. 엉뚱한 경로로 조용히 돌아가느니 기동에 실패하는 쪽을 택한다.

- LLM provider는 `anthropic|claude-cli|codex-cli|fake` 중 하나를 명시한다. 기본 프로파일은 `anthropic`, local 프로파일은 `claude-cli`이며 `LLM_PROVIDER=codex-cli`로 전환한다. provider 사이 자동 fallback은 없다.
- 로컬 무료 임베딩은 Ollama+BGE-M3를 기본으로 쓴다. 설치·환경변수·Docker 연결·문제 해결은 [로컬 임베딩 설정](local_embedding_setup.md)을 따른다.
- `anthropic`은 `ANTHROPIC_API_KEY`가 없으면 기동에 실패한다. `claude-cli`·`codex-cli`는 각각 로그인된 로컬 CLI가 필요하고, 실행 실패·타임아웃은 PEL 재처리 경로로 전파한다. `fake`는 `alphatalk.llm.allow-fake=true`일 때만 허용한다.
- CLI provider는 개인 구독 로컬 단일 인스턴스 전용이다. `consumer-batch=1`이 아니거나 CLI timeout이 `claim-idle` 이상이면 기동에 실패해, 긴 CLI 호출 중 다른 consumer가 아직 처리하지 않은 배치 레코드를 회수하는 구성을 막는다.
- 시크릿(환경변수): `ANTHROPIC_API_KEY` · `NAVER_CLIENT_ID/SECRET` · 임베딩 API 키. 로그 출력 금지. 임베딩 키는 `provider=rest`에서 fail-closed한다.
- 설정: 시드 종목 목록, 소스별 폴링 주기, 유사도 임계값(0.85), 클러스터 창(72h), 원문 허용 호스트·호스트별 요청 간격(기본 1초), digest 시각(18:00) — 임계값 튜닝에 대비해 전부 프로퍼티로 외부화한다.
- 메트릭: `ingest_fetched_total{source}` · `ingest_dup_skipped_total` · `queue_ingest_pending`(PEL, 기획안 §10 알람 항목) · `llm_processed_total{type}` · `llm_failed_total` · `cluster_merged_total` · `dlq_total` · `llm_tokens_total{model}`(비용 감시, NFR-09).
- 알람: PEL 적체 > N(기존 합의), DLQ 유입 > 0, 일 LLM 토큰 예산 초과.

```bash
SPRING_PROFILES_ACTIVE=local ./gradlew :worker-llm:bootRun
SPRING_PROFILES_ACTIVE=local LLM_PROVIDER=codex-cli ./gradlew :worker-llm:bootRun
SPRING_PROFILES_ACTIVE=local LLM_PROVIDER=fake ./gradlew :worker-llm:bootRun
```

## 9. 구현 단계 & DoD

> **상태(2026-07-24): N0~N6 전 단계 구현 완료.** LLM·임베딩은 포트 뒤에 있고 기본 프로파일은 API 키 미설정 시 fail-closed한다. local은 Claude/Codex CLI 구독을 고르고 test는 명시적 fake를 쓴다. 실서비스 투입 전 남은 것: 키 주입, RSS 소스 목록·시드 종목 설정(§10-5·§2.2), 임베딩 제공자 확정(§10-1).

| 단계 | 범위 | DoD |
|---|---|---|
| **N0** | 두 모듈 스캐폴딩 · settings.gradle 등록 · :contracts 상수(§6 잔여분) | `./gradlew :worker-ingest:test :worker-llm:test` 통과 |
| **N1** | ingest: RSS 1소스 → 정규화·seen → XADD *(사전 매핑은 구현 후 v0.6에서 제거)* | 동일 기사 재수집 시 큐 적재 0건(Testcontainers Redis) |
| **N2** | llm: 소비 → 요약·감성 → persist→publish→ack (클러스터링 없이 1기사=1이벤트) | **FR-11**: 동일 sourceId 중복 요약 0건 · XADD→`GET /rooms/{code}/stream` 노출 E2E |
| **N3** | 클러스터링(pgvector·락·편입 병합) + 네이버 검색 API 소스 | 동일 사건 3개 언론사 기사 → stream_event 1건 · `sources` 3건 · 편입 재발행 0건 |
| **N4** | 일일 다이제스트 | `digest:{code}:{date}` 멱등 — 잡 2회 적재에도 브리핑 1건 · 호재/악재 리스트 노출 |
| **N5** | 운영: DLQ·XPENDING/XCLAIM·메트릭·알람 | poison 5회 초과 → DLQ 격리 · PEL 알람 동작 |
| **N6** | 섹터·매크로(§3.6): scope 판정 · 섹터 fan-out · 다이제스트 sectorIssues/marketIssues — **선행: `sector`·`stock_master.sector_code` 적재(worker-batch `industry_sync`)** | 금리 인상 기사 1건 → 은행 섹터 커버 종목 각 방에 `scope=SECTOR` 이벤트 1건씩 · MARKET 기사는 방 이벤트 0건 + 다이제스트 반영 |

각 단계 = PR 1개(git_convention: scope=worker-ingest/worker-llm). `ClusterAssigner` 판정 로직은 refcount 규칙과 동급이다 — 단위 테스트 없는 변경 금지.

## 10. 오픈 이슈

아래는 아직 결정하지 않은 항목이다. 기본값으로 돌려두었을 뿐이므로, 실데이터가 쌓이면 재평가한다.

| # | 항목 | 선택지 · 기본값 |
|---|---|---|
| 1 | 임베딩 제공자 | Voyage `voyage-3.5-lite`(기본 제안) vs OpenAI `text-embedding-3-small` vs 로컬(KoSimCSE). 차원·비용·한국어 성능 비교 후 확정 — `vector(N)` 차원 연동 |
| 2 | 유사도 임계값·창 | 0.85 · 72h로 시작, 실데이터 오합류/미합류 사례로 튜닝 |
| 3 | digest 시각 | 18:00 장후(기본) vs 07:50 장전 브리핑 추가 — 둘 다 하려면 sourceId에 슬롯 포함(`digest:{code}:{date}:{am|pm}`) |
| 4 | 폴링 종목 확장 | 시드 41 → watchlist 합집합 ∪ 거래대금 상위 N. 네이버 API 일 한도 내 배분 설계 필요 |
| 5 | RSS 이용조건 확정 | 현재 9개 언론사의 공식 RSS를 사용한다. 상업 출시 전 언론사별 이용조건·제휴 필요 여부를 최종 확인 |
| 6 | 편입 시 클라 갱신 | 현재 재발행 없음(접속 중 클라는 `sources` 갱신을 못 봄). 필요해지면 갱신 전용 경량 이벤트 검토 — MVP 아님 |
| 7 | 섹터 분류 체계 | 기본: KIS 마스터 파일 업종 필드(`stock_master_sync`가 이미 파싱하는 소스). 세분화가 부족하면 KRX 업종분류/GICS 검토 — 판단 기준은 LLM 섹터 후보 목록의 품질 |
| 8 | SECTOR fan-out 파라미터 | 상한 100종목·impact LOW 제외로 시작, 실데이터로 튜닝 |
| 9 | MARKET 뉴스 실시간 노출면 | MVP는 다이제스트만. 홈 피드/시장 브리핑 방(종목 방 밖 노출면)은 별도 기획 필요 — P3 |
