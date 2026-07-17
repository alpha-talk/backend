# Alpha Talk — 뉴스 파이프라인 명세 v0.1
**worker-ingest · worker-llm · 담당: 팀원**

뉴스를 수집해 관련 종목 방으로 배달하고, 같은 사건을 다룬 여러 언론사 기사를 하나로 묶고, 매일 종목별 호재·악재 브리핑을 생성하는 파이프라인의 설계 기준. [redis_contract.md](redis_contract.md) §2(`queue:ingest`)와 [ws_api_spec.md](ws_api_spec.md) §4.3(stream payload)을 전제로 하며, 계약에 없는 항목은 §6에 **증보 제안**으로 모아 표시했다(합의 후 해당 계약 문서에 병합).

---

## 0. 개요 & 책임 경계

| 요구 | 대응 | 근거 FR |
|---|---|---|
| ① 신문사 크롤링 → 관련 종목 방 배달 | worker-ingest 수집·종목 매핑 → `queue:ingest` → worker-llm 요약 → `stream:{code}` | FR-11 (M1) |
| ② 유사 내용의 타 언론사 기사 묶기 | worker-llm 임베딩 클러스터링 — 클러스터당 스트림 이벤트 1건 | FR-04/05 노이즈 감소 |
| ③ 매일 호재·악재 AI 요약 | 일일 다이제스트 — `queue:ingest`에 digest 잡 적재 → worker-llm 생성 | 기획안 AI_SUMMARY(P3)의 뉴스 기반 조기 구체화 |

| 워커 | 책임 | 비책임 |
|---|---|---|
| **worker-ingest** | 소스 폴링(RSS·네이버 검색 API), 정규화·exact 중복 제거, 종목 후보 매핑, `XADD queue:ingest`, 일일 다이제스트 잡 적재(스케줄러) | LLM 호출, DB 쓰기(읽기는 허용), Pub/Sub 발행 |
| **worker-llm** | `XREADGROUP g:llm` 소비, 본문 확보, 클러스터 판정, LLM 요약·감성 분류, `stream_event` persist, `PUBLISH stream:{code}`, XACK, 일일 다이제스트 생성 | 수집·폴링, 스케줄(무상태 ×N 유지) |

**핵심 설계 결정** (근거는 각 절):

1. **클러스터링은 worker-llm에서, 임베딩 + pgvector로** — 수집 시점엔 비교 대상이 없고, 임베딩·LLM 접근이 llm 쪽에 응집돼 있다. (§3.3)
2. **클러스터당 `stream_event` 1건/종목** — 후속 편입 기사는 payload jsonb 병합만 하고 재발행하지 않는다(스트림 중복 노출 방지, core-api 명세 §12 노트 9와 동일 패턴). (§3.5)
3. **감성(호재/악재)은 (클러스터, 종목) 단위** — 같은 기사가 A사엔 호재·B사엔 악재일 수 있다. (§3.4)
4. **일일 다이제스트도 `queue:ingest`를 탄다** — ingest 스케줄러(싱글턴)가 `type=digest` 엔트리를 적재하고 llm-worker가 경쟁 소비. 신뢰성 장치(PEL·ACK·멱등)를 그대로 재사용하고 llm-worker의 무상태 ×N을 유지한다. (§4)
5. **본문은 LLM 입력으로만 쓰고, 저장은 발췌(200자)+원문 링크** — 기획안 §6 저작권 방침(요약+원문 링크) 준수. (§2.3)

---

## 1. 전체 파이프라인

```
[RSS 피드들]  [네이버 뉴스검색 API]                        (매일 18:00 KST)
      │              │                                        │
      ▼              ▼                                        ▼
┌─────────────────────────────┐                    ┌──────────────────────┐
│ worker-ingest               │                    │ ingest 스케줄러       │
│  폴링 → 정규화 → seen 체크   │                    │  당일 뉴스 있는 종목별 │
│  → 종목 후보 매핑(사전)      │                    │  digest 잡 생성       │
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

### 2.1 소스 전략

| 소스 | 방식 | 종목 매핑 | 비고 |
|---|---|---|---|
| 언론사 RSS (한경·매경·연합 등 경제 섹션) | 주기 폴링(5분), `If-Modified-Since`/ETag 활용 | 사전 매칭 필요(§2.4) | 제목+링크+요약만 제공 — 저작권 안전 |
| 네이버 뉴스 검색 API | 종목명 쿼리, 주기 폴링(10분) | 쿼리 자체가 종목 — 매핑 공짜 | 공식 오픈 API, **25,000건/일** 한도 |
| OpenDART 공시 *(후속)* | 목록 API 폴링 | 공시 자체에 종목 포함 | `type=disclosure`, 동일 경로 재사용 |

- 소스는 `NewsSource` 포트 뒤의 어댑터로 추가한다 — 소스 추가가 파이프라인 코드에 영향 없어야 한다.
- **크롤링 준법**: robots.txt 준수, 식별 가능한 User-Agent, 사이트별 요청 간격 제한. 본문 페이지 fetch는 worker-llm이 요약 직전 1회만 수행한다(수집 단계 대량 fetch 금지).
- 네이버 API 예산: 쿼리 대상 종목 × 폴링 횟수가 한도를 넘지 않게 설계. 시드 41종목 × 6회/시간 × 24h ≈ 5,900건/일로 여유. 전 종목(~2,600) 확장 시 수요 기반 선별 필요(§10 오픈 이슈).

### 2.2 폴링 대상 종목

MVP는 **설정 파일의 시드 종목 목록**(worker-price의 41종목과 동일 세트)으로 시작한다. 확장 시 후보: `watchlist` distinct 코드 합집합(워커의 core-api 테이블 읽기는 core-api의 워커 테이블 읽기와 대칭으로 허용) ∪ 거래대금 상위 N(`daily_candle`). RSS는 종목 무관 전체 수집이므로 이 목록과 무관하다.

### 2.3 정규화 · exact 중복 제거

| 단계 | 규칙 |
|---|---|
| URL 정규화 | 추적 파라미터(utm_* 등) 제거, 스킴·호스트 소문자화 |
| `sourceId` | `{source}:{기사 고유 ID}` — 고유 ID 없으면 정규화 URL의 SHA-256 앞 16자 |
| exact 중복 | `SETNX seen:ingest:{sourceId}` (TTL 7일) 실패 시 skip — 같은 기사 재수집 흡수 |
| 저장 범위 | 제목 + 리드 발췌(≤200자) + 원문 URL만 큐에 싣는다. 전문(全文)은 싣지도 저장하지도 않는다 |

**같은 사건을 다룬 다른 언론사 기사는 여기서 버리지 않는다** — sourceId가 다르므로 통과하고, 묶는 것은 worker-llm의 클러스터링(§3.3) 몫이다. exact 중복 제거는 "같은 기사 재수집"만 거른다.

### 2.4 종목 후보 매핑

| 단계 | 방법 | 담당 |
|---|---|---|
| 1차 | 소스가 종목을 알면 그대로(네이버 쿼리, DART 공시) | ingest |
| 2차 | `stock_master` 종목명 + 별칭 사전을 제목·발췌에 매칭(Aho-Corasick 또는 단순 contains — 종목 수천 개 수준에서 충분) | ingest |
| 3차 | LLM 요약 시 관련 종목 확정 — 오탐 제거(예: "삼성" 단독 매칭) 및 추가 종목 발견 | llm |

- 별칭 사전은 `stock_alias(code, alias)` 테이블(§5). 초기엔 정식 종목명 + 수동 등록 별칭("삼전" 등)으로 시작.
- 2차까지 후보 0건이면 **큐에 넣지 않는다**(배달할 방이 없음). 3차에서 LLM이 모든 후보를 기각해도 drop(§3.4).
- `codes` 필드는 큐 스키마(Redis 계약 §2.1) 그대로 콤마 구분 다중.

### 2.5 큐 적재

Redis 계약 §2.1 스키마를 그대로 사용한다: `source` · `sourceId` · `type` · `codes` · `title` · `url` · `body`(발췌) · `fetchedAt`. `XADD queue:ingest MAXLEN ~ 10000`.

---

## 3. worker-llm — 가공

### 3.1 소비 루프

- `XREADGROUP GROUP g:llm {consumerName} COUNT 1 BLOCK 5000 STREAMS queue:ingest >` — consumerName은 호스트명+PID.
- 유휴 회수: 주기적으로 `XAUTOCLAIM queue:ingest g:llm {me} min-idle-time=300000` — 죽은 워커의 PEL 엔트리 인계.
- **poison 엔트리**: delivery count > 5면 `queue:ingest:dlq`로 XADD 후 원큐 XACK + 알람 메트릭. (⚠️ §6 증보)

### 3.2 type=news 처리 순서

```
1. 멱등 체크: news_article에 sourceId 존재 && cluster_id 확정 && 클러스터 SUMMARIZED → 바로 XACK (재처리 흡수)
2. 본문 확보: 원문 URL fetch → 본문 추출(jsoup). 실패 시 제목+발췌만으로 진행(품질 저하 허용, 실패 아님)
3. 임베딩: 제목 + 리드 300자 → 벡터 (본문은 임베딩 후 폐기 — 저장 안 함)
4. 클러스터 판정 (§3.3) → 신규 생성 or 기존 편입, news_article INSERT(발췌만)
5-a. [신규] LLM 요약·감성(§3.4) → ① stream_event INSERT(종목별) → ② PUBLISH stream:{code} 각 종목 → ③ XACK
5-b. [편입] 대응 stream_event.payload에 sources 병합(재발행 없음) → XACK
     단, 편입 기사가 새 종목을 추가하면 그 종목에만 신규 INSERT+PUBLISH
```

중간 단계에서 죽으면 XACK 전이므로 PEL 재처리된다. 각 쓰기는 자연키 upsert(`sourceId`, `(cluster_id, code)`)라 재처리해도 중복이 없다 — FR-11 DoD(동일 sourceId 중복 요약 0건)를 이 구조가 보장한다.

### 3.3 클러스터링 — "비슷한 기사 하나로 묶기"

| 항목 | 값 | 근거 |
|---|---|---|
| 비교 대상 | 종목 교집합이 있는 최근 **72시간** 클러스터 | 다른 종목 기사끼리 오합류 방지, 오래된 사건과 분리 |
| 검색 | pgvector `embedding <=> :v` 최근접 5건 조회 | PG가 이미 있고 Flyway 단일 관리 원칙에 부합 — 별도 벡터 스토어 불요 |
| 판정 | 코사인 유사도 ≥ **0.85** → 최고 유사 클러스터에 편입, 미만 → 신규 클러스터 | 임계값은 실데이터로 튜닝(§10) |
| 지름길 | 정규화 제목 해시(언론사명·[속보]·특수문자 제거)가 기존 기사와 일치하면 임베딩 생략하고 그 클러스터에 편입 | 통신사 전재 기사(제목 거의 동일)가 다수 — 임베딩 호출 절약 |
| 동시성 | 판정~INSERT 구간을 `lock:cluster:{primaryCode}` 분산락(SET NX PX 3000)으로 직렬화. LLM 호출은 락 밖 | llm-worker ×N이 동일 사건 기사를 동시 처리하면 클러스터가 중복 생성됨 |

클러스터 상태: `NEW`(생성 직후) → `SUMMARIZED`(요약·발행 완료). 요약 전에 워커가 죽으면 재처리 시 `NEW` 클러스터를 발견하고 요약부터 재개한다.

### 3.4 LLM 요약 · 호재/악재 분류

한 클러스터당 LLM 1회 호출(묶인 기사 N건이어도 1회 — 클러스터링이 곧 비용 절감). 구조화 출력(tool use)으로 스키마를 강제한다.

**입력**: 대표 기사 본문(확보 시) + 클러스터 내 기사 제목들 + 후보 종목 목록(코드·종목명)
**출력 스키마**:

```json
{
  "summary": "3줄 요약 (각 줄 ≤ 80자)",
  "stocks": [
    { "code": "005930", "relevant": true, "sentiment": "POSITIVE|NEGATIVE|NEUTRAL", "confidence": 0.0~1.0, "reason": "한 줄" }
  ]
}
```

- `relevant=false`인 후보는 제외(사전 매칭 오탐 제거). 전부 false면 클러스터를 `IRRELEVANT`로 마킹하고 발행 없이 XACK.
- confidence < 0.6이면 sentiment를 NEUTRAL로 강등 — 애매한 건 호재/악재로 단정하지 않는다.
- 모델: 클러스터 요약은 **claude-haiku-4-5**(건수 많음·단순), 일일 다이제스트는 **claude-sonnet-5**(하루 종목당 1회·종합 판단). `LlmClient` 포트 뒤라 교체 자유.
- 비용 추정: 시드 41종목 기준 일 ~500기사 → ~150클러스터 × ~2K tokens(Haiku) + 41다이제스트 × ~3K tokens(Sonnet) — 월 수 달러 수준.
- 워커 내 재시도는 백오프 1회만 — 그 이상은 PEL 재처리에 맡긴다(이중 재시도 루프 금지).

### 3.5 발행 정책

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

`stream_event.payload`는 JSONB라 core-api 스키마 변경 없이 그대로 조회에 노출된다(`GET /rooms/{code}/stream`).

---

## 4. 일일 다이제스트 — 호재/악재 브리핑

### 4.1 트리거와 멱등

- **ingest 스케줄러**가 매일 **18:00 KST**(장 마감 후)에: 윈도 `[전일 18:00, 당일 18:00)` 에 기사가 편입된 클러스터가 있는 종목마다 `XADD queue:ingest type=digest codes={code} sourceId=digest:{code}:{yyyy-MM-dd}`.
- llm-worker가 같은 그룹(`g:llm`)으로 경쟁 소비 — 스케줄은 싱글턴(ingest), 실행은 ×N(llm)으로 분리돼 리더 선출이 필요 없다.
- 멱등 키 `digest:{code}:{date}` → `stream_event` upsert. 재처리·중복 적재에도 브리핑은 하루 1건.

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
    "neutralCount": 4,
    "newsCount": 12
  },
  "occurredAt": 1752654000000
}
```

- `eventId` 참조로 클라가 브리핑에서 원 뉴스 이벤트로 점프할 수 있다.
- 프롬프트에 면책 고정: 투자 판단의 근거가 아니라 정보 요약임을 명시(기획안 FR-17 면책 방침과 동일 기조). 클라 노출 문구는 클라 몫.
- persist → publish(`stream:{code}`) → XACK — 뉴스와 동일 불변식.

---

## 5. DB 스키마 (Flyway 신규 마이그레이션, 워커 소유 — core-api는 읽기 전용)

```sql
CREATE EXTENSION IF NOT EXISTS vector;

news_cluster(
  id CHAR(26) PK,                -- ULID
  rep_title TEXT, summary TEXT NULL,
  status TEXT,                   -- NEW | SUMMARIZED | IRRELEVANT
  first_published_at, last_article_at, article_count INT, created_at
)
news_article(
  id BIGSERIAL PK,
  source TEXT, source_id TEXT UNIQUE,   -- 멱등 키
  url TEXT, title TEXT, excerpt VARCHAR(200),   -- 전문 저장 금지
  title_hash CHAR(16),                  -- 전재 기사 지름길(§3.3)
  embedding vector(1024) NULL,
  published_at, fetched_at, cluster_id CHAR(26) FK NULL
)
news_cluster_stock(
  cluster_id FK, code CHAR(6),
  sentiment TEXT, confidence NUMERIC(3,2),
  stream_event_id CHAR(26) NULL,        -- 편입 시 payload 병합 대상
  PK(cluster_id, code)
)
stock_alias(code CHAR(6), alias TEXT, PK(code, alias))

-- INDEX news_article (embedding vector_cosine_ops) ivfflat · (title_hash) · (published_at)
-- INDEX news_cluster (last_article_at) — 72h 창 후보 조회
```

⚠️ pgvector는 PostgreSQL 확장 — docker-compose 이미지 `postgres:16` → `pgvector/pgvector:pg16` 교체, Testcontainers 이미지도 동일 변경 필요. `vector(1024)` 차원은 임베딩 제공자 확정 시(§10) 함께 확정.

---

## 6. 계약 증보 제안 ⚠️

**아래는 현행 계약(redis_contract v0.2 · ws_api_spec v0.4)에 없는 신규 제안이다. 팀 합의 후 각 문서에 병합하고 나서 구현한다** (CLAUDE.md: 새 채널/키는 코드보다 문서 먼저).

| 대상 문서 | 항목 | 내용 |
|---|---|---|
| redis_contract §2.1 | `type` 값 추가 | `"digest"` — 일일 브리핑 잡. `sourceId=digest:{code}:{date}`, title/url/body 공란 |
| redis_contract §2/§3 | `queue:ingest:dlq` | poison 엔트리 격리(Stream, 소비자 없음 — 수동 점검) |
| redis_contract §3 | `lock:cluster:{code}` | 클러스터 판정 직렬화 락, TTL 3s, llm-worker 전용 |
| ws_api_spec §4.3 | stream payload 필드 추가 | `sentiment`(news) · `sources[]`(news) · `digest{}`(ai) — 전부 optional, 기존 클라 파싱 비파괴 |
| :contracts | 상수 추가 | `Queues.INGEST`·`Queues.INGEST_DLQ`·그룹명 `g:llm`, `Keys.seenIngest(sourceId)`·`Keys.clusterLock(code)`, payload 필드명 |

---

## 7. 모듈 구조 & 포트 (저장소 DIP 컨벤션)

```
worker-ingest/
├─ scheduler/   IngestPoller(소스 폴링 오케스트레이션) · DigestTrigger(§4.1)
├─ source/      NewsSource(포트) · RssNewsSource · NaverSearchNewsSource
├─ mapping/     StockCodeMapper(포트) · DictionaryStockCodeMapper(stock_master+stock_alias)
├─ dedup/       SeenMarker(포트) · RedisSeenMarker
└─ queue/       IngestQueue(포트) · RedisIngestQueue(XADD)

worker-llm/
├─ consume/     IngestConsumer(XREADGROUP 루프 · XAUTOCLAIM · DLQ 격리)
├─ article/     ArticleFetcher(포트) · JsoupArticleFetcher(본문 추출)
├─ cluster/     EmbeddingClient(포트) · ClusterAssigner(판정·락) · PgVectorClusterStore
├─ enrich/      LlmClient(포트) · ClusterSummarizer(§3.4) · DailyDigestWriter(§4)
├─ persist/     StreamEventWriter(upsert·payload 병합) · NewsRepository
└─ publish/     StreamPublisher(포트) · RedisStreamPublisher
```

- 두 워커 모두 `:contracts`에만 의존(서버 → 서버 의존 금지). JWT 불요 — `:auth-jwt` 의존하지 않는다.
- 포트는 도메인 패키지에 구현과 함께 둔다(`port/` 패키지로 몰지 않음). 도메인 클래스는 Lettuce·HTTP 클라이언트·LLM SDK를 직접 import하지 않는다 — `ClusterAssigner`·`ClusterSummarizer` 전이 로직이 인프라 없이 단위 테스트되어야 한다.

## 8. 설정 · 메트릭

- 시크릿(환경변수): `ANTHROPIC_API_KEY` · `NAVER_CLIENT_ID/SECRET` · 임베딩 API 키. 로그 출력 금지.
- 설정: 시드 종목 목록, 소스별 폴링 주기, 유사도 임계값(0.85), 클러스터 창(72h), digest 시각(18:00) — 전부 프로퍼티로 외부화(임계값 튜닝 대비).
- 메트릭: `ingest_fetched_total{source}` · `ingest_dup_skipped_total` · `queue_ingest_pending`(PEL, 기획안 §10 알람 항목) · `llm_processed_total{type}` · `llm_failed_total` · `cluster_merged_total` · `dlq_total` · `llm_tokens_total{model}`(비용 감시, NFR-09).
- 알람: PEL 적체 > N(기존 합의), DLQ 유입 > 0, 일 LLM 토큰 예산 초과.

## 9. 구현 단계 & DoD

| 단계 | 범위 | DoD |
|---|---|---|
| **N0** | 두 모듈 스캐폴딩 · settings.gradle 등록 · :contracts 상수(§6 합의 후) | `./gradlew :worker-ingest:test :worker-llm:test` 통과 |
| **N1** | ingest: RSS 1소스 → 정규화·seen·사전 매핑 → XADD | 동일 기사 재수집 시 큐 적재 0건(Testcontainers Redis) |
| **N2** | llm: 소비 → 요약·감성 → persist→publish→ack (클러스터링 없이 1기사=1이벤트) | **FR-11**: 동일 sourceId 중복 요약 0건 · XADD→`GET /rooms/{code}/stream` 노출 E2E |
| **N3** | 클러스터링(pgvector·락·편입 병합) + 네이버 검색 API 소스 | 동일 사건 3개 언론사 기사 → stream_event 1건 · `sources` 3건 · 편입 재발행 0건 |
| **N4** | 일일 다이제스트 | `digest:{code}:{date}` 멱등 — 잡 2회 적재에도 브리핑 1건 · 호재/악재 리스트 노출 |
| **N5** | 운영: DLQ·XAUTOCLAIM·메트릭·알람 | poison 5회 초과 → DLQ 격리 · PEL 알람 동작 |

각 단계 = PR 1개(git_convention: scope=worker-ingest/worker-llm). `ClusterAssigner` 판정 로직은 refcount 규칙과 동급 — 단위 테스트 없는 변경 금지.

## 10. 오픈 이슈

| # | 항목 | 선택지 · 기본값 |
|---|---|---|
| 1 | 임베딩 제공자 | Voyage `voyage-3.5-lite`(기본 제안) vs OpenAI `text-embedding-3-small` vs 로컬(KoSimCSE). 차원·비용·한국어 성능 비교 후 확정 — `vector(N)` 차원 연동 |
| 2 | 유사도 임계값·창 | 0.85 · 72h로 시작, 실데이터 오합류/미합류 사례로 튜닝 |
| 3 | digest 시각 | 18:00 장후(기본) vs 07:50 장전 브리핑 추가 — 둘 다 하려면 sourceId에 슬롯 포함(`digest:{code}:{date}:{am|pm}`) |
| 4 | 폴링 종목 확장 | 시드 41 → watchlist 합집합 ∪ 거래대금 상위 N. 네이버 API 일 한도 내 배분 설계 필요 |
| 5 | RSS 소스 목록 확정 | 언론사별 RSS 제공 여부·약관 확인 후 목록 픽스 |
| 6 | 편입 시 클라 갱신 | 현재 재발행 없음(접속 중 클라는 `sources` 갱신을 못 봄). 필요해지면 갱신 전용 경량 이벤트 검토 — MVP 아님 |
