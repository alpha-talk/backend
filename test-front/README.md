# test-front — RSS → WS E2E 수동 테스트

실제 RSS 기사가 `worker-ingest → queue:ingest → worker-llm → Redis stream:{code} → ws → 브라우저`로 흐르는지 눈으로 확인하는 테스트 도구. **로컬 전용** — 운영 배포 대상이 아니다.

- `index.html` — STOMP 테스트 클라이언트 (외부 라이브러리 없음, JWT를 브라우저에서 직접 서명)
- `seed-stocks.sql` — `stock_master`·`sector` 임시 시드 (worker-batch 착수 전 대역)

## 사전 조건

- Docker (compose로 Redis·PostgreSQL)
- Claude CLI 로그인 (`claude` — worker-llm local 프로파일의 LLM provider)
- Ollama + bge-m3 (임베딩): `ollama pull bge-m3` 후 `ollama serve` — 상세는 [md/local_embedding_setup.md](../md/local_embedding_setup.md)

## 실행 순서

```bash
# 1. 인프라
docker compose up -d

# 2. 시드 — stock_master·sector (worker-llm이 fail-closed로 요구)
docker exec -i alphatalk-postgres psql -U alphatalk alphatalk < test-front/seed-stocks.sql

# 3. 관심목록 — ws가 CONNECT 시 이 키로 해소해 /user/queue/stream에 흘린다
docker exec alphatalk-redis redis-cli SADD watchlist:1 005930 000660 373220 005380 000270 035420 035720 051910 005490 105560
```

터미널 3개에서 각각:

```bash
./gradlew :worker-ingest:bootRun --args='--spring.profiles.active=local'
./gradlew :worker-llm:bootRun --args='--spring.profiles.active=local'
./gradlew :ws:bootRun --args='--spring.profiles.active=local'
```

프론트 열기:

```bash
open test-front/index.html
```

기본값(엔드포인트 `ws://localhost:8081/ws`, local 시크릿, userId 1) 그대로 **연결** 버튼 → `/user/queue/stream`·`/user/queue/quote` 자동 구독. 이후 ingest 폴링(기동 5초 뒤 첫 폴링, 이후 5분 주기)이 기사를 넣고 worker-llm이 요약을 마치면 카드가 뜬다.

> local 프로파일은 피드 3개(연합·한경 경제, 매경 증권)로 축소돼 있다(첫 폴링 물량 제한 — LLM 호출은 클러스터당 1회, claude-cli는 직렬 처리라 전체 소진에 수십 분 걸릴 수 있다). 관심목록에 있는 종목의 기사가 요약될 때만 카드가 온다는 점에 주의.

## 단계별 확인 (이벤트가 안 올 때 어디서 멈췄는지)

```bash
# ① ingest가 큐에 넣었나
docker exec alphatalk-redis redis-cli XLEN queue:ingest

# ② llm이 소비 중인가 (PEL·미소비 잔량)
docker exec alphatalk-redis redis-cli XINFO GROUPS queue:ingest

# ③ 클러스터·요약 상태
docker exec alphatalk-postgres psql -U alphatalk -c "SELECT status, count(*) FROM news_cluster GROUP BY status"
docker exec alphatalk-postgres psql -U alphatalk -c "SELECT code, sentiment, rejected, stream_event_id IS NOT NULL AS published FROM news_cluster_stock ORDER BY cluster_id DESC LIMIT 20"

# ④ 발행이 나가는가 (실시간 감시 — 프론트가 받는 것과 동일한 채널)
docker exec alphatalk-redis redis-cli PSUBSCRIBE 'stream:*'

# ⑤ DLQ에 격리된 게 있나 (5회 초과 실패)
docker exec alphatalk-redis redis-cli XLEN queue:ingest:dlq
```

## 자주 걸리는 것

| 증상 | 원인 |
|---|---|
| 연결 직후 끊김 | ws가 local 프로파일이 아니어서 시크릿 불일치, 또는 ws 미기동 |
| 연결은 되는데 카드 0건 | 관심목록(`watchlist:1`)을 **연결 후에** 넣었다 — 해소는 CONNECT 시점 1회. 넣고 재연결 |
| ③에서 `SUMMARIZING`이 안 줄어듦 | claude CLI 미로그인 / Ollama 미기동 — worker-llm 로그 확인, PEL 재시도가 계속 도는 상태 |
| `relation "sector" does not exist` | 2번 시드 SQL을 안 돌렸다 (fail-closed 정상 동작) |
| 카드가 왔는데 sentiment 없음 | `confidence < 0.6` NEUTRAL 강등 또는 편입 이벤트 — 정상 |

`quote` 큐는 worker-price가 아직 없으므로 항상 조용한 게 정상이다.
