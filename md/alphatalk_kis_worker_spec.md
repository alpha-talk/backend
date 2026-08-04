# Alpha Talk — KIS 수집 워커 명세 v0.2
**worker-price · worker-batch · `:kis-client` 공유 라이브러리 · 담당: 민균**

> **v0.2 (2026-07-27)**: 증권사별 투자의견 수집·소식 발행 추가. 회원사 코드별 `FHKST663400C0` 조회 → immutable observation 적재 → 기존 `eventId`를 재사용하는 at-least-once Pub/Sub 통보로 확정. WS payload는 v0.6의 하위 호환 확장을 사용한다.

worker-price·worker-batch와 두 워커가 공유하는 `:kis-client`의 구현 기준 문서다. KIS 인증·유량 정책(§1), 실시간 구독·conflation(§2), 배치 잡 카탈로그(§3), 워커 적재 테이블 스키마(§4)를 여기서 결정한다. 서비스 간 Redis 채널·키는 [redis_contract.md](redis_contract.md)가 단일 진실이고, 클라이언트가 받는 payload 구조는 [ws_api_spec.md](ws_api_spec.md)가 소유한다 — 이 문서는 워커 내부 동작과 KIS 연동 세부를 다룬다. 두 워커를 구현·수정하기 전에 해당 절부터 확인한다.

---

## 0. 개요 & 책임 경계

| 워커 | 책임 | 비책임 |
|---|---|---|
| **worker-price** | KIS WS 실시간 틱 수집 → conflation → `quote:{code}` 발행 + `price:{code}` 캐시. (선택) trade/depth. 용량 초과분 REST 폴링 강등. 봉(OHLCV) 수집·백필 | 뉴스 수집(=ingest), 요약(=llm), DB 글 저장(=core-api) |
| **worker-batch** | 종목마스터·수급·밸류에이션·투자의견 (KIS), 재무 (OpenDART) 스케줄 적재. 신규 투자의견 observation은 `stream_event` 저장 후 `stream:{code}`로 직접 통보(§3.3) | 실시간 시세 처리, 투자의견 전달 보장(Redis Pub/Sub은 best-effort) |
| **`:kis-client`** | 두 워커가 공유: 토큰/Approval 관리, 레이트리미터, REST/WS 클라이언트, 프레임 파서 | 비즈니스 로직 |

설계 원칙은 둘이다. 첫째, **틱은 영속화하지 않는다** — 클라이언트는 최신 스냅샷을 덮어쓰며 소비하고 과거 시세는 봉으로 조회하므로, 틱 원본 저장은 Redis 스냅샷 + 봉(OHLCV)으로 대체한다. 둘째, 시세 계열(실시간·봉)은 price로, 비시세(마스터·재무·수급)는 batch로 응집한다.

---

## 1. KIS OpenAPI 공통 (`:kis-client`)

### 1.1 환경·도메인

| 환경 | REST | WebSocket |
|---|---|---|
| 실전 (prod) | `https://openapi.koreainvestment.com:9443` | `ws://ops.koreainvestment.com:21000` |
| 모의 (vts) | `https://openapivts.koreainvestment.com:29443` | `ws://ops.koreainvestment.com:31000` |

`KIS_ENV=vts|prod`로 전환한다. 개발·스테이징은 모의 고정.

### 1.2 인증 수명주기

토큰 발급 자체에 유량 제한(1분당 1회)이 있어 여러 인스턴스가 동시에 발급을 시도하면 안 된다. 그래서 발급은 분산락 안에서만 하고, 발급된 토큰은 Redis에 캐시해 공유한다.

| 항목 | 값 | 대응 설계 |
|---|---|---|
| 접근토큰 | `POST /oauth2/tokenP` `{grant_type:"client_credentials", appkey, appsecret}` → `access_token` **24h 유효** | Redis `kis:token:{keyId}` 캐시(TTL=만료−5분) |
| 재발급 제한 | **1분당 1회** (+`tokenP` 자체 유량 1건/초) | 발급은 분산락 `SET kis:token:lock:{keyId} NX PX 5000` 안에서만. 락 실패 시 짧게 대기 후 캐시 재조회 |
| WS 접속키 | `POST /oauth2/Approval` `{grant_type:"client_credentials", appkey, secretkey}` → `approval_key` | 세션 연결 시 발급, 계정별 보관 |
| 토큰 만료 응답 | REST 401/토큰 오류 코드 수신 시 | 캐시 무효화 → 재발급(락) → **1회만** 재시도 |

`keyId` = appkey 해시 앞 8자. 로그·키에 appkey 원문을 쓰지 않는다.

### 1.3 유량 제한 & 레이트리미터

KIS 유량은 슬라이딩 윈도로 측정되는 것으로 알려져 있다. 공식 한도에 딱 맞춰 호출하면 윈도 경계에 호출이 몰릴 때 초과가 난다. 그래서 내부 한도는 **공식의 75%**(실전 15/s, 모의 1.5/s)로 잡는다.

| 구분 | 한도 | 비고 |
|---|---|---|
| REST | **실전 20건/초 · 모의 2건/초 (계좌 단위)** | 내부 한도는 공식의 75% — 실전 15/s, 모의 1.5/s (근거는 위) |
| WS 등록 | **1세션 합산 41건, 계좌(앱키)당 1세션** | 국내/해외·체결가/호가 등 전 실시간 합산 41 |
| ⚠️ 정책 변동 | 포털 공지 "신규 고객 초당 호출 제한 안내(2026-03-20)" | **구현 착수 전 원문 확인** 후 본 표 갱신 (§9 오픈 이슈) |

구현: Resilience4j `RateLimiter`를 **계정(keyId) 단위**로 생성하고 모든 REST 호출이 이를 통과한다. 429/유량 오류를 받으면 지수 백오프 후 재시도하고 메트릭 `rest_throttled`를 올린다.

프로세스별 limiter만으로는 부족하다. `worker-price`와 `worker-batch`가 같은 계정을 공유하면 각자 한도를 지켜도 합산 유량이 한도를 넘는다. 그래서 `:kis-client`에 `KisRateGate` 포트를 두고, 서버는 Redis 토큰 버킷 `rate:kis-rest:{keyId}` 구현을 주입한다. 모든 일반 REST 시세·배치 호출은 **로컬 smoothing limiter → 공용 Redis gate** 순서로 통과한다. 토큰 버킷은 Lua로 `{tokens, updatedAt}` 계산·차감을 원자화한다. 파라미터는 실전 `capacity=15, refill=15/s`, 모의 `capacity=2, refill=1.5/s`, 마지막 소비 후 TTL 2분. permit이 없으면 다음 충전 시각까지 기다리고, 호출별 타임아웃을 넘으면 실패 처리한다. 토큰 발급의 1분 가드와 Approval 발급은 §1.2의 별도 제한을 따른다.

### 1.4 REST 공통 헤더

`content-type: application/json` · `authorization: Bearer {token}` · `appkey` · `appsecret` · `tr_id: {TR}` · `custtype: P`

---

## 2. worker-price (실시간)

### 2.1 수요(demand) 정의와 신호 — Redis 계약 v0.12에 반영됨

WS 구독 용량이 유한하므로(§1.3) 전 종목이 아니라 수요가 있는 종목만 구독한다. **수요 = ⋃(접속 중 유저의 관심목록) ∪ (입장 중인 방)** (기획안 §2.5 검증 #2). 세션·관심목록 해소·방 토픽 구독을 모두 아는 쪽은 게이트웨이다. 그래서 카운트는 게이트웨이가 유지하고 worker-price는 소비만 한다.

> ✅ **2026-08-04 [Redis 계약](redis_contract.md) v0.12 §1.3·§3에 반영·구현됨.** 단일 진실은 계약 문서다 — 아래 표는 요약이며 어긋나면 계약 문서를 따른다.

| 키/채널 | 타입 | 쓰기 | 읽기 | 내용 |
|---|---|---|---|---|
| `demand:quote:{gwId}` | Hash `{code: refCount}` | 게이트웨이(스냅샷 전체 재기록) | worker-price | 접속 세션의 관심목록 기준 참조 수 |
| `demand:room:{gwId}` | Hash `{code: refCount}` | 게이트웨이(스냅샷 전체 재기록) | worker-price | 방 토픽 구독(입장) 기준 — trade/depth·우선순위 판단 |
| `demand:updated` | Pub/Sub | 게이트웨이 | worker-price | `{"kind":"quote|room","code":"005930","active":true,"ts":...}` — 종목 참조수 0↔1 전이 시만 발행 |
| `gw:alive:{gwId}` | String TTL 15s | 게이트웨이(하트비트) | worker-price | 살아있는 게이트웨이 식별. 죽은 gwId의 해시는 수요 합산에서 제외(스테일 정리) |

- 게이트웨이 인메모리 수요 변경 시점: CONNECT 후 첫 SUBSCRIBE에서 관심목록 부착 / 마지막 세션 DISCONNECT에서 관심목록 제거 / `watchlist:updated` diff 반영 / 방 토픽 SUBSCRIBE·UNSUBSCRIBE에서 room 증감. 종목 수요의 0↔1 전이는 즉시 동기화를 트리거하고, 5초 주기 동기화가 refcount 변화와 실패·유실을 보정한다.
- worker-price 소비는 두 경로다. ① `demand:updated` 구독으로 즉시 반영한다. ② **60초마다 전체 리컨실**(`gw:alive` 스캔 → 살아있는 gw들의 해시 HGETALL 합산 → 목표 구독 집합 재계산)로 메시지 유실·스테일을 자기치유한다.
- **구현 확정(v0.12)**: `{gwId}`는 생략하지 않고 게이트웨이 부팅마다 새로 발급한다. 단일 동기화 스레드가 5초 주기와 0↔1 전이 트리거마다 `DemandRegistry` 스냅샷을 Lua `DEL+HSET`으로 원자적 전체 재기록하고 해시 TTL 60초·`gw:alive` TTL 15초를 갱신한다. worker-price는 `demand:updated`를 리컨실 트리거로만 쓰고 목표 집합은 항상 alive gw 해시 합산으로 재계산한다.

### 2.2 세션 풀 & 구독 배정

- WS 등록 한도는 세션당 41건이고 **등록 단위는 (tr_id, 종목)**이다. 실전은 심볼당 2건(통합 체결가+시간외 체결가, §2.3)을 등록하므로 심볼 용량 `C = ⌊41 / 2⌋ × 계정 수 = 20 × 계정 수`, 모의는 1건이라 `41 × 계정 수`. 계정 목록은 `KIS_ACCOUNTS`(JSON 배열: keyId·appkey·appsecret)로 주입한다.
- 시간외 TR을 상시 등록하는 이유: 장중/시간외 경계에서 TR을 갈아끼우면 하루 두 번 대량 재구독·재배정 플래핑이 생기고, 경계 시각에 용량이 출렁여 강등이 요동친다. 용량 절반을 내주고 등록을 고정하는 쪽을 택한다 — 초과 수요는 어차피 REST 폴링 강등(§2.5)이 받는다. 시간대별 TR 스왑은 계정 추가로도 부족해질 때의 후속 최적화로 남긴다.
- 목표 집합은 리컨실마다 산출한다. `rooms ∪ topN(quote)`이 C를 넘으면 **우선순위: ① 입장 방 ② quote refCount 내림차순**으로 자른다. 탈락분은 REST 폴링으로 강등한다(§2.5).
- 배정: `종목 → (세션, 슬롯)` 맵 + 역인덱스. 신규는 빈 슬롯 최다 세션에 배정하고 해지는 슬롯을 반납한다. 잦은 재배정(플래핑)을 막으려고 **해지는 30초 유예**한다 — 그 사이 재수요가 오면 취소.
- 세션 상태머신: `DISCONNECTED → CONNECTING(Approval 발급) → CONNECTED(구독 리플레이) → DEGRADED(오류 누적)`. 재접속은 지수 백오프+지터. 성공하면 그 세션 배정분 전체를 재구독한다.

### 2.3 KIS WS 프로토콜

**구독 TR 세트** — env별로 다르다. 실전(prod)은 심볼당 2건을 등록해 KRX·NXT 전 세션을 커버한다:

| tr_id | 내용 | 수신 시간대(KST) |
|---|---|---|
| `H0UNCNT0` | 실시간 체결가 (KRX+NXT **통합**) | NXT 프리 08:00–08:50 · KRX/NXT 메인 09:00–15:30 · NXT 애프터 15:30–20:00 |
| `H0STOUP0` | **시간외** 실시간 체결가 (KRX) | 장후 시간외종가 15:40–16:00 · 시간외단일가 16:00–18:00 |

모의투자(vts)는 통합·시간외 TR 지원이 미확인이라 `H0STCNT0`(KRX 정규장 체결가)만 등록한다(§9). KRX 전용 `H0STCNT0`은 NXT 체결분이 빠지므로 실전에서는 쓰지 않는다.

16:00–18:00에는 NXT 애프터(`H0UNCNT0`)와 KRX 시간외단일가(`H0STOUP0`) 체결이 같은 종목에 교차 유입될 수 있다. quote는 최신 체결 스냅샷이므로(§2.4 conflation) 거래소 교차 덮어쓰기는 통합 수신의 정상 동작이다.

**구독/해지 요청 (JSON 텍스트 프레임)** — `tr_type` 1=등록, 2=해제:

```json
{ "header": { "approval_key": "...", "custtype": "P", "tr_type": "1", "content-type": "utf-8" },
  "body": { "input": { "tr_id": "H0UNCNT0", "tr_key": "005930" } } }
```

**수신 프레임 2종**
1. **제어(JSON)**: 구독 성공/실패 응답, `PINGPONG` — PINGPONG 수신 시 **동일 프레임 그대로 에코**한다. 에코하지 않으면 서버가 세션을 끊는다.
2. **데이터(파이프 구분 텍스트)**: `암호화유무|TR_ID|데이터건수|응답데이터`
    - 첫 필드 `0`=평문, `1`=AES 암호화(체결통보 등 — 본 서비스 미사용 TR. 파서는 플래그 분기만 두고 `1`은 드랍+경고).
    - 응답데이터는 `^` 구분 필드, 건수>1이면 레코드가 이어 붙음(필드 수로 분할).
      **체결가 TR 주요 필드 인덱스** — 세 TR(H0STCNT0·H0UNCNT0·H0STOUP0)의 필드 수는 공식 open-trading-api columns 대조 기준 **46·46·43**이다. H0STCNT0과 H0UNCNT0은 idx 21 필드명(CCLD_DVSN/CNTG_CLS_CODE — 같은 체결구분)만 다르고 순서 동일, H0STOUP0은 앞 43필드가 같고 뒤 3필드(HOUR_CLS_CODE·MRKT_TRTM_CLS_CODE·VI_STND_PRC)만 없다. 파서가 쓰는 idx 0~13은 셋이 완전히 일치하므로 인덱스 매핑 하나를 공유한다. 파싱 상수는 `:contracts`가 아닌 `:kis-client`에 고정:

| idx | 필드 | 매핑 |
|---|---|---|
| 0 | MKSC_SHRN_ISCD | code |
| 1 | STCK_CNTG_HOUR (HHMMSS) | 체결시각 |
| 2 | STCK_PRPR | price |
| 3 / 4 / 5 | 대비부호 / PRDY_VRSS / PRDY_CTRT | change·changeRate |
| 7 / 8 / 9 | STCK_OPRC / HGPR / LWPR | open·high·low |
| 12 / 13 | CNTG_VOL / ACML_VOL | 체결량·volume(누적) |

> 전체 필드 순서는 공식 open-trading-api columns 기준 전 필드 픽스처(46·43필드, 다건 이어붙임 포함)로 단위 테스트에 고정했고, **실 수신 프레임 캡처**로 재확정한다(§9). `prevClose`는 틱에 없음 → `price − PRDY_VRSS`로 산출.

H0STASP0(실시간 호가)는 P3 `depth` 확장 시 동일 구조로 추가한다.

### 2.4 Conflation & 발행 (Redis 계약 §1.1 이행)

클라이언트는 최신 스냅샷을 덮어쓰며 소비하므로 모든 틱을 그대로 중계할 필요가 없다. 종목별 최신값만 버퍼에 남기고 주기마다 변경분을 묶어 발행한다(conflation). 중간 틱 누락은 정상이다.

```
KIS 프레임 → 파싱 → 종목별 최신값 버퍼(덮어쓰기)
   └─ 100~250ms 주기 플러시(변경분만):
        ① HSET price:{code} price prevClose change changeRate open high low volume ts   (+선택 EXPIRE 없음)
        ② PUBLISH quote:{code} {"type":"quote","code":"005930","ts":...,"data":{...WS §4.2와 동일}}
```

- 봉투·필드는 `:contracts` DTO 사용. `eventId` 없음(스냅샷성 — 계약 §1.2).
- 발행 실패(Redis 순단)는 드랍을 허용한다 — 다음 틱이 대체한다.

### 2.5 REST 스냅샷 폴링 (강등 경로 + 장전 워밍)

- **주식현재가 시세**: `GET /uapi/domestic-stock/v1/quotations/inquire-price` · TR `FHKST01010100` · `FID_COND_MRKT_DIV_CODE=J`, `FID_INPUT_ISCD={code}` → `stck_prpr, prdy_vrss, prdy_ctrt, stck_oprc/hgpr/lwpr, acml_vol` (+`per, pbr, eps, bps` — batch가 활용).
- 결과는 §2.4와 **동일한 캐시+발행 경로**로 흘린다(`ts` 간격만 큼). 클라·게이트웨이는 WS 수신과 폴링을 구분하지 못한다.
- 폴링 대상은 강등 심볼. 예산은 계정 유량의 50% → 실전 1계정 기준 7.5콜/s ≈ 심볼 225개를 30s 주기로 커버한다. 강등조차 초과하면 refCount 하위는 60s 주기로 늦춘다.
- 구독 시작 직전(07:55) 수요 심볼을 1회 워밍 폴링해 입장 스냅샷 공백을 막는다.

### 2.6 봉(OHLCV) 수집·백필

- **기간별 시세**: `GET /uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice` · TR `FHKST03010100` · `FID_INPUT_DATE_1/2`(범위), `FID_PERIOD_DIV_CODE=D`, `FID_ORG_ADJ_PRC=0`(수정주가) → **1콜 최대 100봉**.
- 정기: 매 영업일 16:30 전 상장 종목 최신 일봉 upsert. ~2,600콜 → 실전 15/s 페이싱으로 약 3분, 모의 1.5/s로 약 29분 — 모의에선 수요 종목(§2.1)만. 전 종목의 원천은 `stock_master`(worker-batch 적재)이며, 비어 있으면(초기 구축 전) 수요 종목으로 대체하고 경고를 남긴다.
- 백필: 신규 종목/초기 구축 시 종목당 `(영업일수/100)`콜을 야간 슬롯에서 수행한다. 액면분할 등으로 마스터의 상장주식수 급변을 감지하면 해당 종목을 **전 구간 재적재**한다(수정주가 재계산 반영).

### 2.7 장 운영 캘린더

수신 대상 세션 전체(KST): NXT 프리마켓 08:00–08:50 · KRX/NXT 메인 09:00–15:30 · NXT 애프터마켓 15:30–20:00 · KRX 장후 시간외종가 15:40–16:00 · KRX 시간외단일가 16:00–18:00. **07:50 세션 준비(토큰·Approval·연결) → 08:00 구독 → 20:00 구독 해제·유휴.** 주말·KRX 휴장일은 스킵한다(MVP: 휴장일 YAML 수동 관리, P3: 캘린더 소스 자동화). KIS 새벽 점검 시간대에는 재접속을 억제한다.

### 2.8 단일 실행 보장 & 그레이스풀 셧다운

- worker-price는 세션 풀·구독 배정을 인메모리로 드는 상태有 워커이고, KIS WS는 계좌당 1세션이다(§1.3). 같은 계정으로 두 인스턴스가 동시에 돌면 안 되므로 **리더 락** `SET worker:price:leader {instanceId} NX PX 30000` + 10s 갱신으로 단일 실행을 보장한다. 락 미획득 인스턴스는 스탠바이로 대기한다.
- SIGTERM: 신규 구독 중지 → 전 세션 `tr_type=2` 해제 → WS close → 락 해제. 배포 중 틱 공백 수십 초는 허용한다 — NFR상 무손실 대상이 아니다.

---

## 3. worker-batch (스케줄 수집)

### 3.1 잡 카탈로그

| 잡 | 소스 · TR/엔드포인트 | 스케줄 (KST) | 대상·볼륨 | 멱등 키 |
|---|---|---|---|---|
| `stock_master_sync` | KIS 마스터 파일 `https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip`·`kosdaq_code.mst.zip`·`idxcode.mst.zip` (CP949, 고정폭 — KIS GitHub 파서 참조) | 매일 08:00 (원본 07:40경 갱신) | 전 종목 ~2,600 + 업종 ~490 | `code` upsert |
| `daily_candle_sync` | KIS `FHKST03010100` (§2.6 — 실행 주체는 price, 트리거·이력 관리는 batch 잡 테이블로 일원화 가능. MVP: price 내 스케줄) | 영업일 16:30 | 전 종목 | `(code,date)` |
| `valuation_daily` | KIS `FHKST01010100` 응답의 `per,pbr,eps,bps` + 마스터 시총 | 영업일 16:50 | 전 종목 (~2,600콜) | `(code,date)` |
| `investor_flow_daily` | KIS `GET .../inquire-investor` · TR `FHKST01010900` — **장마감 후 확정치** | 영업일 17:10 | 전 종목 | `(code,date)` |
| `invest_opinion_sync` | KIS `GET /uapi/domestic-stock/v1/quotations/invest-opbysec` · TR `FHKST663400C0` — 회원사 코드별 전 종목 투자의견(의견·직전의견·목표가) | 영업일 07:00 이상 18:00 미만 **10분 주기**(07:00~17:50) | 활성 회원사 `B`개 × 연속조회 페이지 `P` | `(code, business_date, broker_code, content_hash)` |
| `dart_corp_map` | OpenDART `corpCode.xml`(zip) — corp_code↔종목코드 매핑 | 주 1회 | 전 상장사 | `corp_code` |
| `financials_sync` | OpenDART `list.json`(신규 정기공시 감지) → `fnlttSinglAcntAll.json` (`bsns_year`, `reprt_code` 11013/11012/11014/11011, `fs_div=CFS`→미존재 시 `OFS`) | 매일 06:00 (공시 시즌 증분) | 신규 공시 기업만 | `(corp_code, year, reprt_code)` |

업종은 `idxcode.mst`(45바이트 고정폭 — 코드 5자리 + 이름)가 코드와 이름을 함께 준다. 종목 마스터의 업종 필드는 4자리라 그대로는 `sector.code`와 맞지 않는다. **앞에 시장 접두어(KOSPI `0`, KOSDAQ `1`)를 붙여 5자리로 맞춘다** — 예: KOSPI `0027` → `00027`(제조), KOSDAQ `1009` → `11009`(제조). 두 시장이 별개 코드 대역을 쓰므로 접두어 없이는 서로 충돌한다.

일일 KIS 호출 예산(실전 1계정): candle 2.6k + valuation 2.6k + investor 2.6k ≈ **7.8k콜**(15/s 페이싱 ~9분) + opinion `B × P × 66회`. `B`와 `P`는 실응답으로 계측해 확정한다(§9). opinion 잡 자체 상한은 **4콜/s**로 두고, 공용 Redis gate가 price REST 폴링과의 합산을 계정 내부 한도(실전 15/s) 아래로 묶는다. 앞 회차가 10분 안에 끝나지 않으면 다음 회차는 ShedLock 획득 실패로 건너뛰고 `opinion_sync_overrun`을 기록한다. 모의투자는 지원 여부·페이지 수를 확인하기 전까지 비활성이다. 지원이 확인돼도 공용 gate 한도 안에서만 실행한다. OpenDART는 일일 한도 내 여유가 있다(분기 시즌에도 수천 콜) — 정확한 한도는 포털에서 확인한다(§9).

### 3.2 실행 프레임워크

- Spring `@Scheduled` + **ShedLock**(Redis) — 다중 기동에 안전하다. 잡 이력은 테이블 `batch_job_run(job, run_date, status, ok_count, fail_count, started_at, finished_at, error)`에 기록하고, 동일 `(job, run_date)` SUCCESS가 있으면 스킵한다(재실행 멱등).
- **일내 반복 잡 예외**: `invest_opinion_sync`(10분 주기)는 `(job, run_date)` SUCCESS 스킵을 적용하지 않는다 — 실행 이력만 기록하고 매 회 실행한다. 멱등은 잡 내부의 upsert·미발행 스캔(§3.3)이 담당한다. ShedLock은 동일하게 적용해 실행 중인 회차가 있으면 다음 트리거를 시작하지 않는다. lock TTL은 최대 예상 실행시간보다 길게 두고, 장기 실행 시 만료되지 않도록 연장 가능한 lock provider를 사용한다.
- 실패 종목은 잡 말미에 1회 재시도한다. 잔여 실패는 `fail_count`+로그로 남기고 다음 날 upsert로 자연 회복한다.
- 재무 요약 변환: DART 계정과목 → `revenue/operatingProfit/netIncome/assets/liabilities/equity` 매핑 테이블(연결 우선). 매핑 불가 계정은 raw 보존 없이 스킵+카운트한다(포트폴리오 범위 단순화).

### 3.3 투자의견 → 실시간 소식 파이프라인 (`stream:{code}` 직접 발행) — ★ Redis 계약 v0.7·WS 계약 v0.6 반영

투자의견은 DB 적재로 끝나지 않고 **소식(stream) 경로로 클라이언트에 실시간 통보**한다. 큐·llm-worker를 거치지 않고 **worker-batch가 `stream_event` 저장 후 `stream:{code}`를 직접 PUBLISH**한다 — 메인서버의 post 경로와 같은 **persist-then-publish** 패턴(계약 §1 원칙). STOMP 목적지·봉투는 그대로 재사용하고, `stream.data`만 WS v0.6에서 하위 호환 확장한다.

```
worker-batch invest_opinion_sync (영업일 07:00~17:50 · 10분 주기)
  ① 활성 회원사별 FHKST663400C0 조회
       - FID_COND_MRKT_DIV_CODE=J, FID_COND_SCR_DIV_CODE=16634
       - FID_INPUT_ISCD={brokerCode}, FID_DIV_CLS_CODE=0
       - FID_INPUT_DATE_1={직전 영업일}, FID_INPUT_DATE_2={당일}
       - 응답 header tr_cont=M이면 tr_cont=N으로 다음 페이지 반복
  ② 응답 정규화 + content_hash 생성 후 invest_opinion INSERT
       - code는 stck_shrn_iscd, 의견·직전의견·목표가는 KIS 응답 필드 사용
       - broker_code는 요청의 FID_INPUT_ISCD, broker_name은 같은 코드의 로컬 회원사 마스터에서 결합
       - PK (code, business_date, broker_code, content_hash)
       - 동일 observation은 ON CONFLICT DO NOTHING, 기존 published_at을 NULL로 되돌리지 않음
  ③ 미통보 스캔 — published_at IS NULL 행 each:
       ⓐ DB 트랜잭션: source_key로 stream_event INSERT 시도
          - 충돌 시 기존 stream_event_id·payload 조회
          - 신규면 ULID를 1회 생성하고, 기존/신규 모두 invest_opinion.stream_event_id 연결
       ⓑ 트랜잭션 커밋 후 기존/신규 동일 eventId로 PUBLISH stream:{code}
       ⓒ Redis가 PUBLISH 명령을 정상 수락한 경우에만
          WHERE stream_event_id=:eventId AND published_at IS NULL 조건으로 해당 observation의 published_at 마킹
게이트웨이 / 클라
  ④ 기존 stream:{code} 구독·relay 그대로, eventId로 중복 제거
```

**재시도·전달 의미론**: `stream_event` 커밋이 진실의 원천이다. ⓑ 전에 크래시하면 다음 회차가 기존 `stream_event_id`를 읽어 같은 `eventId`로 재발행한다. ⓑ 후·ⓒ 전에 크래시하면 같은 메시지를 중복 발행할 수 있다 — 클라는 `eventId`로 흡수한다. Redis Pub/Sub은 ACK 없는 best-effort이므로 `published_at`은 **클라이언트 전달 완료가 아니라 PUBLISH 명령 수락 완료**를 뜻한다. 실시간 통보는 유실될 수 있다. 클라는 REST(`GET /rooms/{code}/stream`)에서 마지막 `eventId` 이후를 조회해 복구한다.

**stream_event payload** — `category="report"`를 재사용하고 `kind`·`opinion`을 WS v0.6의 optional 확장으로 추가:

```json
{
  "category": "report",
  "kind": "opinion",
  "title": "미래에셋증권 투자의견 매수",
  "occurredAt": 1785106800000,
  "opinion": {
    "brokerCode": "0000",
    "brokerName": "미래에셋증권",
    "ratingCode": "1",
    "rating": "매수",
    "previousRatingCode": "2",
    "previousRating": "중립",
    "targetPrice": 95000,
    "businessDate": "20260727"
  }
}
```

- `businessDate`는 KIS `stck_bsop_date`를 그대로 보존한다. KIS가 시각을 주지 않으므로 `occurredAt`은 해당 observation을 처음 수집한 `collected_at`으로 고정한다.
- `FHKST663400C0` 응답에는 회원사 코드·이름이 없다. 그래서 `brokerCode`는 조회 요청의 `FID_INPUT_ISCD`, `brokerName`은 회원사 마스터의 표시명을 쓴다. 알 수 없는 코드는 수집을 버리지 않고 `brokerName=null`, 제목은 `"{brokerCode} 투자의견 {rating}"`으로 발행한다.
- `content_hash` = `SHA-256(ratingCode + "|" + previousRatingCode + "|" + targetPrice)` 소문자 hex. 해시 전 문자열 필드는 trim하고 `null`은 빈 문자열, 목표가는 부호 없는 10진 문자열로 정규화한 UTF-8 바이트를 사용한다. 같은 날 같은 회원사의 의견·목표가가 바뀌면 별도 observation과 이벤트가 된다.
- `source_key` = `opinion:{code}:{businessDate}:{brokerCode}:{contentHash}`. 해시로 회원사 자체를 식별하지 않고 KIS 회원사 코드를 사용한다.
- `stream_event.type=REPORT`, `stream_event.source={brokerCode}`, payload는 위 JSON이다. 발행 봉투·채널명·카테고리·payload 타입은 전부 `:contracts` 상수/DTO를 사용한다.
- `:contracts`는 `StreamData.kind: String?`, `StreamData.opinion: OpinionData?`와 위 `OpinionData` 필드를 추가한다. 기존 payload 기준으로 두 최상위 필드는 nullable인 하위 호환 확장이고, `OpinionData` 내부의 필수·nullable 구분은 WS 명세 v0.6 §4.3을 따른다. JSON 계약 테스트로 기존 news/report/ai payload가 변하지 않음을 고정한다.
- `sentiment`는 싣지 않는다 — 의견·목표가 자체가 정보이고, "매수=POSITIVE" 같은 기계 매핑을 하지 않는다. 일일 다이제스트(뉴스 명세 §4.2) 취합 대상도 아니다(클러스터 기반이 아니므로 자연 제외).
- **소유 경계**: 이 경로로 worker-batch는 `stream_event` INSERT·`stream:{code}` PUBLISH 주체가 된다 — llm-worker와 **공동 생산자**(계약 §1.1 v0.7). `stream_event`의 논리 소유자는 core-api stream 모듈이고, DDL은 `db-migrations`가 단일 소유한다.
- 주기: **영업일 07:00 이상 18:00 미만 KST · 10분** — 장외 시간·주말·휴장일(§2.7 캘린더 공유)은 스킵한다. 호출 예산·overrun 정책은 §3.1 참조.

## 4. 워커 소유 데이터 스키마 (Liquibase 관리 — `db-migrations` 모듈)

```
stock_master(code CHAR(6) PK, name, market, sector_code NULL, shares_outstanding BIGINT,
             is_active BOOL, listed_at NULL, updated_at)
sector(code TEXT PK, name)   -- KIS 업종 마스터 idxcode.mst에서 stock_master_sync가 함께 적재(§3.1)
daily_candle(code, date CHAR(8), open, high, low, close INT, volume BIGINT, value BIGINT,
             PK(code, date))
valuation_daily(code, date, per NUMERIC, pbr NUMERIC, eps INT, bps INT, market_cap BIGINT,
             PK(code, date))
investor_flow_daily(code, date, individual BIGINT, foreign BIGINT, institution BIGINT,  -- 순매수 백만원
             PK(code, date))
invest_opinion(code CHAR(6), business_date CHAR(8), broker_code TEXT, broker_name TEXT,
             rating_code TEXT, rating TEXT, previous_rating_code TEXT NULL, previous_rating TEXT NULL,
             target_price BIGINT NULL, content_hash CHAR(64), collected_at TIMESTAMPTZ,
             stream_event_id CHAR(26) NULL, published_at TIMESTAMPTZ NULL,
             PK(code, business_date, broker_code, content_hash))
stream_event(..., source_key TEXT NULL, ...)
-- UNIQUE(source_key) WHERE source_key IS NOT NULL
-- source_key DDL의 논리 소유자는 core-api stream, changeSet 파일의 단일 소유자는 db-migrations
dart_corp_map(corp_code CHAR(8) PK, code CHAR(6) UQ NULL, corp_name)
financial_summary(code, year SMALLINT, reprt_code CHAR(5), fs_div CHAR(3),
             revenue BIGINT, operating_profit BIGINT, net_income BIGINT,
             assets BIGINT, liabilities BIGINT, equity BIGINT, disclosed_at,
             PK(code, year, reprt_code))
batch_job_run(id, job, run_date, status, ok_count, fail_count, started_at, finished_at, error)
```

읽기 소비자는 core-api stockinfo/search 모듈(REST 명세 §8)과 worker-llm 섹터 해소(`sector`·`stock_master.sector_code` — [뉴스 파이프라인 명세](alphatalk_news_worker_spec.md) §3.6)다. 금액 컬럼은 원 단위로 저장하고, API 단위 변환은 core-api 책임이다(명세와 합의).

## 5. 설정·환경변수

| 변수 | 예 | 설명 |
|---|---|---|
| `KIS_ENV` | `vts` | vts(모의)/prod(실전) |
| `KIS_ACCOUNTS` | `[{"keyId":"a1b2c3d4","appkey":"...","appsecret":"..."}]` | 세션풀 계정 목록(시크릿 매니저 주입) |
| `KIS_RATE_FACTOR` | `0.75` | 공식 유량 대비 내부 한도 비율 |
| `DART_API_KEY` | — | OpenDART |
| `DEMAND_RECONCILE_SEC` / `CONFLATION_MS` | `60` / `200` | §2 파라미터 |
| `MARKET_HOLIDAYS_FILE` | `holidays-2026.yml` | 휴장일 |
| `REDIS_URL` / `DB_URL` | — | 공용 |

## 6. 관측성

메트릭: `kis_ws_sessions{state}` · `kis_subscribed_symbols` · `demand_symbols` · `degraded_symbols` · `tick_in_rate`/`quote_publish_rate` · `conflation_lag_ms` · `pingpong_miss` · `rest_call_rate{keyId}` · `rest_throttled` · `token_refresh_total` · `batch_job_duration/fail{job}`. 로그는 구조화 JSON으로 남기고 appkey/token은 마스킹한다. 프레임 원문은 DEBUG+샘플링으로만 남긴다. 알람: WS 세션 전멸 5분, 장중 tick_in=0, 배치 실패, throttled 급증.

## 7. 장애 시나리오 & 대응

| 시나리오 | 대응 |
|---|---|
| WS 끊김/half-open | PINGPONG 타임아웃 감지 → 백오프 재접속 → 배정분 재구독 리플레이 |
| 토큰 만료/무효 | 캐시 무효화 → 분산락 재발급(1분 제한 준수) → 1회 재시도. 실패 지속 시 세션 DEGRADED |
| 유량 초과 응답 | 백오프 + `rest_throttled`↑, 폴링 주기 자동 확대 |
| Redis 순단 | 틱 발행 드랍 허용, 리컨실은 다음 주기 재시도. 배치는 ShedLock 획득 실패 시 스킵 후 다음 슬롯 |
| 게이트웨이 크래시 | `gw:alive` TTL 만료 → 해당 수요 제외 → 과잉 구독 자동 해소(30s 유예) |
| KIS 점검·휴장 | 캘린더로 억제, 점검 중 재접속 백오프 상한 확대 |

## 8. 테스트 전략 (워커 특화)

- 파서 단위: 실 캡처 프레임/mst 파일 픽스처(CP949 포함) 고정 — 체결가 TR 3종(H0STCNT0·H0UNCNT0·H0STOUP0) 필드 순서 검증.
- 세션풀 단위: 가짜 KIS 서버(로컬 WS)로 등록 한도(41건, TR×종목) 초과 배정·다중 TR 구독/해지·재접속 리플레이·해지 유예 검증.
- 통합(Testcontainers): demand 해시 변경 → 구독 집합 수렴 / conflation 발행 주기 / 배치 2회 실행 멱등.
- 모의투자 스모크: 장중 삼성전자 1종목 실수신 → `quote:005930` 발행 확인.

## 9. 오픈 이슈 (구현 착수 전 확인)

1. **KIS 2026-03-20 "신규 고객 초당 호출 제한" 공지 원문** — §1.3 수치 재확인 (포털 공지사항).
2. 모의투자 WS에서 `H0STCNT0` 실시간 지원 범위·시간대 제약, 그리고 **통합 `H0UNCNT0`·시간외 `H0STOUP0`의 vts 지원 여부** 확인 — 지원이 확인되면 vts도 실전과 동일 TR 세트로 통일한다(§2.3).
3. 체결가 TR 3종의 전체 필드 순서 — 공식 예제(open-trading-api) columns 전 필드 대조 완료(46·46·43, idx 0~13 완전 일치). 실프레임 캡처로 재확정만 남음.
4. 마스터 파일 URL 안정성(비공식 경로) — 포털 "종목 다운로드" 링크 주소와 대조, 변경 대비 설정화.
5. `FID_ORG_ADJ_PRC` 값 의미(0=수정주가) 문서 재확인.
6. OpenDART 일일 호출 한도 수치.
7. `FHKST663400C0`의 **모의투자(vts) 지원 여부**, 활성 회원사 코드 원천·갱신 주기, 응답 1페이지 건수와 `tr_cont` 최대 페이지를 실계정 스모크로 확정. 결과로 §3.1의 `B × P` 호출량과 10분 주기 지속 가능성을 검증한다.
8. **업종 분류의 세분도** — `idxcode.mst`가 주는 대분류는 KOSPI 11종·KOSDAQ 20여 종이라 "제조"에 대부분이 몰린다. worker-llm의 섹터 fan-out이 이 정도 해상도로 쓸 만한지 실데이터로 확인하고, 부족하면 중·소분류(마스터 파일의 [68:72]·[72:76]) 사용이나 서비스 자체 분류를 검토한다.
---

*KIS 수집 워커 명세 v0.2 — Redis 계약 v0.12·WS API v0.6·core-api 명세 v0.1과 정합. KIS 수치는 2026-07 공식 샘플 대조 기준이며 §9 항목은 실계정 재확인 대상.*
