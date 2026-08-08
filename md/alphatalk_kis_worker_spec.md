# Alpha Talk — KIS 수집 워커 명세 v0.7
**worker-price · worker-batch · `:kis-client` 공유 라이브러리 · 담당: 민균**

> **v0.7 (2026-08-07)**: 같은 함정이 **실시간에도 있었다** — `H0UNCNT0`(통합)이 NXT 미상장 종목에 등록 SUCCESS를 주고 틱을 0건 준다(047040 45초 0건 vs `H0STCNT0` 48건, 실계정 계측). 대우건설이 구독·거래 중인데 `price:047040` 캐시가 한 번도 안 생긴 사고의 원인이다. ⑴ 체결 TR을 **종목별로** 고르고(§2.3), ⑵ **NXT 상장 여부를 `market-div:{code}` 한 키가 소유**해 분봉·실시간·현재가 폴백이 함께 읽고 확정 판정만 쓰며(Redis 계약 v0.17), ⑶ 등록됐는데 조용한 심볼을 degraded로 강등해 REST 폴링이 받게 한다 — **조용히 비는 상태를 만들지 않는다**. 현재가 폴백도 같은 구분을 써 폴백 구간의 누적거래량 37% 어긋남을 없앤다.
> **v0.6 (2026-08-07)**: 투자의견 수집(§3.3)을 실계정 계측으로 확정 — §9-7 종결. ⑴ 회원사 코드 원천은 KIS 마스터 파일 `memcode.mst.zip`(5자리 코드+이름+외국계 플래그, 집계 행 `99999` 제외 61개사)이고 **요청 `FID_INPUT_ISCD`는 5자리 코드의 뒤 3자리**다 — 5자리를 그대로 보내면 에러가 아니라 `rt_cd=0`에 0행이 온다(조용한 실패). ⑵ 이 TR은 **연속조회를 지원하지 않는다** — 응답은 최신순 최대 100행에서 잘리고 `tr_cont`가 오지 않는다(`P=1`, v0.2의 tr_cont 반복 절차 폐기). ⑶ 응답에 **회원사명 `mbcr_name`이 있다**(v0.2의 "이름 없음" 가정 정정 — 코드만 없다). ⑷ `invt_opnn_cls_code`는 등급 분류가 아니라 위치 값(현재=2·직전=3 고정)이라 **content_hash·payload에서 제외**하고 의견 텍스트를 쓴다(WS 계약 v0.7). ⑸ `hts_goal_prc=0`은 목표가 없음 → `null`.
> **v0.5 (2026-08-06)**: 분봉 시장 구분을 **전 종목 고정 `UN`에서 종목별 결정으로 정정**(§2.6). 실계정 계측 결과 `UN`은 NXT 미상장 종목에 대해 에러도 빈 응답도 아닌 **가격·거래량이 전부 0인 30행을 `rt_cd=0`으로 반환한다** — 활성 종목 60개 표본에서 45개(75%)가 여기 해당했다. 그 0행이 그대로 적재되고 완주 워터마크까지 찍혀 자가 복구가 막히는 사고가 실제로 났다(대우건설 047040, 2026-08-06, 721행 전부 0). 이에 따라 ⑴ **0봉은 적재도 완주 기록도 하지 않는 불변식**을 세우고, ⑵ 종목별 지원 여부를 프로브해 `minute:market-div:{code}:{date}`(Redis 계약 v0.15)에 **날짜별로** 기록한 뒤 그 구분으로 조회하며, ⑶ **수집 창이 종목별로 달라진다**(NXT 지원 08:00–20:00 · 미지원 09:00–15:30). §9.10 종결, §9.12 신설.
> **v0.4 (2026-08-05)**: KIS 모의투자(vts) 환경 제거 — **실전 도메인 단일 운영**으로 확정(§1.1). 개발 계획에 없는 환경을 위해 실시간 TR 세트(§2.3)·일봉 유니버스(§2.6)·분봉 시장 구분(§2.6)·REST 유량(§1.3)을 이중으로 유지하던 분기를 전부 걷어냈다. `KIS_ENV` 환경변수와 `alphatalk.price.env` 프로퍼티는 사라졌고, 엔드포인트는 `:kis-client`의 `KisApi` 상수가 단일 소유한다. 환경 차이는 KIS가 아니라 워커 활성화 여부(`alphatalk.price.enabled`, 기본 off)로 만든다. §9.2 종결.
> **v0.3 (2026-08-04)**: 분봉 수집 추가(FR-15 확장, §2.6). **오늘 분봉은 조회 시 동기 신선화** — core-api가 worker-price 내부 API를 트리거하고 worker-price가 KIS 당일분봉 `FHKST03010200`을 공백만큼 사 와 upsert(60s 신선 임계·single-flight·타임아웃 시 저장분 반환). **과거 분봉은 일 배치**(16:00 당일 확정) + 콜드 종목 7영업일 수요 전이 백필(`FHKST03010230`). 1분봉 원본만 `minute_candle`에 보존 30일, 5/15/30/60분은 core-api가 조회 시 파생(core-api 명세 v0.2 §8). 내부 신선화 API는 서버 간 Redis/DB 원칙의 명시 예외(멱등 트리거·무데이터·best-effort).
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

**실전 도메인 단일 운영이다.** KIS 모의투자(vts) 도메인은 지원하지 않는다 — 모의투자는 통합(`H0UNCNT0`)·시간외(`H0STOUP0`) 실시간 TR과 분봉 통합 시장 구분(`UN`), 과거 분봉(`FHKST03010230`) 지원이 확인되지 않아 실전과 다른 수집 경로를 하나 더 유지해야 했고, 그 경로로 쌓인 데이터는 NXT 체결분이 빠진 채 저장돼 실전 데이터와 구분되지 않는다. 환경 분기를 없애고 실전만 남긴다. 엔드포인트 상수는 `:kis-client`의 `KisApi`가 단일 소유한다.

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

KIS 유량은 슬라이딩 윈도로 측정되는 것으로 알려져 있다. 공식 한도에 딱 맞춰 호출하면 윈도 경계에 호출이 몰릴 때 초과가 난다. 그래서 내부 한도는 **공식의 75%**(15/s)로 잡는다.

| 구분 | 한도 | 비고 |
|---|---|---|
| REST | **20건/초 (계좌 단위)** | 내부 한도는 공식의 75% — 15/s (근거는 위) |
| WS 등록 | **1세션 합산 41건, 계좌(앱키)당 1세션** | 국내/해외·체결가/호가 등 전 실시간 합산 41 |
| ⚠️ 정책 변동 | 포털 공지 "신규 고객 초당 호출 제한 안내(2026-03-20)" | **구현 착수 전 원문 확인** 후 본 표 갱신 (§9 오픈 이슈) |

구현: Resilience4j `RateLimiter`를 **계정(keyId) 단위**로 생성하고 모든 REST 호출이 이를 통과한다. 429/유량 오류를 받으면 지수 백오프 후 재시도하고 메트릭 `rest_throttled`를 올린다.

프로세스별 limiter만으로는 부족하다. `worker-price`와 `worker-batch`가 같은 계정을 공유하면 각자 한도를 지켜도 합산 유량이 한도를 넘는다. 그래서 `:kis-client`에 `KisRateGate` 포트를 두고, 서버는 Redis 토큰 버킷 `rate:kis-rest:{keyId}` 구현을 주입한다. 모든 일반 REST 시세·배치 호출은 **로컬 smoothing limiter → 공용 Redis gate** 순서로 통과한다. 토큰 버킷은 Lua로 `{tokens, updatedAt}` 계산·차감을 원자화한다. 파라미터는 `capacity=15, refill=15/s`, 마지막 소비 후 TTL 2분. permit이 없으면 다음 충전 시각까지 기다리고, 호출별 타임아웃을 넘으면 실패 처리한다. 토큰 발급의 1분 가드와 Approval 발급은 §1.2의 별도 제한을 따른다. **구현 상태**: 포트는 `:kis-client`(`KisRateGate`), Redis 토큰 버킷 구현은 worker-price(`RedisKisRateGate`)가 주입해 REST 전 호출이 통과한다. worker-batch는 KIS REST 잡 착수 시 같은 구현을 주입한다. 버킷의 시각은 **Lua 안에서 Redis `TIME`으로 읽는다** — 호출자 프로세스 시각을 쓰면 인스턴스 간 시계 오차가 `updatedAt`을 과거로 되돌려 매 호출이 큰 경과시간만큼 재충전되므로 합산 한도가 깨진다. Redis 호스트 시각이 뒤로 점프하는 경우를 대비해 경과시간은 음수를 0으로 절삭한다.

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

- WS 등록 한도는 세션당 41건이고 **등록 단위는 (tr_id, 종목)**이다. 심볼당 2건(통합 체결가+시간외 체결가, §2.3)을 등록하므로 심볼 용량은 `C = ⌊41 / 2⌋ × 계정 수 = 20 × 계정 수`다. 계정 목록은 `KIS_ACCOUNTS`(JSON 배열: keyId·appkey·appsecret)로 주입한다.
- 시간외 TR을 상시 등록하는 이유: 장중/시간외 경계에서 TR을 갈아끼우면 하루 두 번 대량 재구독·재배정 플래핑이 생기고, 경계 시각에 용량이 출렁여 강등이 요동친다. 용량 절반을 내주고 등록을 고정하는 쪽을 택한다 — 초과 수요는 어차피 REST 폴링 강등(§2.5)이 받는다. 시간대별 TR 스왑은 계정 추가로도 부족해질 때의 후속 최적화로 남긴다.
- 목표 집합은 리컨실마다 산출한다. `rooms ∪ topN(quote)`이 C를 넘으면 **우선순위: ① 입장 방 ② quote refCount 내림차순**으로 자른다. 탈락분은 REST 폴링으로 강등한다(§2.5).
- 배정: `종목 → (세션, 슬롯)` 맵 + 역인덱스. 신규는 빈 슬롯 최다 세션에 배정하고 해지는 슬롯을 반납한다. 잦은 재배정(플래핑)을 막으려고 **해지는 30초 유예**한다 — 그 사이 재수요가 오면 취소.
- 세션 상태머신: `DISCONNECTED → CONNECTING(Approval 발급) → CONNECTED(구독 리플레이) → DEGRADED(오류 누적)`. 재접속은 지수 백오프+지터. 성공하면 그 세션 배정분 전체를 재구독한다.

### 2.3 KIS WS 프로토콜

**구독 TR 세트** — 심볼당 2건(체결 1 + 시간외 1)을 등록해 KRX·NXT 전 세션을 커버한다:

| tr_id | 내용 | 수신 시간대(KST) |
|---|---|---|
| `H0UNCNT0` / `H0STCNT0` | 실시간 체결가 — NXT 상장은 **통합**, 미상장은 **KRX 전용**(아래) | NXT 프리 08:00–08:50 · KRX/NXT 메인 09:00–15:30 · NXT 애프터 15:30–20:00 |
| `H0STOUP0` | **시간외** 실시간 체결가 (KRX) | 장후 시간외종가 15:40–16:00 · 시간외단일가 16:00–18:00 |

**체결 TR은 종목별로 고른다** — NXT 상장 종목은 `H0UNCNT0`(통합), 미상장 종목은 `H0STCNT0`(KRX 전용)이다. 심볼당 등록은 여전히 2건(체결 1 + 시간외 1)이라 세션 용량(41건)은 그대로다.

- **왜 통합 하나로 통일할 수 없나**: `H0UNCNT0`은 NXT 미상장 종목에 **등록은 `SUBSCRIBE SUCCESS`로 받아주고 틱을 하나도 주지 않는다.** 실계정 계측(2026-08-07 13:40, 45초 · 한 연결에서 동시 등록): 047040 대우건설 통합 **0건** / KRX 전용 **48건**, 같은 시각 005930 삼성전자는 통합 166건 · KRX 134건으로 둘 다 정상. 등록 ACK가 성공이라 **ACK만 보고는 절대 감지할 수 없다.** 분봉의 0봉 문제(§2.6)와 같은 뿌리이며, 시계열 API가 NXT 미상장 종목에서 깨지는 현상이다(현재가 `FHKST01010100`은 `UN`으로도 정상).
- **왜 KRX 전용 하나로 통일할 수도 없나**: 통합이 NXT 체결분을 실제로 더 준다. 같은 계측에서 005930 누적거래량이 통합 **23,968,985** vs KRX **15,036,769** — 약 **37%** 차이다.
- **구분의 출처**: `market-div:{code}`(Redis 계약 v0.17) 하나가 소유한다. 분봉(§2.6)과 이 절이 같은 키를 읽고 쓴다.
- **침묵은 판정 근거가 아니라 운영 신호다**: 기록이 없는 종목은 통합으로 등록하고, `silence-ms`(기본 20s) 동안 체결 틱이 하나도 없으면 **degraded로 강등해 REST 폴링(30s, §2.5)이 받는다.** 채널을 KRX로 바꾸지 않는다 — 침묵은 "통합이 이 종목을 안 준다"와 "그 시간에 체결이 없었다"를 구분하지 못하기 때문이다. 특히 프리마켓(08:00–08:50)처럼 얇은 구간에서는 정상 NXT 종목도 20초쯤 조용한 것이 흔하다. 여기서 KRX로 바꾸고 그 뒤 도착한 KRX 틱을 `J`의 근거로 삼으면 **거의 모든 종목이 `J`로 잘못 확정되어** NXT 체결분(누적의 37%)과 장외 세션을 통째로 잃는다. **조용히 틀린 데이터보다 30초 늦은 정확한 데이터가 낫다.**
- **`J`는 실시간이 쓰지 않는다**: 확정 근거가 되는 "거래량은 있는데 통합이 안 준다"를 관측할 수 있는 쪽은 분봉의 REST 응답(`acml_vol > 0` + 봉 전부 0)뿐이다(§2.6). 실시간은 통합 틱이 실제로 도착했을 때 `UN`만 확정한다 — 이건 통합이 그 종목을 다룬다는 직접 증거다. 채널 선택은 분봉이 채운 `market-div:{code}`를 읽어서 한다.
- **판정에 쓰는 틱은 체결 TR의 것만이다**: `H0STOUP0`(시간외)는 시장 구분과 무관하게 항상 등록되는 KRX 전용 채널이므로, 그 틱을 통합 체결의 증거로 삼으면 NXT 미상장 종목이 `UN`으로 잘못 확정되어 7일간 고정된다(15:40–18:00 구간에 기동·재접속하면 바로 걸린다). 같은 이유로 시간외 틱은 **침묵 판정도 리셋하지 않는다**. 틱 프레임의 `tr_id`를 소비 지점까지 전달해 체결 TR만 쓴다.
- **분봉이 뒤늦게 판정하면 활성 구독도 갈아탄다**: 채널 선택은 구독 시점에 한 번 읽고 끝나지 않는다. **틱이 없는 종목에 한해** 매 주기 `market-div:{code}`를 다시 읽어, 값이 바뀌었으면 재구독한다(`tick.div.resubscribed`). 이게 없으면 분봉이 `J`를 기록해도 그 세션 내내 통합에 붙어 있어 실시간이 복구되지 않는다 — 재기동이나 수요 이탈까지 REST 30초에 머문다. 재구독 시점에는 아직 새 채널이 검증되지 않았으므로 **강등 상태를 유지**해 REST 폴링이 계속 받고, 새 채널의 틱이 도착하면 그때 해제한다(공백 없음). 틱이 흐르는 종목은 다시 읽지 않는다 — 잘 받고 있는 채널을 바꿀 이유가 없고, 매 주기 전 종목 조회를 피한다. 그래서 NXT **편입**(J→UN)은 그 종목이 KRX 틱을 받는 동안에는 반영되지 않고 다음 거래일 몫이다(§9.13 ⑴).
- **판정 상태는 정비 스레드가 단독으로 만진다**: 채널 선택은 maintain 스레드가, 틱 수신은 WS 프레임 스레드가 한다. 두 스레드가 같은 상태를 만지면 "현재 채널의 틱인가"를 확인한 뒤 반영하기까지의 틈에 전환이 끼어들어 **이전 채널의 틱이 전환 뒤에 반영된다** — 침묵 감시가 영구히 꺼지고 강등도 풀려, 새 채널이 조용해도 WS도 REST도 받지 못하는 사각지대가 된다. 락으로 그 틈을 막는 대신 **경쟁 자체를 없앤다**: WS 스레드는 관측(`code`·`div`·시각)만 남기고, 채널 비교·침묵 해제·구분 확정은 전부 다음 정비 주기에 maintain이 처리한다. 틱 반영이 최대 1주기(기본 1s) 늦어지지만 침묵 임계 20s·REST 30s에 견주면 무시할 수 있고, 그 대가로 이 경로에 스레드 경계가 사라져 회귀 테스트도 단일 스레드로 결정적으로 쓸 수 있다.
- **강등은 틱이 다시 흐를 때만 풀린다**: 재접속·재구독은 침묵 타이머를 다시 무장하지만 강등 상태는 지우지 않는다. 재접속이 강등을 지우면 그 순간 REST 폴백이 끊기고, 새 침묵 임계(20s)가 다시 찰 때까지 WS도 REST도 없는 공백이 생긴다 — WS 절단이 잦은 환경일수록 강등 종목이 오히려 더 자주 비게 되는 역전이다. 해제 조건은 현재 채널의 틱 도착 하나뿐이다.
- **현재 선택된 채널이 준 틱만 센다**: 채널이 바뀐 직후 이전 채널의 잔여 틱이 늦게 도착할 수 있다. 이걸 세면 **침묵 상태가 오염된다** — 강등이 풀리고 침묵 감시가 영구히 꺼져, 지금 채널이 조용해도 다시는 감지되지 않는다. 확정뿐 아니라 침묵 리셋도 현재 채널의 틱에만 반응한다.
- REST 폴링과 장전 워밍은 같은 `market-div:{code}`를 읽어 현재가를 조회한다 — 폴백 구간에서 누적거래량이 위 37%만큼 어긋나지 않게 한다.

`:kis-client`의 프레임 파서는 세 TR을 모두 인식한다.

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
- 폴링 대상은 강등 심볼. 예산은 계정 유량의 50% → 1계정 기준 7.5콜/s ≈ 심볼 225개를 30s 주기로 커버한다. 강등조차 초과하면 refCount 하위는 60s 주기로 늦춘다.
- 구독 시작 직전(07:55) 수요 심볼을 1회 워밍 폴링해 입장 스냅샷 공백을 막는다.

### 2.6 봉(OHLCV) 수집·백필

- **기간별 시세**: `GET /uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice` · TR `FHKST03010100` · `FID_INPUT_DATE_1/2`(범위), `FID_PERIOD_DIV_CODE=D`, `FID_ORG_ADJ_PRC=0`(수정주가) → **1콜 최대 100봉**.
- 정기: 매 영업일 16:30 전 상장 종목 최신 일봉 upsert. ~2,600콜 → 15/s 페이싱으로 약 3분.
- **유니버스 원천**: `stock_master`의 활성 종목(worker-batch 적재)이다. 수요(§2.1)는 접속자에 따라 휘발하므로 일봉 대상이 될 수 없다 — 둘은 분리한다.
    - `stock_master`가 **비어 있으면**(초기 구축 전) 수요 종목으로 대체하고 경고를 남긴다 — 정상 축소 경로다.
    - `stock_master` **조회가 실패하면**(DB 장애) 축소하지 않는다. exponential backoff+jitter로 3회 재시도 후에도 실패하면 회차를 중단하고 `candle_sync_aborted`를 올린다 — 축소된 채 "성공"으로 끝나 다음 영업일까지 종목이 누락되는 것을 막는다.
- **부분 실패 = 미완주**: 개별 종목 실패는 나머지 종목의 처리를 막지 않지만(계속 진행) 실패 수를 `candle_sync_failed`로 기록하고 **회차는 미완료로 남긴다**. 한 종목이라도 실패하면 완주로 치지 않는다.
- **미완주 재시도**: 완주하지 못한 영업일에는 17·18·19시에 리더가 재시도한다(완주한 날은 no-op). 이미 적재된 종목은 `latestDate()` 비교로 건너뛰므로 재조회 대상은 실패분뿐이고, 장 마감 후라 재조회는 같은 확정 일봉을 upsert하므로 멱등이다.
- 백필: 신규 종목/초기 구축 시 종목당 `(영업일수/100)`콜을 야간 슬롯에서 수행한다. 액면분할 등으로 마스터의 상장주식수 급변을 감지하면 해당 종목을 **전 구간 재적재**한다(수정주가 재계산 반영).

**분봉 (FR-15 확장) — 오늘은 조회 시 동기 신선화, 과거는 일 배치**

- **수집 창은 종목의 NXT 상장 여부에 달렸다.** NXT 지원 종목은 **08:00–20:00** — §2.7의 수신 세션 전체(NXT 프리 08:00–08:50 · KRX/NXT 메인 09:00–15:30 · NXT 애프터 15:30–20:00 · KRX 시간외 15:40–18:00)를 **연속으로** 담고 하루 최대 봉 수는 **721**이다(08:00~20:00 양끝 포함). 실시간 틱은 이 창 전체를 푸시하는데 봉이 정규장만 있으면 장외 시간에 차트가 비고 시세 핀만 움직이는 불일치가 생기기 때문이다. NXT 미지원 종목은 **09:00–15:30**(정규장, 최대 **391**봉)이다 — KIS가 이 종목들에 대해 장외 세션 분봉을 주지 않는다(아래 계측). 세션 사이 공백(08:50–09:00 등)은 봉이 없는 구간으로 자연스럽게 비고, 전방 페이징이 빈 창을 건너뛰며 전진한다(희소 데이터와 같은 처리).

- **소스 API**: 당일은 `GET /uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice` · TR `FHKST03010200` · `FID_INPUT_HOUR_1`(기준 시각) → **1콜 최근 30개 1분봉**. 과거 일자는 주식일별분봉조회 `GET /uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice` · TR `FHKST03010230`. 원천은 KIS 공식 분봉이다 — 자체 틱 집계로 분봉을 만들지 않는다. §0의 "틱 비영속" 원칙은 유지된다: 틱을 대체하는 영속형이 봉이고, 그 봉의 값은 KIS가 준다.

- **시장 구분 `FID_COND_MRKT_DIV_CODE`는 종목별로 정한다** — NXT 지원 종목은 `UN`(통합), 미지원 종목은 `J`(KRX). 전 종목 고정 `UN`은 v0.4까지의 결정이었으나 **실계정 계측으로 폐기**했다(2026-08-06):
    - `UN`은 NXT 미지원 종목에 대해 `rt_cd=0` 정상 응답에 **`output2` 30행의 가격·거래량·누적거래대금을 전부 `0`으로 채워 반환한다**. 에러도 빈 배열도 아니다. `output1`은 종목명·현재가·누적거래량이 정상이라 응답만 보고는 성공과 구분되지 않는다(047040 대우건설: `output1.acml_vol=11,460,116`인데 `output2` 전 행 0, 같은 종목 `J`는 정상).
    - 활성 종목 60개 무작위 표본에서 **45개(75%)가 0행**이었다. 지원 종목 비율(25%)은 전체 상장 종목 대비 NXT 상장 규모(~800/~2,800)와 일치한다. 대형주(대우건설·대한항공)도 포함되므로 시가총액·시장(KOSPI/KOSDAQ)으로 유추할 수 없다.
    - **KIS는 NXT 상장 명단을 주지 않는다** — `kospi_code.mst`(288바이트 고정폭)의 전 컬럼을 0행 종목 5개 vs 정상 종목 5개로 바이트 비교했을 때 두 집단을 가르는 컬럼이 없고, `nxt_code.mst`·`unified_code.mst` 등 별도 마스터는 404, 분봉 응답 `output1`에도 시장 관련 필드가 없다. 따라서 명단은 **우리가 프로브해서 만든다**(아래).
- **NXT 지원 판별과 캐시**: 판정식은 **`output1.acml_vol > 0` 이면서 `output2` 전 행의 가격이 `0`** → 미지원이다. 거래가 없는 종목과 헷갈리지 않는다 — 누적 거래량이 살아 있는데 봉만 0인 조합은 미지원에서만 나온다(표본 60종목에서 오탐 0). 결과는 Redis `minute:market-div:{code}:{date}`(값 `UN`|`J`, TTL 2일 — Redis 계약 v0.15)에 **날짜별로** 기록한다. 종목별 장기 캐시가 아니라 **그날 적재분이 어느 구분으로 만들어졌는지의 기록**이다 — 이 키가 있으면 그 구분으로 1콜만 나가고, 프로브의 추가 콜은 그날 첫 조회에서만 발생한다(`UN` → 0행 감지 → `J` 재조회 2콜).
    - **왜 종목별이 아니라 날짜별인가**: 종목별 장기 캐시는 만료 시점이 장중에 걸리면 "적재분은 있는데 그 구분을 모르는" 상태를 만든다. 그 상태에서 정상 `UN` 응답을 이어 붙이면 앞 구간이 `J`였을 때 누적 거래대금 앵커가 어긋나 첫 봉의 거래대금이 폭증한다(NXT 편입 종목에서 실제로 가능한 경로다). 키를 `(code, date)`로 좁히면 **한 날짜의 행은 그 날짜 기록이 가리키는 한 구분에서만 나온다**는 불변식이 구조적으로 성립하고, 별도 시장 차원을 `minute_candle`에 두지 않아도 된다.
    - **추가 비용은 사실상 없다**: 프로브는 그날 첫 데이터 콜을 겸한다. `UN` 종목은 그 콜이 그대로 적재에 쓰이므로 추가 콜 0이고, `J` 종목만 하루 1콜을 버린다. 대신 NXT 편입·제외가 **다음 거래일 첫 조회에 자동 반영**되어 별도 무효화 경로가 필요 없다.
    - **기록이 없는데 그날 적재분이 있으면 그날은 갱신하지 않는다**(`minute.candle.div.unknown`). Redis 유실 같은 예외 상황에서만 발생하며, 알 수 없는 구분에 이어 붙여 앵커를 깨뜨리는 것보다 그날을 멈추는 편이 안전하다 — 저장분은 그대로 남고 다음 거래일에 정상 복귀한다.

    판정에는 두 개의 전제 조건이 붙는다:
    - **`acml_vol = 0`이면 판정을 보류한다** — 캐시에 쓰지 않고 그 회차는 `UN`으로 진행한다. 첫 체결 전(프리마켓 개장 직후)이나 거래정지 종목은 지원 종목이라도 봉이 0으로 오므로, 이때 판정하면 지원 종목이 `J`로 굳어 그날 장외 구간을 통째로 잃는다. 0봉 가드가 적재를 막으므로 보류에 따르는 손실은 다음 조회까지의 지연뿐이다.
    - **그날 적재분이 하나라도 있으면 판정하지 않는다** — 기록 유무와 무관하다. 구분을 그날 도중에 바꾸면 이미 쌓인 행과 새 행의 누적 거래대금 기준이 달라져 앵커가 깨지기 때문이다(아래 "하루 안에서 시장 구분을 섞지 않는다"). 따라서 판정·전환은 **그날 첫 적재 전에만** 일어난다.
- **기록이 `UN`인데 0봉이면 같은 회차에서 `J`로 전환한다** — 위 두 전제를 만족할 때(적재분 0 · `acml_vol > 0`)에 한한다. 그날 안에서 NXT 제외가 반영되는 자가 복구 경로다. **"기록이 있으면 그 구분으로 1콜, 단 판정 조건이 성립하면 전환 1콜 추가"**가 정확한 계약이다.
- **전환 시 쓰기 순서는 워터마크 삭제 → 구분 기록이다.** 반대 순서면 그 사이에 프로세스가 죽었을 때 `J` 기록과 `UN` 기준으로 남은 워터마크가 함께 살아남아, 다음 실행이 그 워터마크를 `J` 조회의 기점이나 완주 표식으로 써서 실제 KRX 봉을 건너뛴다. 이 순서에서는 크래시가 "둘 다 없음"으로 수렴해 다음 실행이 그날 첫 조회부터 다시 판정한다(멱등).
- **시작 봉도 시장 구분이 정한다**: 전방 페이징의 기점(저장분이 없을 때)은 `UN` 08:00 · `J` 09:00이다. `J` 종목을 08:00에서 시작하면 봉이 없는 08:00~08:59를 매 조회마다 다시 훑어 콜만 태운다.
- **하루 안에서 시장 구분을 섞지 않는다**: `minute_candle`에는 시장 차원이 없어 저장된 행만으로는 `J`로 받은 것인지 `UN`으로 받은 것인지 구분할 수 없고, 두 구분의 누적 거래대금은 실제로 다르다(005930 2026-08-06: `UN` 9,656,077,035,000 vs `J` 6,091,288,502,000 — NXT 체결분이 누적의 37%). 섞이면 누적→분당 변환의 앵커가 깨져 분당 값이 음수·폭증으로 튄다. 그래서 전환은 **그날 적재분이 0일 때만** 허용하고(위 판정 전제), **이미 적재된 분봉은 어떤 경우에도 삭제하지 않는다** — 신선화는 stale-while-revalidate 계약(조회 실패·타임아웃 시 저장분 반환)을 지켜야 하므로, KIS 응답을 받기 전에 기존 데이터를 지우는 경로를 두지 않는다. 전환 시에는 그날 워터마크만 폐기한다(`UN` 20:00 기준으로 남은 표식이 `J`의 15:30 완주 판정을 오염시키기 때문). 앵커 역행은 `minute.candle.value.regressed` 카운터로 드러난다.
- **0봉은 적재도 완주 기록도 하지 않는다(불변식)**: `open=high=low=close=0`인 행은 유효 시세가 아니므로 upsert 대상에서 제외하고, 페이지 전 행이 0이면 완주 워터마크도 남기지 않는다. `minute.candle.zero.page`는 **전 행이 0인 모든 페이지에서** 올린다 — 구분 판정에 쓰여 `J` 전환으로 이어진 페이지도 포함이다(그 페이지가 계측에서 빠지면 판정 경로만 조용해져 오탐·과탐을 못 본다). 시장 구분을 어떻게 고르든 성립해야 하는 방어선이다 — 캐시가 낡거나(신규 NXT 편입) KIS가 다른 이유로 0을 주면 판별 로직과 무관하게 같은 오염이 재발하고, 0행이 마감 봉까지 채워지면 완주 판정이 참이 되어 **그 종목은 그날 다시 조회되지 않는다**(v0.4 구현에서 실제로 발생). 기존 `minute.candle.empty.complete`는 `output2`가 빈 경우만 잡으므로 이 사고를 감지하지 못했다.
- **`J` 경로의 페치 상한은 15:30이다**: 계측상 `J` 응답의 장 마감 이후 구간에는 시간외 체결분이 **조회 시각 부근의 단일 봉으로 얹혀 나온다**(047040을 19:00에 조회하면 `1900` 봉, 19:37에 조회하면 `1937` 봉에 붙었다). 시각이 고정되지 않아 그대로 적재하면 재조회마다 유령 봉이 생긴다. 확정 전까지 `J` 종목은 15:30을 마감 봉으로 두고 장외 구간을 적재하지 않는다 — 계측은 §9.12.
- **오늘 분봉 — 조회 시 신선화(read-through)**: core-api는 분봉 조회 요청을 받으면 먼저 worker-price의 **내부 신선화 API** `POST /internal/minute-candles/{code}/refresh`를 호출한다. worker-price는 `minute_candle`의 당일 최신 행이 신선하면(기본 **60s**, 설정) 그대로 완료를 반환하고, 아니면 `FHKST03010200`을 공백 구간만큼 **전방(저장분 다음 분→최신) 페이징** 호출해(공용 gate 통과, 콜드 최대 **25콜**(`UN`: 08:00~19:59 24콜 + 20:00 마감 행 1콜) 또는 **14콜**(`J`: 09:00~15:29 13콜 + 15:30 마감 행 1콜), 시장 구분 캐시 미스면 판별 프로브 1콜 추가) upsert 후 반환한다. 같은 종목의 동시 신선화는 **single-flight**로 합친다(대기 후 동일 결과 공유 — KIS 중복 구매 금지). core-api는 완료(또는 **타임아웃** — 연결 0.5s + 응답 1.0s) 후 테이블을 읽는다 — 타임아웃·워커 다운이면 저장분만 반환한다(**stale-while-revalidate**: 신선화는 워커에서 계속 진행되고 클라 재조회로 수렴). **데이터 경로는 항상 DB 단일**이고 내부 API는 신선화 트리거일 뿐이다 — 시세 응답 스키마는 core-api가 소유한다.
- **신선화 경계 안전장치**: single-flight는 프로세스 내 합류에 더해 종목별 **분산 락**(`lock:minute-refresh:{code}` — Redis 계약 v0.13, TTL=페치 데드라인+여유)으로 다중 인스턴스에서도 KIS 조회를 1회로 묶는다 — 락 미획득 인스턴스는 no-op(저장분 반환 경로와 동일). 전체 **페치 데드라인**(조회 경로 10s·일 확정 120s)을 넘기면 채운 만큼만 적재하고 종료하는데, 전방 페이징이라 항상 저장분과 연속된 구간만 적재되므로 **중간 구멍이 생기지 않고** 다음 조회가 이어 채운다(누적 거래대금→분당 값 변환의 앵커도 이 연속성에 기대므로 전방 페이징이 전제다). **진행 중인 현재 분은 사 오지 않는다** — 페치 상한은 `min(now−1분, 마감 봉)`이라 확정 분만 적재된다(마감 봉은 `UN` 20:00 · `J` 15:30. 진행 분은 클라의 WS quote 오버레이 몫, core-api 명세 §8). **페치 데드라인은 신선화 1회 전체의 예산이다** — `UN` → `J` 전환이 일어나도 두 pass가 같은 예산을 나눠 쓰고, 첫 콜 이후에는 매 콜 직전에 잔여를 확인한다. pass마다 예산이 새로 열리면 조회 경로가 데드라인의 2배까지 요청 스레드를 붙잡는다. 전환 시점에 예산이 이미 소진됐다면 `J` 조회는 시작하지 않고 구분만 캐시에 남긴다 — 다음 회차가 온전한 예산으로 `J` 조회를 이어받는다. KIS REST 호출은 연결 3s·요청 10s 타임아웃을 가지며, 분봉·일봉 fetcher는 **계정 단위 REST 리미터를 공유하고 그 예산은 `rate-factor×(1−poll-budget-factor)`**(7.5/s)라 폴링 예산(7.5/s)과의 합이 내부 한도(15/s)를 넘지 않는다. 여기에 더해 worker-price의 모든 KIS REST 호출은 **공용 Redis gate**(`rate:kis-rest:{keyId}` 토큰 버킷, §1.3 — capacity·refill=내부 한도, 획득 대기 10s 초과 시 실패)를 통과해 **인스턴스 간 합산도 계정 한도 아래로 묶인다**. worker-batch는 KIS REST 잡 착수 시 같은 gate 구현을 주입한다.
- **서버 간 통신 예외**: 이 내부 API는 "서버 간 통신은 Redis/DB 계약만" 원칙(기획안 §3.1)의 **명시 예외**다 — 멱등 트리거·응답에 데이터 없음·best-effort(실패해도 조회는 저장분으로 동작)로 한정한다. AGENTS.md 의존 규칙에 예외를 기록하고, 이 예외를 데이터 전달 채널로 확장하지 않는다.
- **과거 분봉 — 일 배치 확정**: `minute_candle_daily_sync`가 영업일 **20:05**(마지막 세션 20:00 종료 직후)에 그날 수요·조회 이력이 있던 종목의 당일 1분봉을 확정 적재한다(종목당 최대 25콜(`UN`)·14콜(`J`) — 장중 조회 경로가 이미 채운 구간은 스킵되어 실제 콜은 훨씬 적다). 콜드 종목의 그 이전 **7영업일**(설정)은 수요 0→1 전이(`demand:updated`, §2.1) 시 비동기로 `FHKST03010230`으로 채운다 — 이미 채워진 날은 스킵, 재입장 전이가 중복 콜을 만들지 않는다. ⚠️ **이 백필은 후속 구현이다** — §9.9(`FHKST03010230` 계측) 확정 후 착수하며, 그 전까지 과거 구간은 일 배치가 확정한 날부터만 쌓인다.
- **저장·보존**: 1분봉 원본만 `minute_candle`(§4)에 적재하고 5/15/30/60분은 core-api가 조회 시 파생한다(일→주/월 사다리와 동일, core-api 명세 §8). 보존 **30 달력일**(설정, ≈21 영업일) — 새벽 퍼지 잡이 초과분을 **배치(5천 행) 단위로** 삭제한다(단일 대량 삭제 금지, 상한 도달 시 다음 회차 계속). 조회·수요된 종목만 쌓이므로 안 보는 종목의 콜과 행은 0이다.
- **완주 워터마크**: "마감 봉까지 조회 완료" 표식은 Redis `minute:through:{code}:{date}`(TTL 2일, Redis 계약 v0.13)에 둔다 — 프로세스 로컬이면 리더 전환·재기동 때마다 거래정지·희소 종목을 다시 전 구간 조회하게 된다. 완주 판정은 `마감 봉 존재 ∨ 워터마크 ≥ 마감 봉`이고, **마감 봉은 그 종목의 시장 구분이 정한다**(`UN` 20:00 · `J` 15:30). 워터마크는 완주 판정만이 아니라 **다음 페치 기점의 하한**이기도 하다 — 저장 행이 없어도 워터마크 다음 분부터 이어 조회해, 거래정지·희소 종목이 매 신선화마다 빈 구간을 전부 재스캔하지 않는다. 시장 구분이 바뀌면 그날 워터마크를 **구분 기록보다 먼저** 폐기한다(분봉은 지우지 않는다 — 전환은 적재분 0일 때만 일어난다).
- **범위 통제**: 내부 신선화 API는 `stock_master` 활성 종목만 받는다(그 외 404) — 임의 코드가 KIS 콜을 유발하고 `minute_candle`에 적재되어 일 확정 대상으로 영구 유입되는 것을 막는다. 접근 인증은 §9.11.
- **스케줄러 격리**: 봉 잡은 200ms 틱 flush와 같은 `ThreadPoolTaskScheduler`를 쓰므로 `spring.task.scheduling.pool.size`를 1보다 크게 둔다(기본값 1이면 장시간 봉 잡이 실시간 발행을 굶긴다). 일 확정 잡은 전체 시간 예산(기본 30분)을 넘기면 남은 종목을 다음 회차로 넘긴다.
- **진행 중인 현재 분**: 서버 책임이 아니다 — 클라가 WS `quote`를 마지막 봉에 얹는다(일봉과 동일 패턴, core-api 명세 §5 quote). 신선화 주기(60s)는 확정 분봉의 지연 상한일 뿐 실시간성은 WS가 담당한다.
- **가격 기준**: 분봉은 **원시가**다(수정주가 아님). 보존 30일 안에서 액면분할 등을 감지하면(일봉 재적재 트리거와 동일) 해당 종목 분봉을 전량 삭제 후 재백필한다.

### 2.7 장 운영 캘린더

수신 대상 세션 전체(KST): NXT 프리마켓 08:00–08:50 · KRX/NXT 메인 09:00–15:30 · NXT 애프터마켓 15:30–20:00 · KRX 장후 시간외종가 15:40–16:00 · KRX 시간외단일가 16:00–18:00. **07:50 세션 준비(토큰·Approval·연결) → 08:00 구독 → 20:00 구독 해제·유휴.** 주말·KRX 휴장일은 스킵한다(MVP: 휴장일 YAML 수동 관리, P3: 캘린더 소스 자동화). KIS 새벽 점검 시간대에는 재접속을 억제한다. 이 캘린더 판정은 worker-batch의 영업일 잡(`invest_opinion_sync` 등)도 공유하는데, 서버 간 코드 의존 금지로 **휴장일 목록을 price·batch가 각자 설정으로 든다** — 휴장일을 추가할 때 두 모듈 설정을 함께 갱신한다(한쪽만 고치면 batch가 휴장일에 KIS를 조용히 호출한다).

### 2.8 단일 실행 보장 & 그레이스풀 셧다운

- worker-price는 세션 풀·구독 배정을 인메모리로 드는 상태有 워커이고, KIS WS는 계좌당 1세션이다(§1.3). 같은 계정으로 두 인스턴스가 동시에 돌면 안 되므로 **리더 락** `SET worker:price:leader {instanceId} NX PX 30000` + 10s 갱신으로 단일 실행을 보장한다. 락 미획득 인스턴스는 스탠바이로 대기한다.
- SIGTERM: 신규 구독 중지 → 전 세션 `tr_type=2` 해제 → WS close → 락 해제. 배포 중 틱 공백 수십 초는 허용한다 — NFR상 무손실 대상이 아니다.

---

## 3. worker-batch (스케줄 수집)

### 3.1 잡 카탈로그

| 잡 | 소스 · TR/엔드포인트 | 스케줄 (KST) | 대상·볼륨 | 멱등 키 |
|---|---|---|---|---|
| `stock_master_sync` | KIS 마스터 파일 `https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip`·`kosdaq_code.mst.zip`·`idxcode.mst.zip` (CP949, 고정폭 — KIS GitHub 파서 참조) | 매일 08:00 (원본 07:40경 갱신) | 전 종목 ~2,600 + 업종 ~490 | `code` upsert |
| `daily_candle_sync` | KIS `FHKST03010100` (§2.6 — 실행 주체는 price, 트리거·이력 관리는 batch 잡 테이블로 일원화 가능. MVP: price 내 스케줄) | 영업일 16:30 (+미완주 시 17·18·19시 재시도) | 전 종목(`stock_master` 활성) | `(code,date)` |
| `minute_candle_refresh` | KIS `FHKST03010200` (§2.6 — worker-price 내부 API, core-api 조회가 트리거) | 조회 시 (신선 60s 이내면 no-op, single-flight) | 조회된 종목의 당일 공백 구간 | `(code,date,time)` |
| `minute_candle_daily_sync` | KIS `FHKST03010200` (§2.6 — 실행 주체는 price. **완주 판정 = 20:00 봉 존재 또는 20:00까지 조회 완료 워터마크**(희소·거래정지 종목은 마지막 체결이 일러도 완주). 종목별 시도 3회·백오프, 예외·경합은 다음 회차 재시도) | 영업일 **20:05** + 21·22·23시 15분 재시도 — **회차마다 대상을 재계산**해 미완주가 없으면 no-op | 당일 수요·조회 이력 종목(`symbols ∪ minute_candle` 당일 적재 종목) | `(code,date,time)` |
| `minute_candle_backfill` | KIS `FHKST03010230` (§2.6 — 실행 주체는 price, ⚠️ 후속 구현 — §9.9 계측 후) | 수요 0→1 전이 즉시(비동기) | 콜드 종목 × 직전 7영업일 | `(code,date,time)` |
| `minute_candle_purge` | DB 삭제 (§2.6 — 보존 30일 초과분) | 매일 04:30 | `minute_candle` 보존 초과 행 | `(code,date,time)` |
| `valuation_daily` | KIS `FHKST01010100` 응답의 `per,pbr,eps,bps` + 마스터 시총(상장주식수×종가) | 영업일 16:50 (+부분 실패 시 17:20·18:20 재실행 — §3.2 예외) | 전 종목 (~2,600콜, 재실행 회차당 +2.6k콜) | `(code,date)` |
| `investor_flow_daily` | KIS `GET .../inquire-investor` · TR `FHKST01010900` — **장마감 후 확정치** | 영업일 17:10 | 전 종목 | `(code,date)` |
| `invest_opinion_sync` | KIS `GET /uapi/domestic-stock/v1/quotations/invest-opbysec` · TR `FHKST663400C0` — 회원사 코드별 전 종목 투자의견(의견·직전의견·목표가). 회원사 목록은 `memcode.mst.zip`(§3.3) | 영업일 07:00 이상 18:00 미만 **10분 주기**(07:00~17:50) | 회원사 61개 × 1콜(연속조회 없음 — §3.3) | `(code, business_date, broker_code, content_hash)` |
| `dart_corp_map` | OpenDART `corpCode.xml`(zip) — corp_code↔종목코드 매핑 | 주 1회 | 전 상장사 | `corp_code` |
| `industry_sync` | OpenDART `corpCode.xml` → `company.json`(`induty_code`) + KSIC 10차 분류표(worker-batch 리소스 `ksic10.csv`) | 주 1회 일 06:30 (`dart_corp_map`과 한 잡) | 활성 종목 ~2.6k (우선주 제외 — DART는 보통주에만 corp_code 부여) | `code` upsert |

**업종 소유권**: `stock_master_sync`는 종목명·시장·상장주식수·상장일·활성 여부만 소유하고 **업종 필드를 갱신하지 않는다**(KIS 마스터의 업종 파싱·`sector` 적재는 제거). `sector` 카탈로그와 `stock_master.sector_code`·`dart_induty_code`는 `industry_sync`가 단독 소유한다 — 두 잡이 같은 컬럼을 쓰면 일 배치가 주 배치 결과를 덮는다.

**전환 절차**: 마이그레이션은 기존 `sector` 행과 `stock_master.sector_code`를 지우지 않는다. 지우면 첫 `industry_sync`가 끝날 때까지 worker-llm이 fail-closed로 멈추고, 그 사이 들어온 기사는 PEL 재시도 한도를 넘겨 `queue:ingest:dlq`로 영구 격리된다(DLQ 재처리 경로 없음). KIS 업종축을 그대로 둔 채 `industry_sync`가 `sector_code`를 KSIC로 덮어쓰게 하고, 참조가 끊긴 KIS 업종 행은 `allSectors()`의 사용 중 필터에서 자연히 빠진다.
| `financials_sync` | OpenDART `list.json`(신규 정기공시 감지) → `fnlttSinglAcntAll.json` (`bsns_year`, `reprt_code` 11013/11012/11014/11011, `fs_div=CFS`→미존재 시 `OFS`) | 매일 06:00 (공시 시즌 증분) | 신규 공시 기업만 | `(corp_code, year, reprt_code)` |

업종은 `idxcode.mst`(45바이트 고정폭 — 코드 5자리 + 이름)가 코드와 이름을 함께 준다. 종목 마스터의 업종 필드는 4자리라 그대로는 `sector.code`와 맞지 않는다. **앞에 시장 접두어(KOSPI `0`, KOSDAQ `1`)를 붙여 5자리로 맞춘다** — 예: KOSPI `0027` → `00027`(제조), KOSDAQ `1009` → `11009`(제조). 두 시장이 별개 코드 대역을 쓰므로 접두어 없이는 서로 충돌한다.

일일 KIS 호출 예산(1계정): candle 2.6k + valuation 2.6k + investor 2.6k ≈ **7.8k콜**(15/s 페이싱 ~9분) + 분봉(상시 폴링 없음 — 조회 연동 신선화·일 배치·콜드 백필 모두 조회/수요 종목에 비례, 공용 gate 안에서 흡수) + opinion 61사 × 1콜 × 66회 ≈ **4.0k콜**(§9-7 실측 — 연속조회 미지원이라 페이지 변수 없음). opinion 잡 자체 상한은 **4콜/s**로 두고(회차당 ~15초), 공용 Redis gate가 price REST 폴링과의 합산을 계정 내부 한도(15/s) 아래로 묶는다. 앞 회차가 10분 안에 끝나지 않으면 다음 회차는 ShedLock 획득 실패로 건너뛰고 `opinion_sync_overrun`을 기록한다. OpenDART는 일일 한도 내 여유가 있다(분기 시즌에도 수천 콜) — 정확한 한도는 포털에서 확인한다(§9).

### 3.2 실행 프레임워크

- Spring `@Scheduled` + **ShedLock**(Redis) — 다중 기동에 안전하다. 잡 이력은 테이블 `batch_job_run(job, run_date, status, ok_count, fail_count, started_at, finished_at, error)`에 기록하고, 동일 `(job, run_date)` SUCCESS가 있으면 스킵한다(재실행 멱등).
- **일내 반복 잡 예외**: `invest_opinion_sync`(10분 주기)는 `(job, run_date)` SUCCESS 스킵을 적용하지 않는다 — 실행 이력만 기록하고 매 회 실행한다. 멱등은 잡 내부의 upsert·미발행 스캔(§3.3)이 담당한다. ShedLock은 동일하게 적용해 실행 중인 회차가 있으면 다음 트리거를 시작하지 않는다. lock TTL은 최대 실행시간보다 길어야 한다 — 잡은 **회차 데드라인**(lock TTL보다 짧게, 최악 단일 콜 소요를 더해도 TTL 미만)을 두고 도달 시 잔여 회원사를 다음 회차로 미룬다. KIS가 전반적으로 느려도 락이 실행 중에 만료되지 않아 다중 기동에서 동시 실행이 생기지 않고, 이연분은 조회 창이 직전 영업일을 포함하므로 유실되지 않는다. 순회 시작 위치는 **트리거 시각에서 유도**한다(10분 회차 번호 mod 회원사 수) — 어떤 인스턴스가 락을 잡아도 같은 회차엔 같은 위치에서 시작하고 회차마다 회전하므로, 이연이 반복돼도 특정 회원사가 계속 뒤로 밀리지 않는다(인메모리 커서 없음 — 재기동 안전).
- 실패 종목은 잡 말미에 1회 재시도한다. 잔여 실패는 `fail_count`+로그로 남기고 다음 날 upsert로 자연 회복한다. **예외 — `valuation_daily`**: 날짜 키 스냅샷이라 다음 날 실행이 그날의 공백을 채우지 못한다. 부분 실패 회차는 SUCCESS 대신 **FAILED로 기록**하고(ok/fail 카운트 보존 — `failCounted`), 17:20·18:20 재실행 크론이 FAILED 회차만 전 종목 재조회로 채운다(SUCCESS면 no-op, upsert 멱등). 마지막 재실행까지 실패가 남으면 FAILED로 확정돼 알람 대상이 되고, 그날 행은 수동 재실행으로만 채울 수 있다.
- 재무 요약 변환: DART 계정과목 → `revenue/operatingProfit/netIncome/assets/liabilities/equity` 매핑 테이블(연결 우선). 매핑 불가 계정은 raw 보존 없이 스킵+카운트한다(포트폴리오 범위 단순화).
- **기동 시 미실행 회차 따라잡기**: 워커가 잡 시각에 떠 있지 않으면(개발 노트북·재배포·장애) 그날 회차는 영원히 실행되지 않는다. `@Scheduled`는 지나간 시각을 소급해 주지 않기 때문이다. 그래서 `ApplicationReadyEvent`에서 **일 1회 잡의 오늘 크론 시각이 이미 지났는지** 확인하고, 지났으면 그 잡을 호출한다(아래 "한 번으로 끝내지 않는다"의 재확인 패스 포함). 대상은 `stock_master_sync`·`valuation_daily`·`investor_flow_daily`·`financials_sync`이며 **이 순서로 순차 실행**한다 — 콜드 부팅에서 `stock_master`가 비어 있으면 뒤의 잡들이 빈 유니버스로 0건 성공해 버린다. 제외 대상: `invest_opinion_sync`(10분 주기라 다음 회차가 곧 온다), `industry_sync`(주 1회인데 `run_date`가 일 단위라 "오늘 SUCCESS 없음"이 매일 참이 된다).
  - 중복 실행 방지는 새로 만들지 않는다. 잡이 이미 성공했으면 `runs.start`가 null을 돌려 잡 스스로 스킵하고, 다른 인스턴스가 같은 잡을 돌고 있으면 ShedLock이 막는다(따라잡기도 잡의 `@SchedulerLock` 진입점을 그대로 호출한다). 휴장일 판정도 잡 내부 캘린더가 그대로 맡는다.
  - **따라잡기는 "호출했다"까지만 보장한다**(`batch.catchup.triggered{job}`). ShedLock이 스킵하면 호출은 조용히 반환하므로 그 회차가 실제로 돌았는지는 `batch_job_run`이 진실이다. 락을 살아 있는 인스턴스가 쥔 경우는 그쪽이 처리하므로 유실이 아니다. 크래시가 남긴 스테일 락은 아래 재확인 패스와 `valuation_daily`의 17:20·18:20 재실행 크론이 함께 회수하며, 그마저 놓친 잡은 명세의 기존 규칙대로 다음 회차/다음 날 회복에 맡긴다.
  - **기동을 막지 않는다** — 별도 스레드에서 순차 실행하고, 한 잡이 실패해도 다음 잡을 계속 돌린다(실패는 `batch.catchup.failed{job}`).
  - **종료 생명주기에 참여한다**(`DisposableBean`). 시작은 `ApplicationReadyEvent`로 유지한다 — 컨텍스트 새로고침 중에 시작하면 워커가 준비되기도 전에 배치 부작용이 나간다. 종료가 시작되면 남은 잡을 **새로 시작하지 않고**(남은 목록을 로그로 남긴다) `stop-timeout`(기본 5s)까지만 join한다 — 잡 하나가 수 분이 걸리므로 완주를 기다리지 않는다. 실행 중이던 잡은 `batch_job_run`에 RUNNING으로 남고 `start`의 RUNNING 재시작 규칙이 회수한다.
    - **스레드를 인터럽트하지 않는다.** 잡들의 종목 단위 실패 처리가 `InterruptedException`을 일반 예외로 흡수하므로(예: `withOneRetry`의 `runCatching`, 밸류에이션·수급의 종목 단위 `catch`), 인터럽트를 쏘면 "종료"가 "종목 하나 실패"로 둔갑해 잡이 계속 돌고 누락된 데이터를 확정할 수 있다. 대신 패스 사이 대기를 잘게 쪼개 종료 플래그를 확인하므로, 대기 중이던 워커는 인터럽트 없이도 즉시 멈춘다. 잡 실행 중이면 join 상한까지만 기다리고 나머지는 JVM 종료에 맡긴다(데몬 스레드).
    - 종료 플래그 확인과 잡 시작 사이의 창은 원리적으로 없앨 수 없다(없애려면 종료가 잡 완주를 기다려야 한다). 그 경계에서 시작된 잡은 멱등이고 RUNNING 행으로 회수되므로 손실이 아니다.
  - **한 번으로 끝내지 않는다**: 앞 프로세스가 재배포·크래시로 죽으면 ShedLock 락이 `lockAtMostFor`까지 남아, 곧바로 뜬 새 인스턴스의 첫 패스가 통째로 스킵된다. 그래서 첫 패스에서 하나라도 **호출을 시도했으면**(성공 여부가 아니라 시도 기준 — 창이 안 온 선행 잡의 창이 그 사이 열릴 수 있으므로 전부 실패한 패스도 재확인 대상이다) `pass-interval`(기본 20분) 간격으로 `passes`(기본 3)회까지 다시 훑는다. `passes`는 1 이상, `pass-interval`은 양수여야 하며 아니면 기동에서 실패시킨다. 이미 성공한 잡은 `runs.start`가 null을 돌려 DB 조회 1회로 끝나므로 반복 패스는 정상 상태에서 사실상 무비용이고, 첫 패스가 아무것도 호출하지 않았으면 재확인하지 않는다.
  - **재확인 지평은 `valuation_daily` 기준(총 40분 > 그 잡의 `PT30M` 락)으로 잡는다.** 다른 잡의 락까지 덮도록 늘리지 않는다 — 재확인은 "락에 막혀 못 돈 회차"와 "돌았는데 부분 실패한 회차"를 구분하지 못하므로, 지평을 늘리면 부분 실패한 `valuation_daily`의 전 종목 재조회(회차당 ~2.6k콜)만 배가된다. 지평을 벗어나는 경우의 손실은 잡마다 다르고 valuation만 복구 불가다: `financials_sync`는 락이 `PT2H`라 40분 지평을 벗어날 수 있지만 7일 감지 창이 있어 다음 날 같은 공시를 다시 잡고, `investor_flow_daily`는 다음 날 응답이 과거 구간을 포함하며, `stock_master_sync`는 하루 낡은 마스터를 감수한다. 이 재확인이 없으면 마지막 재실행 크론(18:20) 이후의 재배포에서 그날 `valuation_daily`가 영구 누락된다.
  - **한계 — 자정 부근**: 모든 잡은 자기 `today()`로 `run_date`를 정하므로 따라잡기는 **오늘 회차만** 복구할 수 있다. 자정 직전에 스테일 락에 막히면 다음 패스는 날짜가 넘어가 어제 창을 더 이상 인정하지 않고, 어제 회차는 복구되지 않는다. 지난 날짜를 채우려면 잡을 `run_date` 파라미터로 실행하는 별도 수동 경로가 필요하며 현재 범위 밖이다.
  - 크론 시각은 각 잡의 설정(`alphatalk.batch.*.cron`)을 그대로 읽어 판정한다 — 따라잡기 전용 시각을 따로 두면 크론을 바꿀 때 어긋난다. 크론이 비활성(`-`)이면 그 잡을 건너뛴다. 창 판정은 **잡마다 그 시점의 시각으로 다시 한다** — 앞 잡이 자정을 넘기면 뒤 잡은 새 날짜 기준으로 판정해야 아직 오지 않은 그날 회차를 앞당겨 실행하고 SUCCESS로 막는 일이 없다. 게이트는 `alphatalk.batch.catch-up.enabled`(기본 on)이며 `alphatalk.batch.enabled`도 함께 본다 — 후자가 꺼지면 `@EnableSchedulerLock`이 등록되지 않아 따라잡기가 락 없이 돌게 된다.
- **선행 데이터 부재는 성공이 아니다**: `valuation_daily`·`investor_flow_daily`·`financials_sync`는 `stock_master`가 비면 0건 SUCCESS가 아니라 FAILED로 남긴다. 성공으로 확정하면 그날 재실행이 막혀(`start`의 SUCCESS 스킵) 마스터가 뒤늦게 적재돼도 그날 지표가 비게 된다. 따라잡기가 `stock_master_sync`를 먼저 돌리는 것과 같은 이유이며, 순서만으로는 부족하다 — 잡별 크론 시각이 달라 기동 시점에 따라 마스터 창은 아직 안 지나고 뒤 잡 창만 지난 조합(예: 06:00~08:00 기동)이 생기기 때문에, 순서가 아니라 **데이터 유무로** 막는다.

### 3.3 투자의견 → 실시간 소식 파이프라인 (`stream:{code}` 직접 발행) — ★ Redis 계약 v0.7·WS 계약 v0.6 반영

투자의견은 DB 적재로 끝나지 않고 **소식(stream) 경로로 클라이언트에 실시간 통보**한다. 큐·llm-worker를 거치지 않고 **worker-batch가 `stream_event` 저장 후 `stream:{code}`를 직접 PUBLISH**한다 — 메인서버의 post 경로와 같은 **persist-then-publish** 패턴(계약 §1 원칙). STOMP 목적지·봉투는 그대로 재사용하고, `stream.data`만 WS v0.6에서 하위 호환 확장한다.

```
worker-batch invest_opinion_sync (영업일 07:00~17:50 · 10분 주기)
  ⓪ 회원사 마스터 memcode.mst.zip 일 1회 갱신(인메모리 캐시)
       - 실패 시 직전 목록 사용, 목록이 아예 없으면 그 회차 스킵 — 조회 창이
         직전 영업일을 포함하므로 하루 안 복구가 유실이 되지 않는다
  ① 회원사별 FHKST663400C0 조회 (61사 × 1콜, 연속조회 없음 — §9-7)
       - FID_COND_MRKT_DIV_CODE=J, FID_COND_SCR_DIV_CODE=16634
       - FID_INPUT_ISCD={회원사 5자리 코드의 뒤 3자리}, FID_DIV_CLS_CODE=0
       - FID_INPUT_DATE_1={직전 영업일}, FID_INPUT_DATE_2={당일}
       - 응답은 최신순 최대 100행 — 100행이 오면 절단 가능성으로 보고
         opinion_page_truncated 메트릭·경고를 남긴다(조회 창이 2영업일이라 실제 도달 희박)
  ② 응답 정규화 + content_hash 생성 후 invest_opinion INSERT
       - code=stck_shrn_iscd, rating=invt_opnn, previous_rating=rgbf_invt_opnn,
         target_price=hts_goal_prc(0이면 NULL) — invt_opnn_cls_code류는 위치 값이라 버린다
       - broker_code는 회원사 마스터의 5자리 코드(요청 코드의 원형), broker_name은 응답 mbcr_name
         (trim, 빈 값이면 마스터 이름, 그것도 없으면 NULL)
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
  "title": "미래에셋 투자의견 매수",
  "occurredAt": 1785106800000,
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

- `businessDate`는 KIS `stck_bsop_date`를 그대로 보존한다. KIS가 시각을 주지 않으므로 `occurredAt`은 해당 observation을 처음 수집한 `collected_at`으로 고정한다.
- **회원사 마스터**: `memcode.mst.zip`(종목 마스터와 같은 다운로드 호스트, zip 안 CP949 텍스트 — 5자리 코드 + 이름 + 외국계 플래그). `99999`(외국계합) 같은 집계 행은 조회 대상에서 제외한다. `brokerCode`는 이 마스터의 5자리 코드(정본), 요청 `FID_INPUT_ISCD`는 그 뒤 3자리다 — 5자리를 그대로 보내면 `rt_cd=0`에 0행이 오는 조용한 실패이므로 변환을 테스트로 고정한다(§9-7).
- `brokerName`은 응답 `mbcr_name`(trim)을 쓰고, 빈 값이면 마스터 이름, 그것도 없으면 `null`·제목 `"{brokerCode} 투자의견 {rating}"`으로 발행한다. `rating`·`previousRating`은 회원사 표기 그대로다(`매수`·`BUY`·`NotRated` 등 — 표준화하지 않는다). 응답의 `invt_opnn_cls_code`·`rgbf_invt_opnn_cls_code`는 등급 분류가 아니라 위치 값(현재=2·직전=3 고정 — §9-7 실측)이므로 사용하지 않는다.
- `content_hash` = `SHA-256(rating + "|" + previousRating + "|" + targetPrice)` 소문자 hex. 해시 전 문자열 필드는 trim하고 `null`은 빈 문자열, 목표가는 부호 없는 10진 문자열로 정규화한 UTF-8 바이트를 사용한다(`hts_goal_prc=0`은 목표가 없음 → `null` → 빈 문자열). 같은 날 같은 회원사의 의견·목표가가 바뀌면 별도 observation과 이벤트가 된다.
- `source_key` = `opinion:{code}:{businessDate}:{brokerCode}:{contentHash}`. 해시로 회원사 자체를 식별하지 않고 회원사 마스터의 5자리 코드를 사용한다.
- `stream_event.type=REPORT`, `stream_event.source={brokerCode}`, payload는 위 JSON이다. 발행 봉투·채널명·카테고리·payload 타입은 전부 `:contracts` 상수/DTO를 사용한다.
- `:contracts`는 `StreamData.kind: String?`, `StreamData.opinion: OpinionData?`와 위 `OpinionData` 필드를 추가한다. 기존 payload 기준으로 두 최상위 필드는 nullable인 하위 호환 확장이고, `OpinionData` 내부의 필수·nullable 구분은 WS 명세 v0.7 §4.3을 따른다. JSON 계약 테스트로 기존 news/report/ai payload가 변하지 않음을 고정한다.
- `sentiment`는 싣지 않는다 — 의견·목표가 자체가 정보이고, "매수=POSITIVE" 같은 기계 매핑을 하지 않는다. 일일 다이제스트(뉴스 명세 §4.2) 취합 대상도 아니다(클러스터 기반이 아니므로 자연 제외).
- **소유 경계**: 이 경로로 worker-batch는 `stream_event` INSERT·`stream:{code}` PUBLISH 주체가 된다 — llm-worker와 **공동 생산자**(계약 §1.1 v0.7). `stream_event`의 논리 소유자는 core-api stream 모듈이고, DDL은 `db-migrations`가 단일 소유한다.
- 주기: **영업일 07:00 이상 18:00 미만 KST · 10분** — 장외 시간·주말·휴장일(§2.7 캘린더 공유)은 스킵한다. 호출 예산·overrun 정책은 §3.1 참조.

## 4. 워커 소유 데이터 스키마 (Liquibase 관리 — `db-migrations` 모듈)

읽기 전용 소비자: core-api(시세·지표 조회) 외에 **worker-llm**이 `daily_candle`·`investor_flow_daily`·`stock_master`를 시장 다이제스트 팩트시트(업종별 등락·수급 집계)용으로 조회한다([뉴스 워커 명세](alphatalk_news_worker_spec.md) §4.3). 쓰기 주체는 변함없이 price/batch 워커뿐이다.

```
stock_master(code CHAR(6) PK, name, market, sector_code NULL, shares_outstanding BIGINT,
             is_active BOOL, listed_at NULL, updated_at)
sector(code TEXT PK, name)   -- KIS 업종 마스터 idxcode.mst에서 stock_master_sync가 함께 적재(§3.1)
daily_candle(code, date CHAR(8), open, high, low, close INT, volume BIGINT, value BIGINT,
             PK(code, date))
minute_candle(code, date CHAR(8), time CHAR(4), open, high, low, close INT, volume BIGINT, value BIGINT,
             PK(code, date, time))  -- 1분봉 원본(원시가), time=봉 시작 HHmm, 보존 30일(§2.6) — 5/15/30/60분은 core-api가 조회 시 파생
valuation_daily(code, date, per NUMERIC, pbr NUMERIC, eps INT, bps INT, market_cap BIGINT,
             PK(code, date))
investor_flow_daily(code, date, individual BIGINT, foreign BIGINT, institution BIGINT,  -- 순매수 백만원
             PK(code, date))
invest_opinion(code CHAR(6), business_date CHAR(8), broker_code TEXT, broker_name TEXT NULL,
             rating TEXT, previous_rating TEXT NULL,
             target_price BIGINT NULL, content_hash CHAR(64), collected_at TIMESTAMPTZ,
             stream_event_id CHAR(26) NULL, published_at TIMESTAMPTZ NULL,
             PK(code, business_date, broker_code, content_hash))
-- INDEX (collected_at) WHERE published_at IS NULL — 미통보 스캔(§3.3 ③)이 테이블 누적과 무관하게 좁게 돌도록
-- rating_code·previous_rating_code 없음: KIS cls_code는 위치 값이라 저장하지 않는다(§3.3, v0.6)
stream_event(..., source_key TEXT NULL, ...)
-- UNIQUE(source_key) WHERE source_key IS NOT NULL
-- source_key DDL의 논리 소유자는 core-api stream, changeSet 파일의 단일 소유자는 db-migrations
dart_corp_map(corp_code CHAR(8) PK, code CHAR(6) UQ NULL, corp_name, modify_date, updated_at)
sector(code PK, name, level SMALLINT, parent_code NULL, version)  -- KSIC 10차 전 계층(2~5자리)
stock_master(..., sector_code,        -- 라우팅에 쓰는 유효 KSIC 코드 → sector.code
             dart_induty_code, ...)   -- DART 신고 원본(2~5자리, 회사마다 깊이가 다르다)
-- INDEX sector (parent_code) · stock_master (sector_code)
financial_summary(code, year SMALLINT, reprt_code CHAR(5), fs_div CHAR(3),
             revenue BIGINT, operating_profit BIGINT, net_income BIGINT,
             assets BIGINT, liabilities BIGINT, equity BIGINT, disclosed_at,
             PK(code, year, reprt_code))
batch_job_run(id, job, run_date, status, ok_count, fail_count, started_at, finished_at, error)
```

읽기 소비자는 core-api stockinfo/search 모듈(REST 명세 §8)과 worker-llm 섹터 해소([뉴스 파이프라인 명세](alphatalk_news_worker_spec.md) §3.6)이며, **둘 다 같은 `sector`·`stock_master.sector_code` 축을 본다**(v0.3에서 KIS 대분류 18종 → KSIC로 교체). core-api는 `sector.name`만 읽으므로 이름이 바뀌어도 API 형태는 깨지지 않는다.

`sector_code` 배정 규칙: KSIC 소분류(3자리)에서 시작해, 구성 종목이 `group-max-size`(기본 100, worker-llm `fanout-cap`과 맞춘다)를 넘는 그룹만 한 단계씩 세세분류(5자리)까지 내린다. **더 내려갈 자릿수가 없는 그룹은 상한을 넘어도 배정을 유지하고 경고·메트릭(`batch.industry.oversized`)만 남긴다** — DART 신고 코드가 3자리뿐인 종목이 100개를 넘으면 어떤 자릿수로도 쪼개지지 않으므로, 여기서 잡을 실패시키면 같은 입력으로 매주 실패해 업종축이 영구히 비게 된다. 그 경우의 노이즈 방어는 worker-llm의 2단 상한이 맡는다 — `fanout-cap`(100)을 넘으면 `impact=LOW` 섹터를 덜어내고, `fanout-hard-cap`(500)까지 넘으면 실시간 발행을 억제한다([뉴스 파이프라인 명세](alphatalk_news_worker_spec.md) §3.6). **실시간 배달의 실질 상한은 `group-max-size`가 아니라 `fanout-hard-cap`이다** — 쪼갤 수 없는 300종목 그룹이 `impact=HIGH`로 판정되면 300개 방에 배달된다. 반대로 **쪼갤 수 있는데도 상한을 넘는 배정이 남으면 잡을 실패시킨다** — 그건 배정 로직의 결함이다. 그룹 크기는 **조회 성공분이 아니라 활성 종목 전체**(직전 실행의 `dart_induty_code` 포함)로 계산한다. 부분 성공분만으로 재계산하면 같은 산업이 3자리와 4자리 그룹으로 갈라진다. KSIC 표에 없는 옛 코드는 상위 분류 이름으로 대체하고, DART 신고 업종이 뉴스 맥락과 어긋나는 소수 종목은 `group-overrides`로 `sector_code`만 보정한다(`dart_induty_code` 원본은 보존).

OpenDART 응답 상태는 **세 갈래로 나눈다**. ① 데이터 없음(`013`)·회사코드 목록에서 사라진 종목은 **배정 해제 대상**이다 — `sector_code`와 `dart_induty_code`를 함께 비운다. 둘 중 하나만 지우면 다음 실행이 낡은 `dart_induty_code`를 되살려 배정이 영원히 회수되지 않는다. ② 인증·요청제한·시스템 점검(`010`·`011`·`012`·`020`·`021`·`100`·`101`·`800`·`901`)은 예외로 전파해 잡을 실패시킨다. ③ 네트워크 오류·타임아웃·응답 파싱 실패는 종목 단위 실패로 흡수하고 1회 재시도한다 — 2,600건을 순차 호출하는 잡에서 한 건의 연결 끊김이 전체 회차를 버리면 안 된다.

**부분 성공의 경계**: 종목 단위 실패가 `max-failure-ratio`(기본 5%)를 넘으면 아무것도 저장하지 않고 잡을 실패시킨다. 전량 실패를 성공으로 기록하면 다음 실행이 같은 날 열리지 않는다. 허용치 안이면 실패 종목은 직전 실행의 `dart_induty_code`를 그대로 유지한 채 그룹 계산에 포함한다.

**쓰기는 네 트랜잭션으로 나뉜다**(`dart_corp_map` → `sector` → `stock_master` 배정 → 배정 해제). 중간에 프로세스가 죽으면 축이 반만 적용된 상태로 남고, 회복은 다음 실행의 재적재에 맡긴다 — 모든 쓰기가 upsert라 재실행이 수렴한다. 스냅샷을 staging 테이블에 적재한 뒤 짧은 트랜잭션으로 교체하는 방식이 더 안전하지만, 주 1회·단일 인스턴스 잡이라 현 단계에서는 부분 적용을 **의식적으로 수용**한다. 대신 아래를 운영 조건으로 둔다.

| 조건 | 내용 |
|---|---|
| 알람 | `batch_job_run`의 `industry_sync`가 `RUNNING`(선행 회차 중단) 또는 `FAILED`로 남으면 알람. 주 1회 크론이라 자동 회복까지 최대 일주일이다 |
| 수동 재실행 | 잡을 다시 호출하기만 하면 된다 — `BatchJobRunStore.start`가 `RUNNING`·`FAILED` 행을 같은 `run_date`로 재시작한다(`SUCCESS` 행만 스킵). **행을 지우지 않는다** — 감사 이력이다 |
| 첫 전환 배포 | ① `industry_sync` 성공 확인 → ② 활성 종목 배정 수(`sector_code is not null`)와 `batch.industry.oversized = 0` 확인 — 0이 아니면 더 쪼갤 수 없는 초과 그룹이 있다는 뜻이므로 로그의 그룹 목록을 보고 운영 승인 후 진행 → ③ worker-llm 기동. 순서를 지키지 않으면 worker-llm이 fail-closed로 멈춘다 |
| 메트릭 | `batch.industry.synced` · `batch.industry.failed` · `batch.industry.oversized` |

**`sector.version`은 행의 출처를 표시한다** — `industry_sync`가 upsert한 행만 `KSIC_10`이고, 전환 전부터 있던 KIS 업종 행은 `KIS_MASTER`로 남는다(컬럼 기본값도 `KIS_MASTER`). 판별은 `level`로 한다 — 전환 마이그레이션이 KIS 행을 `level = 0`으로 남기고 KSIC 행은 항상 `level >= 2`다. 전환이 끝나 참조가 사라진 `KIS_MASTER` 행은 정리 가능하다. 금액 컬럼은 원 단위로 저장하고, API 단위 변환은 core-api 책임이다(명세와 합의).

## 5. 설정·환경변수

| 변수 | 예 | 설명 |
|---|---|---|
| `KIS_ACCOUNTS` | `[{"keyId":"a1b2c3d4","appkey":"...","appsecret":"..."}]` | 세션풀 계정 목록(시크릿 매니저 주입) |
| `KIS_RATE_FACTOR` | `0.75` | 공식 유량 대비 내부 한도 비율 |
| `DART_API_KEY` | — | OpenDART |
| `DEMAND_RECONCILE_SEC` / `CONFLATION_MS` | `60` / `200` | §2 파라미터 |
| `MINUTE_CANDLE_FRESH_SEC` / `MINUTE_CANDLE_RETENTION_DAYS` | `60` / `30` | §2.6 분봉 신선화 임계·보존 |
| `TICK_SILENCE_MS` | `20000` | §2.3 등록 후 이 시간 동안 체결 틱이 없으면 degraded로 강등해 REST 폴링에 넘긴다 |
| `MARKET_HOLIDAYS_FILE` | `holidays-2026.yml` | 휴장일 |
| `REDIS_URL` / `DB_URL` | — | 공용 |

## 6. 관측성

메트릭: `kis_ws_sessions{state}` · `kis_subscribed_symbols` · `demand_symbols` · `degraded_symbols` · `tick_in_rate`/`quote_publish_rate` · `conflation_lag_ms` · `pingpong_miss` · `rest_call_rate{keyId}` · `rest_throttled` · `token_refresh_total` · `batch_job_duration/fail{job}`. 로그는 구조화 JSON으로 남기고 appkey/token은 마스킹한다. 프레임 원문은 DEBUG+샘플링으로만 남긴다. 실시간 메트릭(§2.3): `tick.div.resubscribed`(구분 기록 갱신으로 재구독) · `tick.silence.degraded`(등록됐는데 체결 틱이 없어 REST 폴링으로 넘긴 종목) · `tick.market.div{div}`(실시간이 확정한 구분 — `UN`만 나온다). **`tick.silence.degraded`는 발생 즉시 알람** — 실시간 경로가 그 종목을 못 받고 있다는 뜻이다. 분봉 메트릭(§2.6): `minute.candle.refresh` · `minute.candle.upsert.retry` · `minute.candle.empty.complete`(`output2`가 빈 채로 완주) · `minute.candle.zero.page`(전 행 0인 페이지 — 시장 구분 오판·KIS 이상) · `minute.candle.value.regressed`(누적 거래대금 역행 = 시장 구분 혼입 의심) · `minute.candle.market.div{div}`(날짜별 판별 결과 분포) · `minute.candle.div.unknown`(그날 구분 기록 없이 적재분만 있어 갱신을 건너뛴 횟수 — Redis 유실 신호). 알람: WS 세션 전멸 5분, 장중 tick_in=0, 배치 실패, throttled 급증, `candle_sync_aborted`(유니버스 조회 실패로 일봉 회차 중단), `candle_sync_failed` 지속(재시도 회차까지 남는 종목 실패 — 둘 다 §2.6), **`minute.candle.value.regressed` 발생 즉시**(앵커 오염은 조용히 번진다).

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
- 실계정 스모크: 장중 삼성전자 1종목 실수신 → `quote:005930` 발행 확인. 실전 키만 쓰므로 스모크는 최소 종목·최소 시간으로 제한한다.

## 9. 오픈 이슈 (구현 착수 전 확인)

1. **KIS 2026-03-20 "신규 고객 초당 호출 제한" 공지 원문** — §1.3 수치 재확인 (포털 공지사항).
2. ~~모의투자(vts) WS의 TR 지원 범위~~ — **종결**. vts 환경을 걷어내고 실전 단일 운영으로 확정했다(§1.1).
3. 체결가 TR 3종의 전체 필드 순서 — 공식 예제(open-trading-api) columns 전 필드 대조 완료(46·46·43, idx 0~13 완전 일치). 실프레임 캡처로 재확정만 남음.
4. 마스터 파일 URL 안정성(비공식 경로) — 포털 "종목 다운로드" 링크 주소와 대조, 변경 대비 설정화.
5. `FID_ORG_ADJ_PRC` 값 의미(0=수정주가) 문서 재확인.
6. OpenDART 일일 호출 한도 수치.
7. ~~`FHKST663400C0`의 활성 회원사 코드 원천·갱신 주기, 응답 1페이지 건수와 `tr_cont` 최대 페이지~~ — **종결**(2026-08-07 실계정 계측, §3.3·v0.6에 반영). 결과: (a) 회원사 원천은 마스터 파일 `memcode.mst.zip`(62행 = 61개사 + 집계 행 `99999` 외국계합; 5자리 코드+이름+외국계 플래그). (b) 요청 `FID_INPUT_ISCD`는 **5자리 코드의 뒤 3자리**(`00003`→`003`) — 5자리 그대로·자릿수 미달(`3`)은 에러 없이 `rt_cd=0` 0행. (c) **연속조회 미지원** — 두 달 범위 전체 조회(`999`)도 최신순 100행에서 잘리고 `tr_cont`·`ctx_area_*`가 비어 온다. `P=1`이며 조회 창을 직전 영업일~당일로 좁게 유지하면 실사용에서 100행에 닿지 않는다(한 회원사 한 달 최대 89행 관측). (d) 호출량 61콜/회차 × 66회 ≈ 4.0k콜/일, 4콜/s 페이싱으로 회차당 ~15초 — 10분 주기 지속 가능. (e) 응답 16필드 확인: `mbcr_name`(회원사명) 존재, `invt_opnn_cls_code`·`rgbf_invt_opnn_cls_code`는 현재=2·직전=3 고정인 위치 값, `hts_goal_prc=0`은 목표가 없음, 정렬은 `stck_bsop_date` 최신순.
8. **업종 분류의 세분도** — `idxcode.mst`가 주는 대분류는 KOSPI 11종·KOSDAQ 20여 종이라 "제조"에 대부분이 몰린다. worker-llm의 섹터 fan-out이 이 정도 해상도로 쓸 만한지 실데이터로 확인하고, 부족하면 중·소분류(마스터 파일의 [68:72]·[72:76]) 사용이나 서비스 자체 분류를 검토한다.
9. **분봉 API 계측(§2.6)** — `FHKST03010200`·`FHKST03010230`의 1콜 최대 건수(30건·과거분 추정치), 시각 필드가 봉 시작인지 종료인지, 분 거래량·거래대금 필드가 분값인지 누적값인지(누적이면 diff 계산)를 실응답으로 확정한다.
10. ~~**분봉 시장 구분 코드(§2.6)**~~ — **종결**(2026-08-06 실계정 계측, §2.6·v0.5에 반영). 결과: `UN`은 NXT 지원 종목에서 프리(08:00–08:50)·애프터(15:30–20:00) 봉을 **실제로 포함한다**(005930 08:30·16:00·19:00 조회 모두 해당 구간 봉 반환). (a) 미지원 시 KIS는 **에러도 빈 `output2`도 주지 않고 `rt_cd=0`에 전 행이 0인 30행을 준다** — 예상한 두 갈래가 모두 틀렸고, 그래서 `minute.candle.empty.complete`가 감지하지 못했다. (b) `UN`의 `acml_tr_pbmn`은 **통합 누적**이다(005930 2026-08-06 최종봉 9,654,526,846,500 ≈ `output1` 9,656,077,035,000, 같은 날 `J`는 6,091,288,502,000). (c) 같은 `stck_cntg_hour`에 KRX·NXT 행이 **따로 오지 않는다**(30행 시각 중복 0). 일봉·현재가의 `J` 유지로 인한 불일치는 §2.6 "가격 기준"(분봉은 원시가, 일봉과 어긋날 수 있음)이 이미 흡수하며, 통합 전환은 분봉에 한정한다.
11. **분봉 신선화 내부 API의 접근 통제(§2.6)** — core-api → worker-price 호출의 인증 방식(내부 네트워크 경계만으로 충분한지, 공유 시크릿 헤더가 필요한지)을 배포 토폴로지 확정 시 결정한다.
12. **`J` 경로의 장외 봉 처리(§2.6)** — NXT 미지원 종목을 `J`로 조회하면 장 마감 이후 구간에 시간외 체결분이 **조회 시각 부근의 단일 봉**으로 붙는다(047040을 19:00에 조회 → `1900` 봉 vol 90,550 / 19:37에 조회 → `1937` 봉). 봉 시각이 조회 시점에 따라 움직이므로 적재하면 재조회마다 유령 봉이 생긴다. 확정할 것: 이 봉이 시간외단일가 총합인지, 고정 시각(예: 18:00)으로 받을 방법이 있는지(`FID_ETC_CLS_CODE` 조합 포함), 과거 일자 TR `FHKST03010230`도 같은 형태인지. 그 전까지 `J` 종목의 페치 상한은 **15:30**이다.
13. **NXT 편입·제외의 장중 발효 여부(§2.6)** — 판정·전환이 "그날 적재분 0"일 때만 일어나므로, 상태 변화는 **다음 거래일 첫 조회**에 반영된다. ⑴ **편입**된 종목은 그날 하루 `J`로 조회되어 장외 구간이 누락된다(0봉이 아니라 정상 응답이라 감지 신호가 없다 — 가장 조용한 갈래다). ⑵ **제외**된 종목은 그날 첫 조회에서 `UN` 0봉을 만나 즉시 `J`로 전환·자가 복구하고, 이미 적재분이 있는 날은 0봉 가드만 작동해 그날은 정지한다(데이터 유지·오염 없음, `minute.candle.zero.page`). 두 경우 모두 **다음 거래일 첫 조회에서 자동 반영**된다 — 구분 기록이 날짜별이라 별도 무효화 경로가 필요 없다. 남은 확인 사항은 장중에 발효되는 편입·제외가 실제로 있는지다. 있다면 그날 하루는 위 규칙대로 이전 구분을 유지하며(적재분이 있으므로 전환하지 않는다) 지나가는데, 그 하루의 장외 구간 누락을 감수할지 아니면 발효 시각에 그날 기록을 무효화하고 재적재할지 정한다.
14. **비12월 결산 법인의 `financials_sync` 매핑(§3.1)** — 분기보고서의 `reprt_code`(11013 Q1/11014 Q3) 판별을 보고서명 괄호의 결산월(`month<=6`→Q1)로 하는데, 이 규칙은 12월·3월 결산에는 맞고 **6월·9월 결산 법인에서는 Q1/Q3이 뒤집힌다**. `bsns_year`도 회계연도가 역년을 걸치면 괄호 연도와 어긋날 수 있다. 오판 시 대부분 `fnlttSinglAcntAll`이 013(데이터 없음)을 돌려줘 적재 누락(`batch.financials.absent`)으로 관측되며, 잘못된 분기로 데이터를 대체 적재하지는 않는다(대체 조회 금지 — 구현 결정). 해당 법인은 극소수라 우선 수용하고, 정확한 매핑이 필요해지면 DART `company.json`의 결산월(`acc_mt`)로 분기 서수를 유도하고 비역년 회계연도의 `bsns_year` 규약을 실호출로 계측해 확정한다.
---

*KIS 수집 워커 명세 v0.7 — Redis 계약 v0.17·WS API v0.7·core-api 명세 v0.3과 정합. KIS 수치는 2026-07 공식 샘플 대조 기준이며 §9 항목은 실계정 재확인 대상.*
