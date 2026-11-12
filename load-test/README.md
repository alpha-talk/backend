# Alpha Talk 부하 스모크

부하 테스트는 한 번에 한 병목만 확인한다. WS 스모크는 단일 게이트웨이의 `DemandRegistry` 락과 quote fan-out을, HTTP 스모크는 core-api 스트림 조회를 측정한다. 같은 머신에서 서버와 생성기를 함께 실행한 결과는 회귀 검사용이며 운영 수용량으로 해석하지 않는다.

## WS 단일 인스턴스

Docker가 실행 중이면 Testcontainers가 Redis와 포트가 임의 배정된 WS 인스턴스를 기동한다. 기본 테스트와 분리된 opt-in 작업이다.

```bash
./gradlew :ws:loadTest
```

기본 프로필은 500세션, 41종목, 동시 SUBSCRIBE/UNSUBSCRIBE 5회, 종목당 초당 4틱을 30초간 발행한다. 축소 배선 검증이나 더 긴 측정은 Gradle 프로퍼티로 조절한다.

```bash
./gradlew :ws:loadTest \
  -PwsLoadClients=100 \
  -PwsLoadRounds=2 \
  -PwsLoadFanoutSeconds=10
```

10,000세션을 한 게이트웨에 연결할 때는 부하 생성기 힙을 함께 늘린다.

```bash
./gradlew :ws:loadTest \
  -PwsLoadClients=10000 \
  -PwsLoadHeap=4g \
  -PwsLoadConnectBatch=25 \
  -PwsLoadConnectPauseMillis=50 \
  -PwsLoadConnectTimeoutSeconds=30 \
  -PwsLoadRounds=5 \
  -PwsLoadFanoutSeconds=30
```

이 프로필은 초당 약 500세션으로 연결을 늘린 뒤 10,000 고정 동시성을 측정한다. 연결 폭주(spike) 수용량 테스트가 아니다.
서버는 Tomcat 기본 연결 상한 8,192를 넘기기 위해 `WS_TOMCAT_MAX_CONNECTIONS`를 기본 12,000으로 두며, 배포 용량에 맞게 조정할 수 있다.

합격 기준은 락 대기 p99 100ms 미만, 락 대기·보유 100ms 초과 0건, quote 수신율 99% 이상, quote 종단 지연 p95 1초 이하, 종료 시 outbound 큐 0이다. 생성기와 서버가 한 JVM의 CPU를 공유하므로 CPU 포화나 transport error가 발생하면 서버 한계로 단정하지 않고 생성기를 별도 머신으로 옮겨 재검증한다.

같은 작업은 네트워크 부하 외에 `DemandRegistryLockContentionTest`를 함께 실행한다. 10,000개 가상 스레드를 한 시작선에서 동시에 풀어 `subscribeRoom`과 `unsubscribeById`가 같은 `ReentrantLock`을 직접 경합하게 한다. 이 테스트는 STOMP·Redis·네트워크 비용을 제외한 락 스파이크 최악 조건이다.

Prometheus 이름은 다음과 같다.

- `ws_demand_lock_wait_seconds`
- `ws_demand_lock_hold_seconds`
- `ws_demand_lock_over_threshold_total{phase,operation}`
- `ws_demand_lock_queued`
- `ws_client_outbound_queue_depth`
- `ws_presence_failures_total{operation}`

## core-api 스트림 조회

테스트 전에 대상 종목과 `stream_event`를 적재하고 만료되지 않은 액세스 토큰을 발급한다. 가입·로그인·시딩은 측정 구간에 포함하지 않는다.

```bash
ACCESS_TOKEN='...' \
CODES='005930,000660' \
RPS=50 \
DURATION=5m \
k6 run load-test/core-api-stream.js
```

스크립트는 고정 도착률로 요청하며 오류율 1% 미만, 검사 성공률 99% 초과, 응답 p95 300ms 미만을 요구한다. 50→100→200→400 RPS 순서로 별도 실행하고 각 실행 사이에 JVM·DB pool·Redis·CPU가 정상 범위로 돌아왔는지 확인한다.

Postman 성능 러너는 HTTP 컬렉션의 동시 사용자 테스트에는 사용할 수 있지만 WebSocket/STOMP 단계를 재현하지 않는다. 따라서 WS S7 검증은 프로젝트의 `WebSocketStompClient` 도구가 단일 진실이다.
