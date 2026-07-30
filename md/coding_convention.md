# Alpha Talk — 코딩 컨벤션

이 저장소에서 코드를 쓰고 리뷰할 때 지키는 스타일 규약이다. 주석·Spring/Kotlin·영속성·운영 안전성·로깅·테스트의 방식을 정한다. 설계 근거(왜 이렇게 설계했는지)는 [ws_architecture.md](ws_architecture.md) 등 설계 문서가 소유하고 이 문서는 코드를 쓰는 방식만 정한다.

---

## 1. 주석을 달지 않는다 (self-documenting code)

**코드에 주석을 달지 않는다.** 의도는 이름·타입·구조로 드러낸다. 설계 근거와 불변식은 코드가 아니라 `md/` 설계 문서가 소유하고 코드에서는 테스트로 고정한다.

### 왜

- **주석은 코드와 함께 썩는다.** 코드가 바뀌어도 주석은 저절로 갱신되지 않는다. 시간이 지나면 주석이 거짓말을 한다. 컴파일러가 검증하지 못하는 유일한 텍스트다.
- 주석이 필요해 보이면 대개 **코드가 의도를 충분히 표현하지 못한다는 신호**다. 주석을 다는 대신 코드를 고친다.

### 주석 대신 이렇게 한다

| 주석을 달고 싶을 때 | 대신 |
|---|---|
| 변수/함수가 뭘 하는지 설명 | 이름을 그 설명으로 바꾼다 (`d` → `demandRegistry`) |
| 복잡한 식의 의미 설명 | 잘 명명된 지역변수로 추출한다 |
| 매직값의 뜻 설명 | 명명 상수로 뽑는다 (`41` → `MAX_SYMBOLS_PER_SESSION`) |
| 함수 안 "이 블록은 ~한다" | 그 블록을 잘 명명된 함수로 분리한다 |
| 왜 이렇게 짰는지(설계 근거) | **설계 문서(`md/`)에 쓴다.** 코드에는 남기지 않는다 (문서 우선 원칙) |

### 설계 의도·불변식·계약은 문서로

포트 계약(예: `WatchlistResolver`는 미존재 유저에 빈 Set 반환), refcount 전이 규칙, persist→publish→ack 같은 **불변식과 근거는 코드 주석이 아니라 설계 문서가 단일 소유**한다:

- 컴포넌트 책임·포트 계약·동시성 → [ws_architecture.md](ws_architecture.md)
- 서비스 간 채널·키 계약 → [redis_contract.md](redis_contract.md)
- 프로토콜 계약 → [ws_api_spec.md](ws_api_spec.md)

코드에서 이 계약을 참조해야 하면 주석을 다는 대신 **테스트로 고정**한다. 계약 테스트가 곧 실행 가능한 명세다 — 주석과 달리 계약이 깨지면 테스트도 깨진다.

### 예외 (극히 드묾)

- **KDoc**: 공개 라이브러리(`:contracts`, `:auth-jwt`)의 외부 공개 API에 한해 허용한다. 사용법이 시그니처만으로 명확하지 않을 때만 최소한으로 달고 내부 구현에는 달지 않는다.
- **외부 버그 우회** 등 코드로는 도저히 표현 못 하는 제약: 한 줄 + 근거 링크(이슈/문서). 이때도 "무엇을 하는지"가 아니라 "코드가 표현 못 하는 제약"만 적는다.
- `TODO`/`FIXME` 주석 금지 — 남길 일이면 이슈로 만든다.

---

## 2. 빈 등록은 컴포넌트 스캔이 기본

**우리가 소유한 클래스는 스테레오타입 어노테이션을 붙여 스캔으로 등록한다.** `@Configuration`에 `@Bean` 메서드를 만들어 손으로 조립하지 않는다.

| 대상 | 어노테이션 |
|---|---|
| 서비스(유스케이스·도메인 로직) | `@Service` |
| 저장소 어댑터(JPA·Redis 구현) | `@Repository` |
| 그 외 우리가 만든 협력자(핸들러·라이프사이클·인터셉터) | `@Component` |
| HTTP 진입점 | `@RestController` |

포트 인터페이스에는 아무것도 붙이지 않는다. 구현에만 붙이고, 주입은 인터페이스 타입으로 받는다 — 스캔을 쓴다고 DIP가 약해지지 않는다.

### 왜

- 손으로 조립하면 **클래스를 추가할 때마다 config를 같이 고쳐야 한다.** 의존성 하나를 추가하는 데 두 파일이 바뀌고, 빠뜨리면 기동 시점에야 드러난다.
- 조립 코드는 생성자 시그니처를 그대로 베낀 중복이다. 컴파일러가 이미 검증하는 내용을 사람이 한 번 더 적는 셈이다.
- 저장소 안에서 방식이 갈리면 **어디를 고쳐야 할지 매번 확인해야 한다.** ws는 처음부터 `@Component` 스캔이고 `config/`에는 프레임워크 설정만 둔다 — 모든 모듈이 이 형태를 따른다.

### `@Bean`을 쓰는 경우

조립에 **판단이 들어갈 때만** config로 간다. 단순 결선은 해당하지 않는다.

- 우리가 만들지 않은 타입: `SecurityFilterChain`·`PasswordEncoder`·`ObjectMapper`처럼 어노테이션을 붙일 수 없는 프레임워크·라이브러리 클래스
- 조건부 등록: 프로파일·프로퍼티에 따라 구현이 갈리거나(`@ConditionalOnProperty`), 미설정 시 기동을 막는 fail-closed 검증이 붙는 경우 (worker-llm `LlmConfig`)
- 같은 타입의 빈을 여러 개 만들어 이름으로 구분해야 하는 경우

생성자에 빈이 아닌 값(시각·난수원 등)이 필요하면 Kotlin 기본 파라미터로 두면 된다. Spring이 기본값을 그대로 쓰고, 테스트는 직접 생성해 원하는 값을 넣는다.

```kotlin
@Service
class AuthService(
    private val users: UserStore,
    private val clock: Clock = Clock.systemUTC(),
)
```

`@ConfigurationProperties`는 `@ConfigurationPropertiesScan`으로 등록한다 — 프로퍼티 클래스마다 `@Bean`을 만들지 않는다.

---

## 3. 로깅

- 로그는 구조화(JSON)를 지향하고 `sessionId`/`userId`를 태깅한다.
- 뜨거운 경로(틱 relay)에는 debug 이상 로그 금지.
- 토큰 원문·시크릿 로그 금지 (NFR-07).

---

## 4. 테스트

- refcount/인덱스 전이 로직을 바꿀 때는 반드시 단위 테스트를 함께 둔다 (`AGENTS.md` 불변 규칙).
- 포트는 페이크 구현으로 인프라(Redis/브로커) 없이 테스트한다.
- 계약(LSP)은 구현 공용 계약 테스트로 고정한다.

---

## 5. 운영 실패와 대규모 트래픽을 정상 조건으로 설계한다

정상 입력에서 한 번 성공하는 것만으로 구현 완료로 보지 않는다. **모든 외부 I/O와 상태 전이는 실행 도중 서버가 종료되고, 같은 작업이 다시 실행되며, 의존성이 느리거나 일부만 실패하고, 여러 인스턴스가 동시에 처리하는 상황을 기본 조건으로 검토한다.** 데이터량과 트래픽은 개발 환경이 아니라 운영 목표 규모를 기준으로 판단한다.

모든 항목에 같은 장치를 기계적으로 추가하라는 뜻은 아니다. 각 설계 문서가 정한 전달 보장과 일관성 수준을 먼저 따르고, 해당하지 않는 항목은 구현하지 않는다. 특히 `ws`의 best-effort 푸시에 재시도·영속 큐·전달 보장을 임의로 추가하지 않는다. 반대로 at-least-once 소비 경로는 중복 실행을 정상 동작으로 다룬다. 판단이 계약이나 동작을 바꾸면 코드를 쓰기 전에 해당 `md/` 설계 문서에 결정과 복구 기준을 반영한다.

### 중단·부분 실패·복구

- 외부 I/O, 영속화, publish, ack 전후를 실패 지점으로 나누고 각 지점에서 종료된 뒤 재기동·재실행하면 어떤 상태가 되는지 정한다.
- 성공으로 간주하는 커밋 지점을 하나로 명확히 한다. 완료되지 않은 작업은 안전하게 다시 실행되거나 명시적으로 폐기되어야 하며, 성공 응답·ack는 그 경로에 필요한 상태가 확정된 뒤에만 보낸다.
- 여러 시스템에 걸친 원자성을 가정하지 않는다. DB와 메시지 발행처럼 원자 커밋이 불가능한 경계는 설계 문서가 정한 순서·outbox·보상·재처리 중 하나로 일관성 모델을 명시한다.
- 프로세스 메모리는 재기동 시 사라진다고 가정한다. 복구에 필요한 상태는 계약이 허용한 영속 저장소에서 재구성하고, 의도적으로 휘발성인 상태는 빈 상태에서 정상 복구되는지 검증한다.
- graceful shutdown에서는 신규 작업 수락을 먼저 중단하고, 진행 중 작업을 제한 시간 안에 완료·반납·취소한다. 종료 중 새 작업 획득, 조기 ack, 연결 누수로 작업이 사라지지 않게 한다.

### 중복·순서·동시성

- 재시도나 at-least-once 전달이 가능한 명령·소비자는 멱등 키, DB unique constraint, 조건부 갱신 등으로 중복 실행 결과를 고정한다. 애플리케이션의 선조회 후 저장만으로 중복을 막지 않는다.
- 이벤트의 중복, 지연, 순서 역전 가능성을 검토한다. 순서가 중요하면 버전·시퀀스·조건부 갱신 규칙을 계약에 두고, 중요하지 않으면 순서에 의존하는 코드를 만들지 않는다.
- 공유 상태의 read-modify-write는 여러 스레드·코루틴·인스턴스가 동시에 실행해도 불변식이 유지되어야 한다. 필요한 범위에서 원자 연산, 낙관적/비관적 잠금, 격리 수준 또는 단일 소유자를 선택한다.
- 시간, 난수, 식별자 생성과 스케줄 실행은 테스트에서 제어 가능하게 주입한다. 서버 간 시계가 완전히 일치한다고 가정하지 않고, 시간대는 저장·계약에서 명시한다.

### 느린 의존성·재시도·과부하

- 모든 네트워크 호출과 대기에는 운영 근거가 있는 connect/read/전체 처리 timeout 또는 deadline을 둔다. 호출자의 취소와 종료 신호가 하위 작업에도 전파되어야 한다.
- 재시도는 일시 오류이면서 다시 실행해도 안전한 작업에만 적용한다. 횟수와 총 시간을 제한하고 exponential backoff와 jitter를 사용한다. 영구 오류, 검증 오류, 비멱등 작업, best-effort 푸시는 무조건 재시도하지 않는다.
- 동시 작업 수, 큐, 버퍼, 캐시, in-memory collection, 요청/메시지 크기, 배치 크기에 상한을 둔다. 생산 속도가 소비 속도를 넘을 때 차단·거절·샘플링·병합·폐기 중 어떤 백프레셔 정책을 쓸지 정한다.
- 장애 난 의존성을 무제한으로 호출하지 않는다. 필요하면 concurrency limit, rate limit, circuit breaker 또는 격리를 적용하되 임계값과 fallback이 계약에 맞는지 검증한다.
- 스레드·코루틴·DB/Redis/HTTP connection 같은 제한 자원은 명시적으로 소유하고 해제한다. 요청마다 무제한 비동기 작업이나 새 executor를 만들지 않는다.

### 데이터 규모·성능

- 운영 최대 규모에서 시간 복잡도와 메모리 사용량을 검토한다. 전체 테이블·전체 키·전체 결과를 한 번에 읽지 않고 pagination, cursor, chunk 또는 streaming을 사용한다.
- 목록 조회의 N+1, 반복문 안 원격 호출, 불필요한 직렬화·복사·객체 생성, hot path의 고카디널리티 로그와 metric label을 피한다.
- DB 쿼리는 필터·조인·정렬·pagination에 필요한 인덱스를 함께 검토한다. 대량 변경은 작은 트랜잭션과 제한된 배치로 처리하고, 장시간 transaction이나 lock으로 온라인 요청을 막지 않는다.
- 캐시를 정확성의 유일한 근거로 삼지 않는다. TTL, 최대 크기, eviction, 동시 miss, stale 허용 범위와 원본 장애 시 동작을 정한다.
- 성능 최적화는 목표 처리량·지연·데이터 규모를 먼저 정하고 측정으로 확인한다. 변경 전후 benchmark, query plan 또는 부하 테스트 결과 없이 복잡한 최적화를 도입하지 않는다.

### 배포 호환성·관측성·검증

- 롤링 배포 중 구버전과 신버전이 동시에 동작한다고 가정한다. API·이벤트·Redis 계약과 DB migration은 하위 호환을 유지하고, 스키마 변경은 expand → migrate/backfill → contract 순서로 수행한다.
- 로그만으로 운영 상태를 추측하게 두지 않는다. 처리량, 지연 분포, 오류율, timeout, retry, drop/DLQ, queue depth, active work와 DB/Redis/HTTP pool 포화도를 해당 경계의 metric으로 노출하고 경보 가능한 형태로 만든다.
- 로그와 trace에는 correlation 가능한 식별자를 넣되 토큰·시크릿·개인정보를 남기지 않는다. metric label과 로그 필드에 무제한 cardinality 값을 넣지 않는다.
- 테스트는 happy path뿐 아니라 커밋 직전·직후 중단, timeout, 부분 실패, 중복 전달, 순서 역전, 동시 실행, 재기동을 포함한다. 경쟁 조건은 반복 가능한 동시성 테스트로, 자원 상한과 목표 성능은 운영 규모에 가까운 부하 테스트로 검증한다.
- 새 로직의 리뷰와 완료 보고에는 적용되는 실패 모델, 전달 보장, 멱등성/복구 방식, timeout/retry, 자원 상한, 관측 지표, 검증 결과를 확인한다. 해당하지 않는 항목은 그 이유가 설계 계약에서 명확해야 한다.

---

## 6. Spring Boot 3.5 + Kotlin 현재 스타일

새 코드는 이 저장소의 Spring Boot 3.5.x와 Kotlin 2.x 기준으로 작성한다. 검색 결과나 예전 프로젝트의 코드를 그대로 옮기지 않고, 현재 버전에서 deprecated되지 않은 Spring API와 Kotlin 친화적인 방식을 사용한다. 프레임워크 버전을 우회하는 자체 구현보다 Spring Boot 자동 구성과 공식 starter가 제공하는 기능을 먼저 사용한다.

### 의존성 주입과 설정

- 의존성은 주 생성자로 주입하고 `private val`로 보관한다. 필드 주입, setter 주입, `lateinit var`, 불필요한 `@Autowired`, 런타임에 컨텍스트에서 빈을 찾는 service locator 방식을 사용하지 않는다.
- 프로퍼티는 모듈별 `@ConfigurationProperties` 타입으로 묶고 `@Validated`와 Bean Validation으로 기동 시 검증한다. 여러 클래스에 `@Value` 문자열을 흩뿌리거나 `Environment`에서 임의로 꺼내지 않는다.
- Boot 자동 구성과 컴포넌트 스캔을 우선한다. 라이브러리 타입 생성, 조건부 등록, fail-closed 검증처럼 조립에 판단이 있을 때만 `@Configuration`과 `@Bean`을 사용한다.
- 프레임워크·라이브러리 버전은 version catalog와 Spring dependency management가 소유한다. 모듈 build 파일에 같은 버전을 다시 하드코딩하거나 Boot가 관리하는 의존성 버전을 임의로 덮어쓰지 않는다.

### HTTP와 애플리케이션 경계

- 컨트롤러는 요청 검증·인증 컨텍스트 변환·유스케이스 호출·응답 변환만 담당한다. 트랜잭션, 영속성 쿼리, 외부 API 호출과 핵심 분기를 컨트롤러에 넣지 않는다.
- 요청·응답에는 전용 DTO를 사용하고 JPA entity를 직렬화하거나 API 계약으로 노출하지 않는다. Kotlin `data class`는 값 DTO에 사용하고 변경 가능한 entity에는 사용하지 않는다.
- 요청 검증은 `jakarta.validation`과 `@Valid`를 사용한다. 오류 응답은 중앙 `@RestControllerAdvice` 하나가 소유하고, 임의의 `Map<String, Any>` 응답이나 컨트롤러별 예외 포맷을 만들지 않는다. 에러 봉투 타입은 해당 모듈의 REST 계약 문서가 정하고, 계약이 정하지 않은 새 진입점은 `ProblemDetail`을 기본으로 한다.
  - core-api는 [alphatalk_core_api_spec.md](alphatalk_core_api_spec.md)가 정한 `{ "error": { "code", "message", "detail" } }` 봉투를 쓴다. 클라 계약이므로 이 봉투를 바꾸려면 코드가 아니라 스펙 개정이 먼저다 — `ProblemDetail` 전환 여부는 계약 개정 안건으로 남겨 두고, 그 전까지 core-api 신규 엔드포인트도 기존 봉투를 그대로 쓴다.
- Spring MVC의 동기 HTTP 클라이언트가 필요하면 `RestClient`를 우선하고 새 코드에 `RestTemplate`을 도입하지 않는다. 비동기·스트리밍 요구가 설계에 있을 때만 `WebClient`를 사용하며, 이를 이유로 서버 전체를 WebFlux 방식으로 섞지 않는다. 이 항목은 Spring 위에서 도는 서버 모듈 기준이며, `contracts`·`auth-jwt`·`kis-client`처럼 Spring 무의존이 설계인 라이브러리 모듈은 대상이 아니다 — 이 규칙을 근거로 spring-web 의존을 새로 추가하지 않는다.
- API·프레임워크의 deprecated 경고를 방치하지 않는다. 교체 API를 확인해 새 방식으로 구현하고, 불가피한 호환성 예외는 설계 문서와 테스트로 범위를 고정한다.

### 트랜잭션과 JPA 모델

- `@Transactional`은 여러 저장소 작업을 하나의 유스케이스로 묶는 public 서비스 경계에 둔다. 조회 전용 경로는 `@Transactional(readOnly = true)`를 사용하고, 컨트롤러·private 메서드·자기 호출에 붙여 프록시가 적용될 것이라고 기대하지 않는다.
- DB transaction 안에서 HTTP, LLM, 파일, Redis 같은 느린 외부 I/O를 호출하지 않는다. DB 상태와 외부 발행의 순서가 중요하면 §5의 일관성 모델을 먼저 설계한다.
- JPA entity는 영속성 모델로만 사용한다. 안정적인 식별자 기준의 `equals`/`hashCode`, 연관관계 소유권과 변경 메서드를 명확히 하고, 무분별한 양방향 연관관계·public mutable collection·entity의 `data class` 사용을 피한다.
- 연관관계는 필요한 시점에 명시적으로 조회하고 기본적으로 지연 로딩한다. OSIV에 기대어 컨트롤러 직렬화 중 쿼리가 발생하게 하지 않으며, N+1은 projection, fetch join 또는 `@EntityGraph`로 해결한다.
- 대량 목록은 `Page`, `Slice` 또는 cursor 기반으로 제한하고, entity 전체 로딩 뒤 애플리케이션 메모리에서 필터링·집계하지 않는다.

---

## 7. 관계형 DB 접근

DB를 사용하는 모든 서버 모듈은 Spring Data JPA를 기본이자 우선 구현으로 사용한다. 쿼리 구현 우선순위는 다음과 같다.

1. `JpaRepository` 기본 CRUD와 파생 쿼리 — 조회 컬럼을 좁혀야 하면 projection을, 연관을 함께 로딩해야 하면 `@EntityGraph`를 붙인다
2. `@Query`의 JPQL — 파생 쿼리로 의도가 불명확하거나 조인·DTO projection·fetch join·벌크 갱신이 필요할 때

엔티티 중심 CRUD와 일반 조회는 먼저 Spring Data JPA로 표현한다.

**native query, `JdbcTemplate`, `JdbcClient`, 직접 JDBC와 문자열 SQL은 일반적인 선택지에 포함하지 않는다. 에이전트는 이를 자체 판단으로 도입하거나 JPA 구현을 raw SQL로 교체하지 않는다.** 불가피하다고 판단하면 코드를 작성하기 전에 다음 내용을 사용자에게 제시하고 명시적 합의를 받는다.

- JPA 파생 쿼리, projection, `@EntityGraph`, fetch join, JPQL로 해결할 수 없는 구체적인 이유
- 필요한 PostgreSQL 전용 기능이나 실행 계획·부하 측정으로 확인된 병목
- transaction, lock, pagination, migration과 테스트에 미치는 영향
- 제안하는 SQL의 입력 바인딩 방식과 회귀·통합 테스트 계획

합의한 예외는 관련 설계 문서에 사용 범위와 이유를 먼저 기록한다. 구현에서는 모든 입력을 파라미터로 바인딩하고 쿼리 동작을 통합 테스트로 고정한다. 합의 범위를 넘어 다른 조회에 raw SQL 방식을 확산하지 않는다.

이 규칙은 신규·변경 코드 기준이다. 규칙 도입 이전부터 있던 raw SQL 어댑터는 아래와 같고, 예외 근거가 기록된 것과 아직 기록되지 않은 것을 구분해 둔다.

| 모듈 | 어댑터 | 상태 |
|---|---|---|
| worker-llm | `JdbcClusterStore`, `JdbcStreamEventStore` | 예외 근거 기록됨 — [alphatalk_news_worker_spec.md](alphatalk_news_worker_spec.md) §7.1 |
| core-api | `JdbcStockSearchStore` | 미기록 — JPA 전환 또는 예외 근거 기록 대상 |
| worker-price | `JdbcDailyCandleStore` | 미기록 — JPA 전환 또는 예외 근거 기록 대상 |
| worker-batch | `JdbcSectorStore`, `JdbcStockMasterStore`, `JdbcBatchJobRunStore` | 미기록 — JPA 전환 또는 예외 근거 기록 대상 |

미기록 어댑터를 건드리는 변경은 JPA 전환과 해당 모듈 설계 문서의 예외 기록 중 하나를 함께 수행한다.

DB 스키마의 단일 소유자는 `:db-migrations`의 Liquibase다. JPA는 스키마를 생성하거나 갱신하지 않고 `ddl-auto=validate`로 엔티티 매핑과 실제 스키마의 정합성만 검증한다.
