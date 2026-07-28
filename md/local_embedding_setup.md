# worker-llm 로컬 임베딩 설정

이 문서는 개발 환경에서 외부 임베딩 API 비용 없이 `worker-llm`의 뉴스 클러스터링을 검증하는 실행 가이드다. 전제(§3) → 설치(§4) → 검증(§5) → 실행(§6~§7) → 문제 해결(§12) 순서로 따라 하면 된다. 파이프라인 설계와 스키마는 [뉴스 파이프라인 명세](alphatalk_news_worker_spec.md)가 소유하고, 이 문서는 설치·환경변수·Docker 연결·문제 해결만 다룬다.

## 1. 권장 구성

로컬에서는 요약·판정을 CLI 구독이 맡고 임베딩은 로컬 Ollama가 맡는다. 로컬 프로파일의 기본 구성은 다음과 같다.

| 역할 | 구현 |
|---|---|
| 뉴스 요약·관련성 판정 | 로그인된 Claude CLI 구독 |
| 임베딩 | 같은 PC나 내부 서버에서 실행하는 Ollama |
| 임베딩 모델 | `bge-m3` |
| 벡터 저장 | PostgreSQL pgvector `vector(1024)` |

역할을 나눈 이유는 Claude CLI와 Codex CLI가 임베딩 API를 제공하지 않기 때문이다. 임베딩은 별도 로컬 서버가 담당한다. `LLM_PROVIDER=codex-cli`로 요약·판정을 Codex CLI 구독으로 전환할 수 있다.

임베딩 호출은 `EmbeddingClient` 포트의 `rest` 구현이 같은 호스트 또는 내부망의 Ollama로 보낸다. `worker-llm` 안에서 모델을 직접 로딩하는 방식은 현재 구현하지 않는다. Ollama를 같은 서버에서 실행하면 기사 텍스트가 외부 임베딩 API로 나가지 않는다.

## 2. BGE-M3를 사용하는 이유

현재 스키마·라이선스·배포 조건에 두루 맞는 로컬 모델이 BGE-M3다.

- 한국어를 포함한 다국어 지원
- 출력 차원 1024로 현재 `news_article.embedding vector(1024)` 스키마와 일치
- MIT 라이선스
- Ollama 배포 모델 약 1.2GB
- OpenAI 호환 `/v1/embeddings` 지원

참고:

- [Ollama BGE-M3](https://ollama.com/library/bge-m3)
- [BGE-M3 모델 카드](https://huggingface.co/BAAI/bge-m3)
- [Ollama 임베딩 문서](https://docs.ollama.com/capabilities/embeddings)
- [Ollama OpenAI 호환 API](https://docs.ollama.com/api/openai-compatibility)

## 3. 사전 조건

worker-llm을 기동하기 전에 다음이 준비되어 있어야 한다.

- JDK 21
- Redis와 PostgreSQL 실행
- `db-migrations` changelog가 뉴스 파이프라인 스키마와 pgvector 확장을 적용한 DB (worker-llm 기동 시 자동 적용)
- Claude CLI 또는 Codex CLI 로그인
- [Ollama 설치](https://ollama.com/download)

로컬 DB의 `news_article.embedding` 컬럼은 1024차원이다. 다른 차원의 모델을 쓰려면 뉴스 워커 명세를 먼저 변경하고 `db-migrations` changeset과 worker-llm 테스트 스키마를 함께 변경해야 한다.

## 4. Ollama 준비

worker-llm은 임베딩을 직접 계산하지 않고 Ollama HTTP API를 호출하므로 모델과 서버를 먼저 준비한다.

모델을 내려받는다.

```bash
ollama pull bge-m3
```

Ollama 애플리케이션이 백그라운드에서 실행 중이지 않다면 서버를 시작한다.

```bash
ollama serve
```

기본 주소는 `http://localhost:11434`다. 이미 Ollama 애플리케이션이 서버를 실행하고 있다면 `ollama serve`를 다시 실행하지 않는다.

설치된 모델을 확인한다.

```bash
ollama list
```

## 5. 임베딩 API 단독 검증

worker-llm을 띄우기 전에 임베딩 서버만 먼저 검증한다. 여기서 통과해 두면 이후 오류가 났을 때 원인이 임베딩 서버인지 애플리케이션 설정인지 바로 가릴 수 있다.

현재 `RestEmbeddingClient`가 사용하는 OpenAI 호환 엔드포인트를 직접 호출한다.

```bash
curl http://localhost:11434/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "bge-m3",
    "input": "삼성전자가 반도체 설비 투자를 확대한다",
    "dimensions": 1024
  }'
```

정상 응답 조건:

- HTTP 200
- `data[0].embedding`이 존재
- 임베딩 배열 길이가 1024

Ollama의 네이티브 엔드포인트는 `/api/embed`지만 Alpha Talk은 제공자 교체가 가능한 OpenAI 호환 `/v1/embeddings`를 사용한다.

## 6. Claude CLI와 함께 worker-llm 실행

Redis·PostgreSQL·Ollama가 모두 준비됐으면 로컬 프로파일로 worker-llm을 기동한다.

```bash
SPRING_PROFILES_ACTIVE=local ./gradlew :worker-llm:bootRun
```

`application-local.yml`의 기본값은 다음과 같다.

- LLM provider: `claude-cli`
- 임베딩 provider: `rest`
- 임베딩 서버: `http://localhost:11434`
- 임베딩 모델: `bge-m3`
- 임베딩 차원: 1024

Claude CLI가 기본 실행 경로에 없다면 `CLAUDE_CLI_EXECUTABLE`에 실행 파일의 절대 경로를 지정한다.

```bash
SPRING_PROFILES_ACTIVE=local \
CLAUDE_CLI_EXECUTABLE=/absolute/path/to/claude \
./gradlew :worker-llm:bootRun
```

`EMBEDDING_API_KEY=ollama`는 실제 비밀키가 아니다. 애플리케이션이 `provider=rest`에서 빈 키를 허용하지 않아 넣는 더미 값이다. 로컬 Ollama는 이 값을 인증에 쓰지 않는다.

## 7. Codex CLI와 함께 worker-llm 실행

LLM provider만 `codex-cli`로 바꾼다. 나머지 절차는 §6과 같다.

```bash
SPRING_PROFILES_ACTIVE=local \
LLM_PROVIDER=codex-cli \
./gradlew :worker-llm:bootRun
```

Codex CLI가 기본 실행 경로에 없다면 `CODEX_CLI_EXECUTABLE`에 실행 파일의 절대 경로를 지정한다.

## 8. 설정값

로컬 실행에 쓰는 환경변수는 다음과 같다.

| 환경변수 | 로컬 기본값 | 설명 |
|---|---|---|
| `SPRING_PROFILES_ACTIVE` | 없음 | `local`로 지정해야 로컬 설정 활성화 |
| `LLM_PROVIDER` | `claude-cli` | `codex-cli`로 전환 가능 |
| `EMBEDDING_PROVIDER` | `rest` | `fake`로 전환 가능 |
| `EMBEDDING_BASE_URL` | `http://localhost:11434` | `/v1`을 붙이지 않은 서버 루트 |
| `EMBEDDING_API_KEY` | `ollama` | 로컬용 비밀이 아닌 더미 값 |
| `EMBEDDING_MODEL` | `bge-m3` | Ollama에 내려받은 모델명 |
| `EMBEDDING_DIMENSION` | `1024` | DB 벡터 차원과 일치 |

`RestEmbeddingClient`가 `EMBEDDING_BASE_URL` 뒤에 `/v1/embeddings`를 붙인다. `EMBEDDING_BASE_URL=http://localhost:11434/v1`로 설정하면 잘못된 `/v1/v1/embeddings` 주소를 호출한다.

로컬 프로파일은 fake 임베딩을 허용한다. `EMBEDDING_PROVIDER`가 정확히 `rest`가 아니면 알 수 없는 provider 값도 fake 임베딩으로 처리될 수 있다. 기동할 때마다 시작 로그에서 실제 클라이언트를 확인한다.

## 9. Docker에서 worker-llm을 실행하는 경우

worker-llm의 실행 위치에 따라 Ollama 주소가 달라진다. 컨테이너 안의 `localhost`는 호스트가 아니라 컨테이너 자신을 가리키기 때문이다.

Ollama는 호스트 macOS에서 실행하고 worker-llm만 컨테이너에서 실행한다면 다음 주소를 사용한다.

```bash
EMBEDDING_BASE_URL=http://host.docker.internal:11434
```

Ollama와 worker-llm을 같은 Docker 네트워크에서 각각 실행한다면 Ollama 서비스 이름을 사용한다.

```bash
EMBEDDING_BASE_URL=http://ollama:11434
```

호스트에서 `./gradlew :worker-llm:bootRun`으로 실행하는 경우에는 `http://localhost:11434`를 유지한다.

## 10. Fake 임베딩으로 전환

Ollama 없이 애플리케이션 연결만 확인하려면 fake 임베딩을 명시한다.

```bash
SPRING_PROFILES_ACTIVE=local \
EMBEDDING_PROVIDER=fake \
./gradlew :worker-llm:bootRun
```

Fake 임베딩은 문자열 토큰 해시를 1024차원 벡터로 만드는 테스트 구현이다. 비용과 모델 설치가 필요 없지만 문장 의미를 제대로 비교하지 못한다. 그래서 다음 용도로만 쓴다.

- 애플리케이션 기동 확인
- Redis·DB·LLM 파이프라인 연결 확인
- 클러스터링 외 기능의 단위·통합 테스트

실제 뉴스가 같은 사건으로 묶이는지 확인하려면 BGE-M3를 사용한다.

## 11. 기존 임베딩 데이터 주의

서로 다른 임베딩 모델이 만든 벡터를 같은 클러스터 검색 공간에 섞으면 코사인 유사도에 의미가 없다. 다음 규칙을 지킨다.

- fake 임베딩으로 만든 테스트 데이터와 BGE-M3 데이터를 혼합하지 않는다.
- 모델을 변경할 때는 별도 테스트 DB를 사용하거나 기존 뉴스 벡터의 재임베딩 계획을 세운다.
- 운영 중 모델을 변경할 때는 모델명·차원·유사도 임계값을 함께 버전 관리한다.
- 현재 유사도 임계값 `0.85`는 초기값이므로 BGE-M3 실제 뉴스 데이터로 오합류·미합류 사례를 확인한 뒤 조정한다.

## 12. 문제 해결

증상별 확인 순서를 정리한다.

### Connection refused

```text
Connection refused: localhost:11434
```

- Ollama 애플리케이션 또는 `ollama serve` 실행 여부 확인
- worker-llm이 컨테이너라면 `host.docker.internal` 사용
- `curl http://localhost:11434/v1/models`로 연결 확인

### Model not found

```bash
ollama pull bge-m3
ollama list
```

`EMBEDDING_MODEL` 값과 `ollama list`의 모델명이 일치해야 한다.

### unexpected embedding shape

애플리케이션이 받은 벡터 길이가 설정한 `dimension`과 다르다는 오류다.

- `EMBEDDING_MODEL=bge-m3` 확인
- `EMBEDDING_DIMENSION=1024` 확인
- DB 컬럼이 `vector(1024)`인지 확인

### fake 임베딩이 활성화됨

- `EMBEDDING_PROVIDER=rest` 철자 확인
- 시작 로그에서 `using fake embedding client` 확인
- `EMBEDDING_BASE_URL`, `EMBEDDING_API_KEY`, `EMBEDDING_MODEL`이 모두 비어 있지 않은지 확인

### 런타임 임베딩 호출 실패

런타임 호출이 실패하면 worker는 해당 기사를 임베딩 없이 신규 클러스터로 처리하고 경고 로그를 남긴다. 파이프라인은 계속 진행되지만 동일 사건이 여러 클러스터로 쪼개질 수 있다. 반복되는 실패를 정상 상태로 방치하지 않는다.
