<div align="center">

# Alpha Talk

**One ticker, one room.**
Backend for a stock community that folds news, disclosures, AI summaries, live quotes, and user posts into a single per-ticker timeline.

[![Kotlin](https://img.shields.io/badge/Kotlin-2.2-7F52FF?logo=kotlin&logoColor=white)](https://kotlinlang.org)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![JDK](https://img.shields.io/badge/JDK-21-437291?logo=openjdk&logoColor=white)](https://openjdk.org/projects/jdk/21/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%20%2B%20pgvector-4169E1?logo=postgresql&logoColor=white)](https://github.com/pgvector/pgvector)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)](https://redis.io)
[![tests](https://img.shields.io/badge/tests-561%20passing-brightgreen)](#development)

[한국어](README.md) · English

</div>

---

## The problem

Following a single stock means checking five places: news on a portal, disclosures on DART, quotes and charts in a trading terminal, and chatter in a message board. The board is noisy, and there is more news than anyone can read.

Alpha Talk normalizes those fragments into a single unit — a `StreamEvent` — and renders them as one flow, keyed by ticker.

- **Unified stream** — enter a room and news, disclosures, analyst reports, AI summaries, and user posts arrive as one chronological feed
- **Article clustering** — stories covering the same event are grouped by embedding similarity, so a cluster surfaces once instead of ten times
- **Three-line AI summaries with sentiment** — sentiment is scored per (event, ticker) pair, because the same headline can be good news for one company and bad news for its competitor
- **Live push** — quote ticks and new events for watchlisted tickers arrive over WebSocket
- **Daily briefing** — every day at 18:00 KST, the day's positives and negatives are rolled up per ticker

## How it works

A **modular monolith plus workers**: only the pieces with genuinely different runtime characteristics are split out. This is not microservices.

```
① Real-time plane
   KIS WebSocket ─► worker-price ─► Redis Pub/Sub (quote:{code}) ─► ws ─► browser (STOMP)

② Async processing plane
   RSS · news search ─► worker-ingest ─► queue:ingest (Redis Streams)
                                          └► worker-llm ─► ① persist ② PUBLISH stream:{code} ③ XACK

③ Synchronous API plane
   browser ─(REST)─► core-api ─► PostgreSQL / Redis
                        └─ on write: ① persist ② PUBLISH post:{code}

④ Batch plane
   KIS REST · master files · OpenDART ─► worker-batch ─► PostgreSQL
```

Three rules hold this shape together.

1. **Servers never depend on each other's code.** They talk through Redis channels, queues, and database contracts. The build graph enforces the boundary.
2. **The database is the source of truth; push is best-effort.** The order is always persist → publish → ack. A missed event is recovered by a REST fetch, not by redelivery.
3. **The gateway stays thin.** It rejects client SEND frames and never touches the database. Its entire job is subscribing to Redis and fanning out.

## Quick start

You need Docker and Git. Gradle's toolchain fetches JDK 21 on its own.

```bash
git clone https://github.com/alpha-talk/backend.git
cd backend

docker compose up -d      # Redis 7 · PostgreSQL 16 (pgvector)
./gradlew build           # all modules, 561 tests
```

To bring up just the gateway:

```bash
./gradlew :ws:bootRun --args='--spring.profiles.active=local'
```

STOMP accepts connections at `ws://localhost:8081/ws`. The JWT goes in the **CONNECT frame header** — query parameters are rejected because they end up in proxy logs.

To watch an RSS article travel through summarization and land as a card in a browser, follow [test-front/README.md](test-front/README.md).

## Repository layout

One repository holds several servers as Gradle subprojects. Not every folder is a server — standalone apps and shared libraries are kept distinct.

| Module | Kind | Role | Port |
|---|---|---|---|
| `contracts` | library | Channel names, keys, STOMP destinations, envelope DTOs — sole owner of strings shared across services | — |
| `auth-jwt` | library | JWT issuing and verification. Add the dependency and a secret property, and the beans register themselves | — |
| `kis-client` | library | Korea Investment & Securities OpenAPI — token lifecycle, rate limiting, REST/WS clients | — |
| `db-migrations` | library | Liquibase changelogs. Sole owner of the database schema | — |
| `core-api` | server | Main REST server — auth, search, watchlist, stream, notifications, community, stock info | 8080 |
| `ws` | server | STOMP gateway — push-only edge | 8081 |
| `worker-price` | server | KIS live quotes: collect, conflate, publish; daily candles | 8082 |
| `worker-batch` | server | Stock master, investor flow, and financial statement batches | 8083 |
| `worker-ingest` | server | News collection, normalization, queue enqueue | 8084 |
| `worker-llm` | server | Consume queue → cluster → summarize → persist and publish | 8085 |

## Stack

| Area | Choice |
|---|---|
| Language · runtime | Kotlin 2.2 / JVM 21 |
| Framework | Spring Boot 3.5 — MVC with `@EnableWebSocketMessageBroker` (SimpleBroker) |
| Data | PostgreSQL 16 + pgvector (article clustering), Redis 7 (Pub/Sub · Streams · cache) |
| Migrations | Liquibase — `db-migrations` holds every changelog |
| LLM · embeddings | Anthropic API or Claude/Codex CLI · OpenAI-compatible embedding endpoint (Ollama + BGE-M3 locally) |
| Build | Gradle Kotlin DSL, multi-module, version catalog |
| Testing | JUnit 5 · Testcontainers (PostgreSQL, Redis) · `WebSocketStompClient` |

## Documentation

Design decisions live in `md/`, not in the code. When code and documents disagree, one of them gets fixed — they are never left out of sync. The documents are written in Korean.

| Document | Covers |
|---|---|
| [기획안](md/기획안.md) | Requirements, overall architecture, roadmap, legal review |
| [ws_architecture](md/ws_architecture.md) | Gateway design at code level — component responsibilities, concurrency, error policy |
| [ws_api_spec](md/ws_api_spec.md) | Client ↔ gateway STOMP protocol contract |
| [redis_contract](md/redis_contract.md) | Gateway ↔ worker ↔ main server Redis contract — single source of truth across services |
| [alphatalk_core_api_spec](md/alphatalk_core_api_spec.md) | Client ↔ main server REST contract |
| [alphatalk_kis_worker_spec](md/alphatalk_kis_worker_spec.md) | KIS and OpenDART collection workers |
| [alphatalk_news_worker_spec](md/alphatalk_news_worker_spec.md) | News pipeline — collection, clustering, summarization, daily briefing |
| [local_embedding_setup](md/local_embedding_setup.md) | Running free local embeddings (Ollama + BGE-M3) |
| [coding_convention](md/coding_convention.md) · [git_convention](md/git_convention.md) | Coding, branching, and commit rules |

## Development

```bash
./gradlew build              # all modules
./gradlew :ws:test           # gateway only
./gradlew :worker-llm:test   # news summarization worker only
```

Integration tests start their own Redis and PostgreSQL through Testcontainers, so there is nothing to set up. All 561 currently pass.

A few things worth knowing:

- **Secrets have no defaults.** Without a JWT secret, KIS credentials, or an LLM key, the affected server refuses to start. Local development uses the `local` profile.
- **Code carries no comments.** Rationale and invariants belong to the documents in `md/`; the code pins them down with tests.
- **Channel names and keys are never written as literals.** Use the constants in `:contracts`.
- Branches are `type/scope/desc`, commits are `type(scope): subject`, and scope is a module name. Nothing lands on main directly. See [git_convention](md/git_convention.md).

## Status

| Area | State |
|---|---|
| Gateway (`ws`) | JWT auth, demand index, Redis relay, watchlist resolution, presence, metrics — working |
| News pipeline (`worker-ingest` · `worker-llm`) | Collection through clustering, summarization, sentiment, fan-out, daily briefing, DLQ — working |
| KIS real-time (`kis-client` · `worker-price`) | Session pool, conflation, publishing, REST polling fallback, daily candles — working |
| Batch (`worker-batch`) | Stock master sync done. Investor flow, financials, and analyst opinions are specified but not built |
| Main server (`core-api`) | Auth, search, watchlist, stream, notifications, community, stock info — 30 REST endpoints working. Deployment hardening (CORS, metrics) is left |

Next up: the remaining batch jobs (investor flow, financials, analyst opinions), deployment hardening for `core-api`, and a load smoke test.

## Disclaimer

This is a personal and team portfolio project. Everything it produces, including AI-generated output, is for reference only and is not investment advice.

Redistributing live Korean market data to the public requires a KOSCOM market data agreement, and AI-processed derivatives are no exception. The design allows switching to a 20-minute delayed mode before any public deployment. The full review is in [기획안 §6](md/기획안.md).
