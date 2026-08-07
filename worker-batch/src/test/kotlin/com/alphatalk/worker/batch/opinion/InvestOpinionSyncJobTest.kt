package com.alphatalk.worker.batch.opinion

import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.kis.rest.KisInvestOpinion
import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.core.SimpleLock
import java.time.Duration
import java.time.Instant
import java.util.Optional
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InvestOpinionSyncJobTest {
    private val meters = SimpleMeterRegistry()
    private val brokers = listOf(Broker("00005", "미래에셋"), Broker("00003", "한국증권"))
    private val runs = FakeRuns()
    private val store = FakeStore()
    private val binder = FakeBinder(store)
    private val publisher = FakePublisher()
    private val weekday: Instant = Instant.parse("2026-08-07T01:00:00Z")

    private fun row(code: String = "005930", rating: String = "매수") = KisInvestOpinion(
        code = code,
        businessDate = "20260807",
        rating = rating,
        previousRating = "중립",
        targetPrice = 95000,
        memberName = "미래에셋",
    )

    private fun job(
        fetcher: OpinionFetcher,
        directory: BrokerDirectory = BrokerDirectory { brokers },
        locks: LockProvider = grantingLocks(),
        now: Instant = weekday,
        holidays: Set<java.time.LocalDate> = emptySet(),
    ) = InvestOpinionSyncJob(
        brokers = directory,
        fetcher = fetcher,
        store = store,
        binder = binder,
        publisher = publisher,
        runs = runs,
        locks = locks,
        meters = meters,
        holidays = holidays,
        requestInterval = Duration.ZERO,
        clock = { now },
        pause = {},
    )

    private fun grantingLocks() = LockProvider { Optional.of(NoopLock()) }

    @Test
    fun `수집한 의견을 저장하고 커밋된 eventId로 발행한 뒤 마킹한다`() {
        val published = job(fetcher = { broker, _, _ -> if (broker.code == "00005") listOf(row()) else emptyList() }).syncOnce()

        assertEquals(1, published)
        assertEquals(1, store.rows.size)
        val saved = store.rows.values.single()
        assertTrue(saved.publishedAt != null)
        assertEquals(binder.issued.single(), publisher.published.single().second)
        assertEquals(listOf("SUCCESS"), runs.finished)
    }

    @Test
    fun `같은 observation 재수집은 재발행하지 않는다`() {
        val fetcher = OpinionFetcher { broker, _, _ -> if (broker.code == "00005") listOf(row()) else emptyList() }
        job(fetcher).syncOnce()
        val republished = job(fetcher).syncOnce()

        assertEquals(0, republished)
        assertEquals(1, store.rows.size)
        assertEquals(1, publisher.published.size)
    }

    @Test
    fun `발행 실패 시 마킹하지 않고 다음 회차가 같은 eventId로 재발행한다`() {
        publisher.accept = false
        val fetcher = OpinionFetcher { broker, _, _ -> if (broker.code == "00005") listOf(row()) else emptyList() }
        job(fetcher).syncOnce()

        assertTrue(store.rows.values.single().publishedAt == null)
        val firstEventId = binder.issued.single()

        publisher.accept = true
        val published = job(fetcher).syncOnce()

        assertEquals(1, published)
        assertEquals(listOf(firstEventId, firstEventId), publisher.published.map { it.second })
        assertTrue(store.rows.values.single().publishedAt != null)
    }

    @Test
    fun `발행 직전 크래시를 재개하면 연결된 eventId를 그대로 쓴다`() {
        store.seed(
            observation = observation(),
            streamEventId = "01EXISTING00000000000000000",
        )

        val published = job(fetcher = { _, _, _ -> emptyList() }).syncOnce()

        assertEquals(1, published)
        assertEquals("01EXISTING00000000000000000", publisher.published.single().second)
        assertEquals(0, binder.issued.size)
    }

    @Test
    fun `락 미획득이면 overrun만 기록하고 아무것도 하지 않는다`() {
        val skipped = job(
            fetcher = { _, _, _ -> error("호출되면 안 된다") },
            locks = LockProvider { Optional.empty() },
        ).syncOnce()

        assertEquals(0, skipped)
        assertEquals(1.0, meters.counter("batch.opinion.overrun").count())
        assertEquals(0, runs.finished.size)
    }

    @Test
    fun `주말과 휴장일은 KIS를 호출하지 않는다`() {
        job(fetcher = { _, _, _ -> error("호출되면 안 된다") }, now = Instant.parse("2026-08-08T01:00:00Z")).syncOnce()
        job(
            fetcher = { _, _, _ -> error("호출되면 안 된다") },
            holidays = setOf(java.time.LocalDate.of(2026, 8, 7)),
        ).syncOnce()

        assertEquals(0, runs.finished.size)
    }

    @Test
    fun `조회 시작일은 직전 영업일이다 - 주말·휴장일을 건너뛴다`() {
        var range: Pair<java.time.LocalDate, java.time.LocalDate>? = null
        job(
            fetcher = { _, from, to ->
                range = from to to
                emptyList()
            },
            directory = BrokerDirectory { listOf(brokers.first()) },
            now = Instant.parse("2026-08-10T01:00:00Z"),
            holidays = setOf(java.time.LocalDate.of(2026, 8, 7)),
        ).syncOnce()

        assertEquals(java.time.LocalDate.of(2026, 8, 6) to java.time.LocalDate.of(2026, 8, 10), range)
    }

    @Test
    fun `회차 데드라인을 넘기면 잔여 회원사를 다음 회차로 미루고 락 만료 전에 끝낸다`() {
        var now = weekday
        val fetched = mutableListOf<String>()
        val job = InvestOpinionSyncJob(
            brokers = { brokers },
            fetcher = { broker, _, _ ->
                fetched += broker.code
                now = now.plusSeconds(600)
                emptyList()
            },
            store = store,
            binder = binder,
            publisher = publisher,
            runs = runs,
            locks = grantingLocks(),
            meters = meters,
            requestInterval = Duration.ZERO,
            clock = { now },
            pause = {},
        )

        job.syncOnce()

        assertEquals(listOf("00005"), fetched)
        assertEquals(1.0, meters.counter("batch.opinion.deadline").count())
        assertEquals(listOf("SUCCESS"), runs.finished)
    }

    @Test
    fun `데드라인 이연 후 다음 회차는 이연 지점부터 회원사를 재개한다`() {
        var now = weekday
        val fetched = mutableListOf<String>()
        val job = InvestOpinionSyncJob(
            brokers = { brokers },
            fetcher = { broker, _, _ ->
                fetched += broker.code
                now = now.plusSeconds(600)
                emptyList()
            },
            store = store,
            binder = binder,
            publisher = publisher,
            runs = runs,
            locks = grantingLocks(),
            meters = meters,
            requestInterval = Duration.ZERO,
            clock = { now },
            pause = {},
        )

        job.syncOnce()
        job.syncOnce()

        assertEquals(listOf("00005", "00003"), fetched)
    }

    @Test
    fun `발행 루프도 데드라인에서 멈추고 잔여 이벤트를 다음 회차로 미룬다`() {
        store.seed(observation(), streamEventId = "01EXISTING00000000000000000")
        store.seed(
            observation().copy(code = "000660", contentHash = OpinionObservation.contentHash("중립", null, null)),
            streamEventId = "01EXISTING00000000000000001",
        )
        var now = weekday
        val slowPublisher = OpinionPublisher { code, eventId, _ ->
            publisher.published += code to eventId
            now = now.plusSeconds(600)
            true
        }
        val job = InvestOpinionSyncJob(
            brokers = { emptyList() },
            fetcher = { _, _, _ -> emptyList() },
            store = store,
            binder = binder,
            publisher = slowPublisher,
            runs = runs,
            locks = grantingLocks(),
            meters = meters,
            requestInterval = Duration.ZERO,
            clock = { now },
            pause = {},
        )

        val published = job.syncOnce()

        assertEquals(1, published)
        assertEquals(1, store.rows.values.count { it.publishedAt == null })
        assertEquals(1.0, meters.counter("batch.opinion.deadline").count())
    }

    @Test
    fun `100행 응답은 절단 경고 메트릭을 남긴다`() {
        val rows = (1..100).map { row(code = it.toString().padStart(6, '0'), rating = "매수$it") }
        job(fetcher = { broker, _, _ -> if (broker.code == "00005") rows else emptyList() }).syncOnce()

        assertEquals(1.0, meters.counter("batch.opinion.truncated").count())
    }

    @Test
    fun `한 회원사 조회 실패는 다른 회원사 수집과 발행을 막지 않는다`() {
        val published = job(
            fetcher = { broker, _, _ ->
                if (broker.code == "00005") error("KIS timeout") else listOf(row())
            },
        ).syncOnce()

        assertEquals(1, published)
        assertEquals(listOf("SUCCESS"), runs.finished)
        assertEquals(1, runs.lastFailCount)
    }

    @Test
    fun `일시 실패한 회원사는 잡 말미에 1회 재시도해 같은 회차에 수집한다`() {
        var attempts = 0
        val published = job(
            fetcher = { broker, _, _ ->
                if (broker.code == "00005") {
                    attempts++
                    if (attempts == 1) error("transient") else listOf(row())
                } else {
                    emptyList()
                }
            },
        ).syncOnce()

        assertEquals(1, published)
        assertEquals(2, attempts)
        assertEquals(0, runs.lastFailCount)
        assertEquals(listOf("SUCCESS"), runs.finished)
    }

    @Test
    fun `회원사 목록 조회가 실패하면 잡을 실패로 기록한다`() {
        val job = job(fetcher = { _, _, _ -> emptyList() }, directory = BrokerDirectory { error("master down") })

        kotlin.test.assertFailsWith<IllegalStateException> { job.syncOnce() }
        assertEquals(listOf("FAILED"), runs.finished)
    }

    private fun observation() = OpinionObservation(
        code = "005930",
        businessDate = "20260807",
        brokerCode = "00005",
        brokerName = "미래에셋",
        rating = "매수",
        previousRating = null,
        targetPrice = null,
        contentHash = OpinionObservation.contentHash("매수", null, null),
        collectedAt = weekday,
    )

    private class NoopLock : SimpleLock {
        override fun unlock() {}
    }

    private class FakeRuns : BatchJobRunStore {
        val finished = mutableListOf<String>()
        var lastFailCount = -1

        override fun start(job: String, runDate: String, startedAt: Instant): Long = 1L
        override fun restart(job: String, runDate: String, startedAt: Instant): Long = 1L
        override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
            finished += "SUCCESS"
            lastFailCount = failCount
        }

        override fun fail(id: Long, error: String, finishedAt: Instant) {
            finished += "FAILED"
        }
    }

    private class StoredRow(
        val observation: OpinionObservation,
        var streamEventId: String?,
        var publishedAt: Instant?,
    )

    private class FakeStore : InvestOpinionStore {
        val rows = linkedMapOf<String, StoredRow>()

        fun seed(observation: OpinionObservation, streamEventId: String?) {
            rows[observation.sourceKey] = StoredRow(observation, streamEventId, null)
        }

        override fun insertIfAbsent(observation: OpinionObservation): Boolean {
            if (observation.sourceKey in rows) return false
            rows[observation.sourceKey] = StoredRow(observation, null, null)
            return true
        }

        override fun findUnpublished(limit: Int): List<UnpublishedOpinion> =
            rows.values.filter { it.publishedAt == null }
                .take(limit)
                .map { UnpublishedOpinion(it.observation, it.streamEventId) }

        override fun markPublished(streamEventId: String, at: Instant): Boolean {
            val row = rows.values.singleOrNull { it.streamEventId == streamEventId && it.publishedAt == null }
                ?: return false
            row.publishedAt = at
            return true
        }
    }

    private class FakeBinder(private val store: FakeStore) : OpinionEventBinder {
        val issued = mutableListOf<String>()
        private var sequence = 0

        override fun ensureEvent(opinion: UnpublishedOpinion): String {
            opinion.streamEventId?.let { return it }
            val eventId = "01FAKE%020d".format(sequence++)
            issued += eventId
            store.rows.getValue(opinion.observation.sourceKey).streamEventId = eventId
            return eventId
        }
    }

    private class FakePublisher : OpinionPublisher {
        var accept = true
        val published = mutableListOf<Pair<String, String>>()

        override fun publish(code: String, eventId: String, data: StreamData): Boolean {
            published += code to eventId
            return accept
        }
    }
}
