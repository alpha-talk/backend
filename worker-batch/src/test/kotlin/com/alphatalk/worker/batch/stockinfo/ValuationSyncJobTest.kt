package com.alphatalk.worker.batch.stockinfo

import com.alphatalk.kis.rest.KisValuationSnapshot
import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ValuationSyncJobTest {
    private val meters = SimpleMeterRegistry()
    private val runs = FakeRuns()
    private val store = FakeValuationStore()
    private val weekday = LocalDate.parse("2026-08-07")

    private fun snapshot(code: String, price: Long = 71200) = KisValuationSnapshot(
        code = code,
        price = price,
        per = BigDecimal("12.10"),
        pbr = BigDecimal("1.35"),
        eps = 5771,
        bps = 52002,
    )

    private fun job(
        fetcher: ValuationFetcher,
        stocks: List<ActiveStock> = listOf(ActiveStock("005930", 5_969_782_550), ActiveStock("000660", 728_002_365)),
        date: LocalDate = weekday,
        holidays: Set<LocalDate> = emptySet(),
        chunkSize: Int = 200,
        runStore: BatchJobRunStore = runs,
    ) = ValuationSyncJob(
        universe = FakeUniverse(stocks),
        fetcher = fetcher,
        store = store,
        runs = runStore,
        meters = meters,
        holidays = holidays,
        chunkSize = chunkSize,
        today = { date },
    )

    @Test
    fun `전 종목 스냅샷을 당일 날짜와 마스터 시총으로 적재한다`() {
        val stored = job(fetcher = { code -> snapshot(code) }).syncOnce()

        assertEquals(2, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        assertEquals(0, runs.lastFailCount)
        val row = store.rows.single { it.code == "005930" }
        assertEquals("20260807", row.date)
        assertEquals(BigDecimal("12.10"), row.per)
        assertEquals(5_969_782_550 * 71200, row.marketCap)
    }

    @Test
    fun `상장주식수가 없으면 시총은 null로 적재한다`() {
        job(
            fetcher = { code -> snapshot(code) },
            stocks = listOf(ActiveStock("005930", null)),
        ).syncOnce()

        assertEquals(null, store.rows.single().marketCap)
    }

    @Test
    fun `잔여 실패는 FAILED로 남겨 당일 재실행이 빈 종목을 채울 수 있게 한다`() {
        val attempts = mutableMapOf<String, Int>()
        var brokenCode: String? = "000660"
        val fetcher = ValuationFetcher { code ->
            attempts.merge(code, 1, Int::plus)
            when {
                code == "005930" && attempts.getValue(code) == 1 -> throw IllegalStateException("transient")
                code == brokenCode -> throw IllegalStateException("outage")
                else -> snapshot(code)
            }
        }

        val stored = job(fetcher = fetcher).syncOnce()

        assertEquals(1, stored)
        assertEquals(2, attempts.getValue("005930"))
        assertEquals(2, attempts.getValue("000660"))
        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1, runs.lastOkCount)
        assertEquals(1, runs.lastFailCount)

        brokenCode = null
        job(fetcher = fetcher).syncOnce()

        assertEquals(listOf("FAILED", "SUCCESS"), runs.finished)
        assertTrue(store.rows.any { it.code == "000660" })
    }

    @Test
    fun `청크 크기마다 나눠 적재해 중단 시 부분 진행이 보존된다`() {
        job(fetcher = { code -> snapshot(code) }, chunkSize = 1).syncOnce()

        assertTrue(store.upsertCalls >= 2)
        assertEquals(2, store.rows.size)
    }

    @Test
    fun `재시도 패스에서 회복한 행도 청크 단위로 적재한다`() {
        val attempts = mutableMapOf<String, Int>()
        job(
            fetcher = { code ->
                attempts.merge(code, 1, Int::plus)
                if (attempts.getValue(code) == 1) throw IllegalStateException("transient") else snapshot(code)
            },
            chunkSize = 1,
        ).syncOnce()

        assertEquals(2, store.rows.size)
        assertTrue(store.maxBatchSize <= 1)
    }

    @Test
    fun `데드라인에 도달하면 부분 진행을 저장하고 FAILED로 남겨 수동 재실행을 연다`() {
        var now = Instant.parse("2026-08-07T07:50:00Z")
        val job = ValuationSyncJob(
            universe = FakeUniverse(listOf(ActiveStock("005930", null), ActiveStock("000660", null))),
            fetcher = { code ->
                now = now.plusSeconds(26 * 60)
                snapshot(code)
            },
            store = store,
            runs = runs,
            meters = meters,
            deadline = java.time.Duration.ofMinutes(25),
            clock = { now },
            today = { weekday },
        )

        val stored = job.syncOnce()

        assertEquals(1, stored)
        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1, store.rows.size)
    }

    @Test
    fun `주말과 휴장일은 실행하지 않는다`() {
        assertEquals(0, job(fetcher = { snapshot(it) }, date = LocalDate.parse("2026-08-08")).syncOnce())
        assertEquals(
            0,
            job(fetcher = { snapshot(it) }, holidays = setOf(weekday)).syncOnce(),
        )
        assertTrue(runs.finished.isEmpty())
    }

    @Test
    fun `당일 SUCCESS가 있으면 스킵한다`() {
        val skipping = object : BatchJobRunStore {
            override fun start(job: String, runDate: String, startedAt: Instant): Long? = null
            override fun restart(job: String, runDate: String, startedAt: Instant): Long = 1L
            override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) = Unit
            override fun fail(id: Long, error: String, finishedAt: Instant) = Unit
        }

        assertEquals(0, job(fetcher = { snapshot(it) }, runStore = skipping).syncOnce())
        assertTrue(store.rows.isEmpty())
    }

    @Test
    fun `유니버스가 비면 FAILED다 - 선행 마스터 동기화 부재는 성공이 아니다`() {
        val stored = job(fetcher = { snapshot(it) }, stocks = emptyList()).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("FAILED"), runs.finished)
    }
}

internal class FakeUniverse(private val stocks: List<ActiveStock>) : StockUniverse {
    override fun activeStocks(): List<ActiveStock> = stocks
    override fun activeCodes(): Set<String> = stocks.mapTo(mutableSetOf(), ActiveStock::code)
}

internal class FakeValuationStore : ValuationStore {
    val rows = mutableListOf<ValuationRow>()
    var upsertCalls = 0
    var maxBatchSize = 0

    override fun upsert(rows: List<ValuationRow>): Int {
        if (rows.isNotEmpty()) upsertCalls += 1
        maxBatchSize = maxOf(maxBatchSize, rows.size)
        this.rows += rows
        return rows.size
    }
}

internal class FakeRuns : BatchJobRunStore {
    val finished = mutableListOf<String>()
    var lastOkCount = -1
    var lastFailCount = -1

    override fun start(job: String, runDate: String, startedAt: Instant): Long = 1L
    override fun restart(job: String, runDate: String, startedAt: Instant): Long = 1L

    override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
        finished += "SUCCESS"
        lastOkCount = okCount
        lastFailCount = failCount
    }

    override fun fail(id: Long, error: String, finishedAt: Instant) {
        finished += "FAILED"
    }

    override fun failCounted(id: Long, okCount: Int, failCount: Int, error: String, finishedAt: Instant) {
        finished += "FAILED"
        lastOkCount = okCount
        lastFailCount = failCount
    }
}
