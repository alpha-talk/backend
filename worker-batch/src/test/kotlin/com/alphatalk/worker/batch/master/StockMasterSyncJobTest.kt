package com.alphatalk.worker.batch.master

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.master.KisMarket
import com.alphatalk.kis.master.KisStockMaster
import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class StockMasterSyncJobTest {
    private val startedAt = Instant.parse("2026-07-28T23:00:00Z")
    private val today = LocalDate.of(2026, 7, 29)

    private class RecordingFetcher(
        private val failing: Set<KisMarket> = emptySet(),
        private val failuresBeforeSuccess: MutableMap<KisMarket, Int> = mutableMapOf(),
        private val corrupt: Set<KisMarket> = emptySet(),
    ) : MasterFileFetcher {
        val requested = mutableListOf<KisMarket>()

        override fun fetch(market: KisMarket): ByteArray {
            requested += market
            if (market in failing) throw KisClientException("boom")
            val remaining = failuresBeforeSuccess[market] ?: 0
            if (remaining > 0) {
                failuresBeforeSuccess[market] = remaining - 1
                throw KisClientException("transient")
            }
            val name = if (market == KisMarket.KOSPI) "kospi_master_sample.mst" else "kosdaq_master_sample.mst"
            val bytes = javaClass.getResourceAsStream("/fixtures/$name")!!.readBytes()
            return if (market in corrupt) bytes + "짧은 줄\n".toByteArray() else bytes
        }

    }

    private class RecordingStockStore(
        private val onDeactivate: (() -> Unit)? = null,
    ) : StockMasterStore {
        val upserted = mutableListOf<KisStockMaster>()
        var deactivateCalls = 0
        var deactivatedWith: List<String> = emptyList()

        override fun upsertAll(stocks: List<KisStockMaster>): Int {
            upserted += stocks
            return stocks.size
        }

        override fun deactivateMissing(activeCodes: Collection<String>): Int {
            deactivateCalls += 1
            deactivatedWith = activeCodes.toList()
            onDeactivate?.invoke()
            return 0
        }
    }


    private class RecordingRuns(private val startResult: Long?) : BatchJobRunStore {
        var succeeded: Triple<Long, Int, Int>? = null
        var failed: Pair<Long, String>? = null
        var failedCounts: Triple<Long, Int, Int>? = null

        override fun start(job: String, runDate: String, startedAt: Instant): Long? = startResult

        override fun restart(job: String, runDate: String, startedAt: Instant): Long = startResult ?: 1L

        override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
            succeeded = Triple(id, okCount, failCount)
        }

        override fun fail(id: Long, error: String, finishedAt: Instant) {
            failed = id to error
        }

        override fun failCounted(id: Long, okCount: Int, failCount: Int, error: String, finishedAt: Instant) {
            failed = id to error
            failedCounts = Triple(id, okCount, failCount)
        }
    }

    private fun job(
        files: MasterFileFetcher,
        stocks: StockMasterStore,
        runs: BatchJobRunStore,
        meters: SimpleMeterRegistry = SimpleMeterRegistry(),
    ) = StockMasterSyncJob(
        files,
        stocks,
        runs,
        meters,
        clock = { startedAt },
        today = { today },
    )

    @Test
    fun `두 시장을 적재하고 주권만 남긴다`() {
        val store = RecordingStockStore()
        val runs = RecordingRuns(startResult = 1L)

        val ok = job(RecordingFetcher(), store, runs).syncOnce()

        assertEquals(6, ok)
        assertTrue(store.upserted.all { it.isCommonStock })
        assertFalse(store.upserted.any { it.groupCode == "EF" })
        assertEquals(Triple(1L, 6, 0), runs.succeeded)
    }

    @Test
    fun `이미 오늘 성공했으면 마스터를 내려받지 않는다`() {
        val files = RecordingFetcher()
        val store = RecordingStockStore()

        val ok = job(files, store, RecordingRuns(startResult = null)).syncOnce()

        assertEquals(0, ok)
        assertTrue(files.requested.isEmpty())
    }

    @Test
    fun `일시적 실패는 한 번 더 시도해 회복한다`() {
        val files = RecordingFetcher(failuresBeforeSuccess = mutableMapOf(KisMarket.KOSPI to 1))
        val runs = RecordingRuns(startResult = 1L)

        val ok = job(files, RecordingStockStore(), runs).syncOnce()

        assertEquals(6, ok)
        assertEquals(2, files.requested.count { it == KisMarket.KOSPI })
        assertEquals(Triple(1L, 6, 0), runs.succeeded)
    }

    @Test
    fun `재시도까지 실패하면 부분 유니버스라 회차를 FAILED로 남긴다`() {
        val files = RecordingFetcher(failing = setOf(KisMarket.KOSDAQ))
        val runs = RecordingRuns(startResult = 1L)

        val ok = job(files, RecordingStockStore(), runs).syncOnce()

        assertEquals(3, ok)
        assertEquals(2, files.requested.count { it == KisMarket.KOSDAQ })
        assertEquals(null, runs.succeeded)
        assertEquals(Triple(1L, 3, 1), runs.failedCounts)
    }

    @Test
    fun `일부 시장이 실패하면 상장폐지 정리를 건너뛴다`() {
        val store = RecordingStockStore()

        job(RecordingFetcher(failing = setOf(KisMarket.KOSDAQ)), store, RecordingRuns(startResult = 1L)).syncOnce()

        assertEquals(0, store.deactivateCalls)
    }

    @Test
    fun `재실행 진입점은 FAILED 회차를 같은 날 다시 채운다`() {
        val files = RecordingFetcher()
        val store = RecordingStockStore()

        job(files, store, RecordingRuns(startResult = 1L)).scheduledRetry()

        assertTrue(store.upserted.isNotEmpty())
    }

    @Test
    fun `재실행 진입점도 이미 성공한 날은 내려받지 않는다`() {
        val files = RecordingFetcher()

        job(files, RecordingStockStore(), RecordingRuns(startResult = null)).scheduledRetry()

        assertTrue(files.requested.isEmpty())
    }

    @Test
    fun `읽지 못한 행이 있으면 상장폐지 정리를 건너뛰고 회차도 FAILED로 남긴다`() {
        val store = RecordingStockStore()
        val runs = RecordingRuns(startResult = 1L)

        job(RecordingFetcher(corrupt = setOf(KisMarket.KOSPI)), store, runs).syncOnce()

        assertEquals(0, store.deactivateCalls)
        assertTrue(store.upserted.isNotEmpty())
        assertEquals(null, runs.succeeded)
        assertNotNull(runs.failedCounts)
    }

    @Test
    fun `모두 온전히 읽었을 때만 이번에 수집한 종목만 남긴다`() {
        val store = RecordingStockStore()

        job(RecordingFetcher(), store, RecordingRuns(startResult = 1L)).syncOnce()

        assertEquals(1, store.deactivateCalls)
        assertEquals(store.upserted.map { it.code }.toSet(), store.deactivatedWith.toSet())
    }

    @Test
    fun `정리 단계가 실패하면 실행 이력을 실패로 남긴다`() {
        val runs = RecordingRuns(startResult = 1L)
        val store = RecordingStockStore(onDeactivate = { throw IllegalStateException("db down") })

        assertFailsWith<IllegalStateException> { job(RecordingFetcher(), store, runs).syncOnce() }

        assertEquals(1L, runs.failed?.first)
        assertEquals(null, runs.succeeded)
    }
}
