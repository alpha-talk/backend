package com.alphatalk.worker.batch.master

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.master.KisMarket
import com.alphatalk.kis.master.KisSector
import com.alphatalk.kis.master.KisStockMaster
import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
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
        var sectorRequests = 0

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

        override fun fetchSectors(): ByteArray {
            sectorRequests += 1
            return javaClass.getResourceAsStream("/fixtures/idxcode_sample.mst")!!.readBytes()
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

    private class RecordingSectorStore : SectorStore {
        val upserted = mutableListOf<KisSector>()

        override fun upsertAll(sectors: List<KisSector>): Int {
            upserted += sectors
            return sectors.size
        }
    }

    private class RecordingRuns(private val startResult: Long?) : BatchJobRunStore {
        var succeeded: Triple<Long, Int, Int>? = null
        var failed: Pair<Long, String>? = null

        override fun start(job: String, runDate: String, startedAt: Instant): Long? = startResult

        override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
            succeeded = Triple(id, okCount, failCount)
        }

        override fun fail(id: Long, error: String, finishedAt: Instant) {
            failed = id to error
        }
    }

    private fun job(
        files: MasterFileFetcher,
        stocks: StockMasterStore,
        runs: BatchJobRunStore,
        sectors: SectorStore = RecordingSectorStore(),
    ) = StockMasterSyncJob(
        files,
        stocks,
        sectors,
        runs,
        SimpleMeterRegistry(),
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
    fun `업종 마스터를 함께 적재한다`() {
        val sectors = RecordingSectorStore()

        job(RecordingFetcher(), RecordingStockStore(), RecordingRuns(startResult = 1L), sectors).syncOnce()

        assertTrue(sectors.upserted.isNotEmpty())
        assertEquals("제조", sectors.upserted.single { it.code == "00027" }.name)
        assertEquals("제조", sectors.upserted.single { it.code == "11009" }.name)
    }

    @Test
    fun `종목의 업종 코드가 업종 마스터 코드와 맞물린다`() {
        val stocks = RecordingStockStore()
        val sectors = RecordingSectorStore()

        job(RecordingFetcher(), stocks, RecordingRuns(startResult = 1L), sectors).syncOnce()

        val sectorCodes = sectors.upserted.map { it.code }.toSet()
        val used = stocks.upserted.mapNotNull { it.sectorCode }.toSet()
        assertTrue(used.isNotEmpty())
        assertTrue(used.all { it in sectorCodes }, "매칭되지 않는 업종 코드: ${used - sectorCodes}")
    }

    @Test
    fun `이미 오늘 성공했으면 마스터를 내려받지 않는다`() {
        val files = RecordingFetcher()
        val store = RecordingStockStore()

        val ok = job(files, store, RecordingRuns(startResult = null)).syncOnce()

        assertEquals(0, ok)
        assertTrue(files.requested.isEmpty())
        assertEquals(0, files.sectorRequests)
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
    fun `재시도까지 실패하면 그 시장만 실패로 남는다`() {
        val files = RecordingFetcher(failing = setOf(KisMarket.KOSDAQ))
        val runs = RecordingRuns(startResult = 1L)

        val ok = job(files, RecordingStockStore(), runs).syncOnce()

        assertEquals(3, ok)
        assertEquals(2, files.requested.count { it == KisMarket.KOSDAQ })
        assertEquals(Triple(1L, 3, 1), runs.succeeded)
    }

    @Test
    fun `일부 시장이 실패하면 상장폐지 정리를 건너뛴다`() {
        val store = RecordingStockStore()

        job(RecordingFetcher(failing = setOf(KisMarket.KOSDAQ)), store, RecordingRuns(startResult = 1L)).syncOnce()

        assertEquals(0, store.deactivateCalls)
    }

    @Test
    fun `읽지 못한 행이 있으면 상장폐지 정리를 건너뛴다`() {
        val store = RecordingStockStore()

        job(RecordingFetcher(corrupt = setOf(KisMarket.KOSPI)), store, RecordingRuns(startResult = 1L)).syncOnce()

        assertEquals(0, store.deactivateCalls)
        assertTrue(store.upserted.isNotEmpty())
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
