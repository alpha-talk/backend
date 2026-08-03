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
        private var corruptSectorTimes: Int = 0,
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
            val bytes = javaClass.getResourceAsStream("/fixtures/idxcode_sample.mst")!!.readBytes()
            if (corruptSectorTimes > 0) {
                corruptSectorTimes -= 1
                return bytes + "짧은 줄\n".toByteArray()
            }
            return bytes
        }
    }

    private class RecordingStockStore(
        private val onDeactivate: (() -> Unit)? = null,
        private val callOrder: MutableList<String>? = null,
    ) : StockMasterStore {
        val upserted = mutableListOf<KisStockMaster>()
        var deactivateCalls = 0
        var deactivatedWith: List<String> = emptyList()

        override fun upsertAll(stocks: List<KisStockMaster>): Int {
            callOrder?.add("stocks")
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

    private class RecordingSectorStore(
        private val callOrder: MutableList<String>? = null,
    ) : SectorStore {
        val upserted = mutableListOf<KisSector>()

        override fun upsertAll(sectors: List<KisSector>): Int {
            callOrder?.add("sectors")
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
        meters: SimpleMeterRegistry = SimpleMeterRegistry(),
    ) = StockMasterSyncJob(
        files,
        stocks,
        sectors,
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
    fun `업종 마스터를 함께 적재한다`() {
        val sectors = RecordingSectorStore()

        job(RecordingFetcher(), RecordingStockStore(), RecordingRuns(startResult = 1L), sectors).syncOnce()

        assertTrue(sectors.upserted.isNotEmpty())
        assertEquals("제조", sectors.upserted.single { it.code == "00027" }.name)
        assertEquals("제조", sectors.upserted.single { it.code == "11009" }.name)
    }

    @Test
    fun `적재한 업종은 모두 구성 종목을 가진다`() {
        val stocks = RecordingStockStore()
        val sectors = RecordingSectorStore()

        job(RecordingFetcher(), stocks, RecordingRuns(startResult = 1L), sectors).syncOnce()

        val referenced = stocks.upserted.mapNotNull { it.sectorCode }.toSet()
        val stored = sectors.upserted.map { it.code }.toSet()
        assertTrue(stored.isNotEmpty())
        assertEquals(referenced, stored, "구성 종목이 없는 업종이 섞였다: ${stored - referenced}")
    }

    @Test
    fun `종목이 참조하지 않는 지수 항목은 적재하지 않는다`() {
        val sectors = RecordingSectorStore()

        job(RecordingFetcher(), RecordingStockStore(), RecordingRuns(startResult = 1L), sectors).syncOnce()

        val stored = sectors.upserted.map { it.code }.toSet()
        assertFalse("00001" in stored, "지수 '종합'이 업종으로 적재됐다")
        assertFalse("11001" in stored, "지수 'KOSDAQ'이 업종으로 적재됐다")
    }

    @Test
    fun `업종 파일에 읽지 못한 행이 있으면 한 번 더 받아 회복한다`() {
        val files = RecordingFetcher(corruptSectorTimes = 1)
        val sectors = RecordingSectorStore()

        job(files, RecordingStockStore(), RecordingRuns(startResult = 1L), sectors).syncOnce()

        assertEquals(2, files.sectorRequests)
        assertTrue(sectors.upserted.isNotEmpty())
    }

    @Test
    fun `업종 파일이 계속 불완전해도 종목 적재는 지키고 경고만 남긴다`() {
        val files = RecordingFetcher(corruptSectorTimes = 2)
        val store = RecordingStockStore()
        val runs = RecordingRuns(startResult = 1L)
        val sectors = RecordingSectorStore()
        val meters = SimpleMeterRegistry()

        val ok = job(files, store, runs, sectors, meters).syncOnce()

        assertEquals(6, ok)
        assertEquals(Triple(1L, 6, 0), runs.succeeded)
        assertEquals(null, runs.failed)
        assertTrue(store.upserted.isNotEmpty(), "업종 실패가 종목 적재를 막았다")
        assertTrue(sectors.upserted.isEmpty())
        assertEquals(1.0, meters.counter("batch.sector.sync.failed").count())
    }

    @Test
    fun `종목을 먼저 저장한 뒤 업종을 동기화한다`() {
        val order = mutableListOf<String>()

        job(
            RecordingFetcher(),
            RecordingStockStore(callOrder = order),
            RecordingRuns(startResult = 1L),
            RecordingSectorStore(callOrder = order),
        ).syncOnce()

        assertEquals(listOf("stocks", "sectors"), order)
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
