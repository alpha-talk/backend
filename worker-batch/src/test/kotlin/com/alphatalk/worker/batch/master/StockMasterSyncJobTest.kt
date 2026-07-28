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
import kotlin.test.assertTrue

class StockMasterSyncJobTest {
    private val startedAt = Instant.parse("2026-07-28T23:00:00Z")
    private val today = LocalDate.of(2026, 7, 29)

    private class RecordingFetcher(
        private val failing: Set<KisMarket> = emptySet(),
    ) : MasterFileFetcher {
        val requested = mutableListOf<KisMarket>()

        override fun fetch(market: KisMarket): ByteArray {
            requested += market
            if (market in failing) throw KisClientException("boom")
            val name = if (market == KisMarket.KOSPI) "kospi_master_sample.mst" else "kosdaq_master_sample.mst"
            return javaClass.getResourceAsStream("/fixtures/$name")!!.readBytes()
        }
    }

    private class RecordingStore(
        private val onUpsert: (() -> Unit)? = null,
        private val onDeactivate: (() -> Unit)? = null,
    ) : StockMasterStore {
        val upserted = mutableListOf<KisStockMaster>()
        var deactivateCalls = 0
        var deactivatedWith: List<String> = emptyList()

        override fun upsertAll(stocks: List<KisStockMaster>): Int {
            onUpsert?.invoke()
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
        store: StockMasterStore,
        runs: BatchJobRunStore,
    ) = StockMasterSyncJob(files, store, runs, SimpleMeterRegistry(), clock = { startedAt }, today = { today })

    @Test
    fun `두 시장을 적재하고 주권만 남긴다`() {
        val store = RecordingStore()
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
        val store = RecordingStore()

        val ok = job(files, store, RecordingRuns(startResult = null)).syncOnce()

        assertEquals(0, ok)
        assertTrue(files.requested.isEmpty())
        assertTrue(store.upserted.isEmpty())
    }

    @Test
    fun `한 시장이 실패해도 나머지는 적재한다`() {
        val store = RecordingStore()
        val runs = RecordingRuns(startResult = 1L)

        val ok = job(RecordingFetcher(failing = setOf(KisMarket.KOSDAQ)), store, runs).syncOnce()

        assertEquals(3, ok)
        assertEquals(Triple(1L, 3, 1), runs.succeeded)
    }

    @Test
    fun `일부 시장이 실패하면 상장폐지 정리를 건너뛴다`() {
        val store = RecordingStore()

        job(RecordingFetcher(failing = setOf(KisMarket.KOSDAQ)), store, RecordingRuns(startResult = 1L)).syncOnce()

        assertEquals(0, store.deactivateCalls)
    }

    @Test
    fun `모두 성공하면 이번에 수집한 종목만 남기고 비활성화한다`() {
        val store = RecordingStore()

        job(RecordingFetcher(), store, RecordingRuns(startResult = 1L)).syncOnce()

        assertEquals(1, store.deactivateCalls)
        assertEquals(store.upserted.map { it.code }.toSet(), store.deactivatedWith.toSet())
    }

    @Test
    fun `정리 단계가 실패하면 실행 이력을 실패로 남긴다`() {
        val runs = RecordingRuns(startResult = 1L)
        val store = RecordingStore(onDeactivate = { throw IllegalStateException("db down") })

        assertFailsWith<IllegalStateException> { job(RecordingFetcher(), store, runs).syncOnce() }

        assertEquals(1L, runs.failed?.first)
        assertEquals(null, runs.succeeded)
    }
}
