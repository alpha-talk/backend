package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.queue.DigestEnqueueResult
import com.alphatalk.worker.ingest.queue.DigestJobQueue
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MarketDigestTriggerTest {
    private val queue = RecordingDigestJobQueue()

    private fun triggerAt(instant: String, digest: IngestProperties.Digest = IngestProperties.Digest()) =
        MarketDigestTrigger(
            queue = queue,
            props = IngestProperties(digest = digest),
            meters = SimpleMeterRegistry(),
            catchUpExecutor = { it.run() },
            clock = Clock.fixed(Instant.parse(instant), ZoneOffset.UTC),
        )

    @Test
    fun `시장 잡을 계약 규약대로 1건 적재`() {
        triggerAt("2026-07-16T09:00:00Z").trigger()

        val entry = queue.entries.single()
        assertEquals(IngestType.DIGEST, entry.type)
        assertEquals("digest:MARKET:2026-07-16", entry.sourceId)
        assertEquals(IngestQueueEntry.DIGEST_SOURCE, entry.source)
        assertEquals(listOf(IngestQueueEntry.MARKET_CODE), entry.codes)
    }

    @Test
    fun `같은 날짜를 다시 트리거해도 잡은 한 번만 적재된다`() {
        val trigger = triggerAt("2026-07-16T09:00:00Z")

        trigger.trigger()
        trigger.trigger()

        assertEquals(1, queue.entries.size)
    }

    @Test
    fun `예정 시각이 지난 뒤 기동하면 오늘치를 보충 적재한다`() {
        triggerAt("2026-07-16T11:30:00Z").catchUpOnStartup()

        assertEquals("digest:MARKET:2026-07-16", queue.entries.single().sourceId)
    }

    @Test
    fun `예정 시각 전에 기동하면 놓친 전일치를 보충 적재한다`() {
        triggerAt("2026-07-16T02:00:00Z").catchUpOnStartup()

        assertEquals("digest:MARKET:2026-07-15", queue.entries.single().sourceId)
    }

    @Test
    fun `크론 틱이 자정을 넘겨 지연돼도 예정 날짜로 적재한다`() {
        triggerAt("2026-07-16T15:10:00Z").trigger()

        assertEquals("digest:MARKET:2026-07-16", queue.entries.single().sourceId)
    }

    @Test
    fun `기동 시 Redis 장애로 보충이 실패하면 다음 재조정에서 다시 적재한다`() {
        queue.failAll(true)
        val trigger = triggerAt("2026-07-16T11:30:00Z")

        trigger.catchUpOnStartup()
        assertEquals(0, queue.entries.size)

        queue.failAll(false)
        trigger.reconcileCatchUp()

        assertEquals("digest:MARKET:2026-07-16", queue.entries.single().sourceId)
    }

    @Test
    fun `보충한 다음 재조정이 다시 돌아도 중복 적재하지 않는다`() {
        val trigger = triggerAt("2026-07-16T11:30:00Z")

        trigger.catchUpOnStartup()
        trigger.reconcileCatchUp()

        assertEquals(1, queue.entries.size)
    }

    @Test
    fun `보충을 끄면 예정 시각이 지나도 적재하지 않는다`() {
        val trigger = triggerAt("2026-07-16T11:30:00Z", IngestProperties.Digest(catchUpOnStartup = false))

        trigger.catchUpOnStartup()
        trigger.reconcileCatchUp()

        assertEquals(0, queue.entries.size)
    }

    private class RecordingDigestJobQueue : DigestJobQueue {
        val entries = mutableListOf<IngestQueueEntry>()
        private val marked = mutableSetOf<String>()
        private var failingAll = false

        fun failAll(failing: Boolean) {
            failingAll = failing
        }

        override fun enqueueIfNew(entry: IngestQueueEntry): DigestEnqueueResult {
            if (failingAll) throw IllegalStateException("queue down")
            if (!marked.add(entry.sourceId)) return DigestEnqueueResult.ALREADY_ENQUEUED
            entries.add(entry)
            return DigestEnqueueResult.ENQUEUED
        }
    }
}
