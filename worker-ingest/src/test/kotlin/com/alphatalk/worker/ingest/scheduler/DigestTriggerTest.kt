package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.dedup.SeenMarker
import com.alphatalk.worker.ingest.queue.IngestQueue
import org.junit.jupiter.api.Test
import org.springframework.scheduling.annotation.Scheduled
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DigestTriggerTest {
    private val queue = RecordingQueue()
    private val seen = InMemorySeenMarker()
    private val stocks = listOf(
        IngestProperties.Stock("005930", "삼성전자"),
        IngestProperties.Stock("000660", "SK하이닉스"),
    )

    private fun triggerAt(instant: String, digest: IngestProperties.Digest = IngestProperties.Digest()) =
        DigestTrigger(
            queue = queue,
            seen = seen,
            props = IngestProperties(stocks = stocks, digest = digest),
            catchUpExecutor = { it.run() },
            clock = Clock.fixed(Instant.parse(instant), ZoneOffset.UTC),
        )

    @Test
    fun `종목별 digest 잡을 계약 규약대로 적재`() {
        val enqueued = triggerAt("2026-07-16T09:00:00Z").triggerFor(LocalDate.parse("2026-07-16"))

        assertEquals(2, enqueued)
        val entry = queue.entries.first()
        assertEquals(IngestType.DIGEST, entry.type)
        assertEquals("digest:005930:2026-07-16", entry.sourceId)
        assertEquals(IngestQueueEntry.DIGEST_SOURCE, entry.source)
        assertEquals(listOf("005930"), entry.codes)
        assertEquals("", entry.title)
    }

    @Test
    fun `같은 날짜를 다시 트리거해도 잡은 한 번만 적재된다`() {
        val trigger = triggerAt("2026-07-16T09:00:00Z")

        assertEquals(2, trigger.triggerFor(LocalDate.parse("2026-07-16")))
        assertEquals(0, trigger.triggerFor(LocalDate.parse("2026-07-16")))
        assertEquals(2, queue.entries.size)
    }

    @Test
    fun `적재에 실패한 종목은 마커를 되돌려 다음 트리거에서 재시도된다`() {
        queue.failFor("005930")
        val trigger = triggerAt("2026-07-16T09:00:00Z")

        assertEquals(1, trigger.triggerFor(LocalDate.parse("2026-07-16")))

        queue.failFor(null)
        assertEquals(1, trigger.triggerFor(LocalDate.parse("2026-07-16")))
        assertEquals(listOf("000660", "005930"), queue.entries.map { it.codes.single() })
    }

    @Test
    fun `예정 시각이 지난 뒤 기동하면 오늘치를 보충 적재한다`() {
        triggerAt("2026-07-16T11:30:00Z").catchUpOnStartup()

        assertEquals(2, queue.entries.size)
        assertTrue(queue.entries.all { it.sourceId.endsWith("2026-07-16") })
    }

    @Test
    fun `예정 시각 전에 기동하면 놓친 전일치를 보충 적재한다`() {
        triggerAt("2026-07-16T02:00:00Z").catchUpOnStartup()

        assertEquals(2, queue.entries.size)
        assertTrue(queue.entries.all { it.sourceId.endsWith("2026-07-15") })
    }

    @Test
    fun `며칠 죽어 있다 기동해도 가장 최근에 놓친 실행 한 건만 보충한다`() {
        triggerAt("2026-07-20T02:00:00Z").catchUpOnStartup()

        assertEquals(2, queue.entries.size)
        assertTrue(queue.entries.all { it.sourceId.endsWith("2026-07-19") })
    }

    @Test
    fun `크론이 이미 돈 날 재기동해도 중복 적재하지 않는다`() {
        val trigger = triggerAt("2026-07-16T11:30:00Z")
        trigger.trigger()

        trigger.catchUpOnStartup()

        assertEquals(2, queue.entries.size)
    }

    @Test
    fun `보충한 다음 재기동해도 같은 날짜를 다시 적재하지 않는다`() {
        triggerAt("2026-07-16T02:00:00Z").catchUpOnStartup()
        triggerAt("2026-07-16T03:00:00Z").catchUpOnStartup()

        assertEquals(2, queue.entries.size)
    }

    @Test
    fun `크론을 비활성 표기로 꺼도 기동에 실패하지 않고 보충만 건너뛴다`() {
        triggerAt("2026-07-16T11:30:00Z", IngestProperties.Digest(cron = Scheduled.CRON_DISABLED)).catchUpOnStartup()

        assertEquals(0, queue.entries.size)
    }

    @Test
    fun `보충을 끄면 예정 시각이 지나도 적재하지 않는다`() {
        triggerAt("2026-07-16T11:30:00Z", IngestProperties.Digest(catchUpOnStartup = false)).catchUpOnStartup()

        assertEquals(0, queue.entries.size)
    }

    private class RecordingQueue : IngestQueue {
        val entries = mutableListOf<IngestQueueEntry>()
        private var failingCode: String? = null

        fun failFor(code: String?) {
            failingCode = code
        }

        override fun enqueue(entry: IngestQueueEntry) {
            if (entry.codes.single() == failingCode) throw IllegalStateException("queue down")
            entries.add(entry)
        }
    }

    private class InMemorySeenMarker : SeenMarker {
        private val marked = mutableSetOf<String>()
        override fun markIfNew(sourceId: String) = marked.add(sourceId)
        override fun clear(sourceId: String) {
            marked.remove(sourceId)
        }
    }
}
