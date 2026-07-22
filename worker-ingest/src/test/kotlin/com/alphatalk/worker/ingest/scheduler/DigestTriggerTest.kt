package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.queue.IngestQueue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertEquals

class DigestTriggerTest {
    private val queue = object : IngestQueue {
        val entries = mutableListOf<IngestQueueEntry>()
        override fun enqueue(entry: IngestQueueEntry) {
            entries.add(entry)
        }
    }
    private val props = IngestProperties(
        stocks = listOf(
            IngestProperties.Stock("005930", listOf("삼성전자")),
            IngestProperties.Stock("000660", listOf("SK하이닉스")),
        ),
    )
    private val trigger = DigestTrigger(
        queue = queue,
        props = props,
        clock = Clock.fixed(Instant.parse("2026-07-16T09:00:00Z"), ZoneOffset.UTC),
    )

    @Test
    fun `종목별 digest 잡을 계약 규약대로 적재`() {
        val enqueued = trigger.triggerFor(LocalDate.parse("2026-07-16"))
        assertEquals(2, enqueued)
        val entry = queue.entries.first()
        assertEquals(IngestType.DIGEST, entry.type)
        assertEquals("digest:005930:2026-07-16", entry.sourceId)
        assertEquals(IngestQueueEntry.DIGEST_SOURCE, entry.source)
        assertEquals(listOf("005930"), entry.codes)
        assertEquals("", entry.title)
    }
}
