package com.alphatalk.contracts.queue

import com.alphatalk.contracts.envelope.StreamCategory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class IngestQueueEntryTest {
    private val entry = IngestQueueEntry(
        source = "hankyung",
        sourceId = "hankyung:a1b2c3",
        type = IngestType.NEWS,
        codes = listOf("005930", "000660"),
        title = "삼성전자·SK하이닉스 동반 강세",
        url = "https://example.com/news/1",
        body = "발췌",
        fetchedAt = 1719500000000,
    )

    @Test
    fun `필드 왕복`() {
        assertEquals(entry, IngestQueueEntry.fromFields(entry.toFields()))
    }

    @Test
    fun `선택 필드는 생략된다`() {
        val fields = entry.copy(body = null).toFields()
        assertFalse(IngestQueueEntry.FIELD_BODY in fields)
        assertFalse(IngestQueueEntry.FIELD_MACRO_HINT in fields)
    }

    @Test
    fun `매크로 기사 - codes 공란 + macroHint 왕복`() {
        val macro = entry.copy(codes = emptyList(), macroHint = "금리")
        val back = IngestQueueEntry.fromFields(macro.toFields())
        assertEquals(macro, back)
        assertEquals(emptyList(), back.codes)
    }

    @Test
    fun `codes 공란 - macroHint 없이도 허용, 관련성 판정은 LLM 몫`() {
        val gateless = entry.copy(codes = emptyList())
        val back = IngestQueueEntry.fromFields(gateless.toFields())
        assertEquals(gateless, back)
        assertEquals(emptyList(), back.codes)
    }

    @Test
    fun `digest는 code 정확히 1개 필수`() {
        assertFailsWith<IllegalArgumentException> {
            entry.copy(type = IngestType.DIGEST, codes = emptyList())
        }
        assertFailsWith<IllegalArgumentException> {
            entry.copy(type = IngestType.DIGEST, codes = listOf("005930", "000660"))
        }
    }

    @Test
    fun `digest 잡 규약`() {
        assertEquals("digest:005930:2026-07-16", IngestQueueEntry.digestSourceId("005930", "2026-07-16"))
        val digest = IngestQueueEntry(
            source = IngestQueueEntry.DIGEST_SOURCE,
            sourceId = IngestQueueEntry.digestSourceId("005930", "2026-07-16"),
            type = IngestType.DIGEST,
            codes = listOf("005930"),
            title = "",
            url = "",
            fetchedAt = 1719500000000,
        )
        assertEquals(digest, IngestQueueEntry.fromFields(digest.toFields()))
    }

    @Test
    fun `미지의 type 거부`() {
        assertFailsWith<IllegalArgumentException> { IngestType.from("unknown") }
    }

    @Test
    fun `수집 type은 발행 카테고리로 관통 - ws_api §4_3 category와 stream_event type`() {
        assertEquals(StreamCategory.NEWS, IngestType.NEWS.streamCategory())
        assertEquals(StreamCategory.REPORT, IngestType.REPORT.streamCategory())
        assertEquals(StreamCategory.DISCLOSURE, IngestType.DISCLOSURE.streamCategory())
        assertEquals(StreamCategory.AI, IngestType.DIGEST.streamCategory())
        assertEquals("report", StreamCategory.REPORT.payload)
        assertEquals("REPORT", StreamCategory.REPORT.eventType)
        assertEquals(StreamCategory.AI, StreamCategory.fromPayload("ai"))
    }
}
