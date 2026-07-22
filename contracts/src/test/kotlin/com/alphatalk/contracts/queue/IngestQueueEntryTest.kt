package com.alphatalk.contracts.queue

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
    fun `codes 공란은 macroHint 없이 불가`() {
        assertFailsWith<IllegalArgumentException> { entry.copy(codes = emptyList()) }
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
}
