package com.alphatalk.coreapi.stream

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class StreamStoreIntegrationTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var store: StreamStore

    @Autowired
    private lateinit var quotes: QuoteStore

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM stream_event")
        jdbc.update("DELETE FROM daily_candle")
        listOf(
            Triple("01J9Z800000000000000000001", "NEWS", "첫 뉴스"),
            Triple("01J9Z800000000000000000002", "NEWS", "둘째 뉴스"),
            Triple("01J9Z800000000000000000003", "AI", "브리핑"),
            Triple("01J9Z800000000000000000004", "POST", "유저 글"),
            Triple("01J9Z800000000000000000005", "NEWS", "최신 뉴스"),
        ).forEach { (id, type, title) ->
            jdbc.update(
                """
                INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
                VALUES (?, '005930', ?, now(), 'hankyung', ?::jsonb)
                """.trimIndent(),
                id,
                type,
                """{"title":"$title"}""",
            )
        }
        jdbc.update(
            """
            INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
            VALUES ('01J9Z800000000000000000009', '000660', 'NEWS', now(), 'hankyung', '{"title":"다른 종목"}'::jsonb)
            """.trimIndent(),
        )
        jdbc.update(
            """
            INSERT INTO daily_candle (code, date, open, high, low, close, volume, value) VALUES
            ('005930', '20260727', 70000, 71000, 69800, 70500, 1000, 70000000),
            ('005930', '20260728', 70600, 71500, 70400, 71200, 2000, 142000000)
            """.trimIndent(),
        )
    }

    private fun query(
        cursor: String? = null,
        direction: CursorDirection = CursorDirection.BEFORE,
        limit: Int = 50,
        types: List<StreamEventType> = emptyList(),
    ) = StreamQuery("005930", cursor, direction, limit, types)

    @Test
    fun `커서 없이 조회하면 최신부터 내려온다`() {
        val items = store.find(query())

        assertEquals(5, items.size)
        assertEquals("01J9Z800000000000000000005", items.first().eventId)
        assertEquals("01J9Z800000000000000000001", items.last().eventId)
    }

    @Test
    fun `before는 커서보다 과거를 내림차순으로 준다`() {
        val items = store.find(query(cursor = "01J9Z800000000000000000004", limit = 2))

        assertEquals(listOf("01J9Z800000000000000000003", "01J9Z800000000000000000002"), items.map { it.eventId })
    }

    @Test
    fun `after는 커서 이후를 오름차순으로 준다 - 복구는 오래된 것부터`() {
        val items = store.find(
            query(cursor = "01J9Z800000000000000000002", direction = CursorDirection.AFTER, limit = 2),
        )

        assertEquals(listOf("01J9Z800000000000000000003", "01J9Z800000000000000000004"), items.map { it.eventId })
    }

    @Test
    fun `타입 필터는 지정한 것만 남긴다`() {
        val newsOnly = store.find(query(types = listOf(StreamEventType.NEWS)))
        val newsAndAi = store.find(query(types = listOf(StreamEventType.NEWS, StreamEventType.AI)))

        assertEquals(3, newsOnly.size)
        assertTrue(newsOnly.all { it.type == "NEWS" })
        assertEquals(4, newsAndAi.size)
    }

    @Test
    fun `다른 종목 이벤트는 섞이지 않는다`() {
        val items = store.find(query())

        assertTrue(items.all { it.code == "005930" })
    }

    @Test
    fun `payload는 JSON 구조 그대로 돌려준다`() {
        val latest = store.find(query(limit = 1)).single()

        assertEquals("최신 뉴스", latest.payload.path("title").asText())
        assertEquals("hankyung", latest.source)
    }

    @Test
    fun `앞뒤 존재 여부는 타입 필터를 함께 본다`() {
        assertTrue(store.hasOlderThan("005930", "01J9Z800000000000000000002", emptyList()))
        assertFalse(store.hasOlderThan("005930", "01J9Z800000000000000000002", listOf(StreamEventType.POST)))
        assertTrue(store.hasNewerThan("005930", "01J9Z800000000000000000004", emptyList()))
        assertFalse(store.hasNewerThan("005930", "01J9Z800000000000000000005", emptyList()))
    }

    @Test
    fun `시세 캐시가 비면 최신 일봉으로 답하고 지연 표시한다`() {
        val quote = quotes.lastCandleQuote("005930")!!

        assertEquals(71200, quote.price)
        assertEquals(70500, quote.prevClose)
        assertEquals(700, quote.change)
        assertEquals(0.99, quote.changeRate)
        assertTrue(quote.delayed)
    }

    @Test
    fun `일봉이 없는 종목은 시세를 만들지 않는다`() {
        assertEquals(null, quotes.lastCandleQuote("000660"))
    }
}
