package com.alphatalk.coreapi.stream

import com.fasterxml.jackson.databind.ObjectMapper
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Testcontainers(disabledWithoutDocker = true)
class StreamStoreIntegrationTest {
    companion object {
        private const val MASTER_CHANGELOG = "db/changelog/db.changelog-master.yaml"

        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )

        private val plain by lazy {
            JdbcTemplate(DriverManagerDataSource(postgres.jdbcUrl, postgres.username, postgres.password))
        }

        @Suppress("DEPRECATION")
        @BeforeAll
        @JvmStatic
        fun migrateAndSeed() {
            DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { connection ->
                val database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(JdbcConnection(connection))
                Liquibase(MASTER_CHANGELOG, ClassLoaderResourceAccessor(), database)
                    .update(Contexts(), LabelExpression())
            }
            listOf(
                Triple("01J9Z800000000000000000001", "NEWS", "첫 뉴스"),
                Triple("01J9Z800000000000000000002", "NEWS", "둘째 뉴스"),
                Triple("01J9Z800000000000000000003", "AI", "브리핑"),
                Triple("01J9Z800000000000000000004", "POST", "유저 글"),
                Triple("01J9Z800000000000000000005", "NEWS", "최신 뉴스"),
            ).forEach { (id, type, title) ->
                plain.update(
                    """
                    INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
                    VALUES (?, '005930', ?, now(), 'hankyung', ?::jsonb)
                    """.trimIndent(),
                    id,
                    type,
                    """{"title":"$title"}""",
                )
            }
            plain.update(
                """
                INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
                VALUES ('01J9Z800000000000000000009', '000660', 'NEWS', now(), 'hankyung', '{"title":"다른 종목"}'::jsonb)
                """.trimIndent(),
            )
            plain.update(
                """
                INSERT INTO daily_candle (code, date, open, high, low, close, volume, value) VALUES
                ('005930', '20260727', 70000, 71000, 69800, 70500, 1000, 70000000),
                ('005930', '20260728', 70600, 71500, 70400, 71200, 2000, 142000000)
                """.trimIndent(),
            )
        }
    }

    private val store by lazy { JdbcStreamStore(NamedParameterJdbcTemplate(plain), ObjectMapper()) }

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
        assertEquals(
            false,
            store.hasOlderThan("005930", "01J9Z800000000000000000002", listOf(StreamEventType.POST)),
        )
        assertTrue(store.hasNewerThan("005930", "01J9Z800000000000000000004", emptyList()))
        assertEquals(
            false,
            store.hasNewerThan("005930", "01J9Z800000000000000000005", emptyList()),
        )
    }

    @Test
    fun `시세 캐시가 비면 최신 일봉으로 답하고 지연 표시한다`() {
        val quotes = RedisQuoteStore(
            redis = org.springframework.data.redis.core.StringRedisTemplate(
                org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory("localhost", 1).apply {
                    afterPropertiesSet()
                },
            ),
            jdbc = plain,
        )

        val quote = quotes.lastCandleQuote("005930")!!

        assertEquals(71200, quote.price)
        assertEquals(70500, quote.prevClose)
        assertEquals(700, quote.change)
        assertEquals(0.99, quote.changeRate)
        assertEquals(true, quote.delayed)
    }
}
