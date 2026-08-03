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
class JpaStreamInboxTest {
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
    private lateinit var inbox: StreamInbox

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM stream_event")
        listOf(
            Triple("01J9Z800000000000000000001", "005930", "NEWS"),
            Triple("01J9Z800000000000000000002", "005930", "NEWS"),
            Triple("01J9Z800000000000000000003", "005930", "AI"),
            Triple("01J9Z800000000000000000004", "000660", "NEWS"),
            Triple("01J9Z800000000000000000005", "000660", "POST"),
            Triple("01J9Z800000000000000000006", "035720", "NEWS"),
        ).forEach { (eventId, code, type) ->
            jdbc.update(
                """
                INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
                VALUES (?, ?, ?, now(), 'seed', '{"title":"제목"}'::jsonb)
                """.trimIndent(),
                eventId,
                code,
                type,
            )
        }
    }

    @Test
    fun `종목별 커서 이후만 세고 커서가 없으면 전부 센다`() {
        val counts = inbox.countUnread(
            listOf(
                UnreadWindow("005930", "01J9Z800000000000000000001"),
                UnreadWindow("000660", null),
            ),
            perCodeFetchLimit = 100,
        )

        assertEquals(mapOf("005930" to 2, "000660" to 2), counts)
    }

    @Test
    fun `종목당 조회 상한으로 비용을 고정한다`() {
        val counts = inbox.countUnread(
            listOf(UnreadWindow("005930", null)),
            perCodeFetchLimit = 2,
        )

        assertEquals(mapOf("005930" to 2), counts)
    }

    @Test
    fun `미읽음 목록은 관심 종목을 섞어 최신부터 준다`() {
        val items = inbox.findUnread(
            windows = listOf(
                UnreadWindow("005930", "01J9Z800000000000000000001"),
                UnreadWindow("000660", null),
            ),
            types = emptyList(),
            beforeEventId = null,
            limit = 10,
        )

        assertEquals(
            listOf(
                "01J9Z800000000000000000005",
                "01J9Z800000000000000000004",
                "01J9Z800000000000000000003",
                "01J9Z800000000000000000002",
            ),
            items.map(StreamItem::eventId),
        )
        assertTrue(items.none { it.code == "035720" }, "관심 밖 종목이 알림에 섞였다")
    }

    @Test
    fun `타입 필터와 페이지 커서를 함께 적용한다`() {
        val items = inbox.findUnread(
            windows = listOf(UnreadWindow("005930", null), UnreadWindow("000660", null)),
            types = listOf(StreamEventType.NEWS),
            beforeEventId = "01J9Z800000000000000000004",
            limit = 10,
        )

        assertEquals(
            listOf("01J9Z800000000000000000002", "01J9Z800000000000000000001"),
            items.map(StreamItem::eventId),
        )
    }

    @Test
    fun `더 과거와 더 최신의 미읽음 존재 여부를 판정한다`() {
        val windows = listOf(UnreadWindow("005930", "01J9Z800000000000000000001"))

        assertTrue(inbox.hasUnreadOlderThan(windows, emptyList(), "01J9Z800000000000000000003"))
        assertFalse(inbox.hasUnreadOlderThan(windows, emptyList(), "01J9Z800000000000000000002"))
        assertTrue(inbox.hasUnreadNewerThan(windows, emptyList(), "01J9Z800000000000000000002"))
        assertFalse(inbox.hasUnreadNewerThan(windows, emptyList(), "01J9Z800000000000000000003"))
    }

    @Test
    fun `종목별 최신 이벤트를 모아 준다`() {
        val latest = inbox.latestEventIds(listOf("005930", "000660", "999999"))

        assertEquals(
            mapOf(
                "005930" to "01J9Z800000000000000000003",
                "000660" to "01J9Z800000000000000000005",
            ),
            latest,
        )
    }

    @Test
    fun `빈 관심목록은 빈 결과다`() {
        assertTrue(inbox.countUnread(emptyList(), 100).isEmpty())
        assertTrue(inbox.findUnread(emptyList(), emptyList(), null, 10).isEmpty())
        assertFalse(inbox.hasUnreadOlderThan(emptyList(), emptyList(), "01J9Z800000000000000000009"))
    }
}
