package com.alphatalk.coreapi.notification

import com.alphatalk.contracts.Keys
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class NotificationEndToEndTest {
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
    private lateinit var rest: TestRestTemplate

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    private val mapper = ObjectMapper()
    private lateinit var token: String
    private var userId = 0L

    @BeforeEach
    fun seed() {
        redisTemplate.connectionFactory!!.connection.use { it.serverCommands().flushDb() }
        jdbc.update("DELETE FROM read_cursor")
        jdbc.update("DELETE FROM stream_event")
        jdbc.update("DELETE FROM watchlist")
        jdbc.update("DELETE FROM refresh_tokens")
        jdbc.update("DELETE FROM users")
        jdbc.update("DELETE FROM stock_master")
        jdbc.update(
            """
            INSERT INTO stock_master (code, name, market, shares_outstanding, is_active) VALUES
            ('005930', '삼성전자',   'KOSPI', 5846278000, true),
            ('000660', 'SK하이닉스', 'KOSPI',  712702000, true),
            ('035720', '카카오',     'KOSPI', 4432000000, true)
            """.trimIndent(),
        )
        post("/api/v1/auth/signup", """{"email":"a@b.c","password":"password1","nickname":"민균"}""")
        token = json(post("/api/v1/auth/login", """{"email":"a@b.c","password":"password1"}""").body)
            .path("accessToken").asText()
        userId = jdbc.queryForObject("SELECT id FROM users WHERE email = 'a@b.c'", Long::class.java)!!
        jdbc.update("INSERT INTO watchlist (user_id, code) VALUES (?, '005930'), (?, '000660')", userId, userId)
        listOf(
            Triple("01J9Z800000000000000000001", "005930", "NEWS"),
            Triple("01J9Z800000000000000000002", "005930", "NEWS"),
            Triple("01J9Z800000000000000000003", "005930", "AI"),
            Triple("01J9Z800000000000000000004", "000660", "NEWS"),
            Triple("01J9Z800000000000000000005", "000660", "NEWS"),
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

    private fun json(body: String?): JsonNode = mapper.readTree(body)

    private fun post(path: String, body: String? = null, bearer: String? = null) = rest.exchange(
        path,
        HttpMethod.POST,
        HttpEntity(
            body,
            HttpHeaders().apply {
                if (body != null) contentType = MediaType.APPLICATION_JSON
                bearer?.let { set("Authorization", "Bearer $it") }
            },
        ),
        String::class.java,
    )

    private fun put(path: String, body: String, bearer: String? = token) = rest.exchange(
        path,
        HttpMethod.PUT,
        HttpEntity(
            body,
            HttpHeaders().apply {
                contentType = MediaType.APPLICATION_JSON
                bearer?.let { set("Authorization", "Bearer $it") }
            },
        ),
        String::class.java,
    )

    private fun get(path: String, bearer: String? = token) = rest.exchange(
        path,
        HttpMethod.GET,
        HttpEntity<String?>(null, HttpHeaders().apply { bearer?.let { set("Authorization", "Bearer $it") } }),
        String::class.java,
    )

    @Test
    fun `FR-12 - 배지는 관심 종목의 미읽음만 센다`() {
        val badge = json(get("/api/v1/notifications/badge").body)

        assertEquals(5, badge.path("total").asInt())
        assertEquals(3, badge.path("byCode").path("005930").asInt())
        assertEquals(2, badge.path("byCode").path("000660").asInt())
        assertTrue(badge.path("byCode").path("035720").isMissingNode)
    }

    @Test
    fun `FR-13 - 커서를 전진하면 그 이후만 미읽음이고 역행은 무시한다`() {
        val advanced = put(
            "/api/v1/rooms/005930/cursor",
            """{"lastEventId":"01J9Z800000000000000000002"}""",
        )
        assertEquals(204, advanced.statusCode.value())

        val badge = json(get("/api/v1/notifications/badge").body)
        assertEquals(3, badge.path("total").asInt())
        assertEquals(1, badge.path("byCode").path("005930").asInt())

        val regressed = put(
            "/api/v1/rooms/005930/cursor",
            """{"lastEventId":"01J9Z800000000000000000001"}""",
        )
        assertEquals(204, regressed.statusCode.value())

        assertEquals(
            "01J9Z800000000000000000002",
            jdbc.queryForObject(
                "SELECT last_event_id FROM read_cursor WHERE user_id = ? AND code = '005930'",
                String::class.java,
                userId,
            ),
        )
        assertEquals(
            "01J9Z800000000000000000002",
            redisTemplate.opsForValue().get(Keys.cursor(userId, "005930")),
        )
    }

    @Test
    fun `알림 목록은 미읽음을 최신순으로 섞고 타입을 거른다`() {
        put("/api/v1/rooms/005930/cursor", """{"lastEventId":"01J9Z800000000000000000002"}""")

        val page = json(get("/api/v1/notifications").body)
        assertEquals(
            listOf(
                "01J9Z800000000000000000005",
                "01J9Z800000000000000000004",
                "01J9Z800000000000000000003",
            ),
            page.path("items").map { it.path("eventId").asText() },
        )
        assertEquals(false, page.path("pageInfo").path("hasMoreBefore").asBoolean())

        val newsOnly = json(get("/api/v1/notifications?types=news").body)
        assertTrue(newsOnly.path("items").all { it.path("type").asText() == "NEWS" })
    }

    @Test
    fun `모두 읽음 뒤에는 배지가 0이고 알림이 비어 있다`() {
        assertEquals(5, json(get("/api/v1/notifications/badge").body).path("total").asInt())

        val readAll = post("/api/v1/notifications/read-all", bearer = token)
        assertEquals(204, readAll.statusCode.value())

        assertEquals(0, json(get("/api/v1/notifications/badge").body).path("total").asInt())
        assertEquals(0, json(get("/api/v1/notifications").body).path("items").size())
    }

    @Test
    fun `없는 종목의 커서 전진은 404다`() {
        val response = put("/api/v1/rooms/999999/cursor", """{"lastEventId":"01J9Z800000000000000000002"}""")

        assertEquals(404, response.statusCode.value())
        assertEquals("NOT_FOUND", json(response.body).path("error").path("code").asText())
    }

    @Test
    fun `알림 API는 인증이 필요하다`() {
        assertEquals(401, get("/api/v1/notifications/badge", bearer = null).statusCode.value())
        assertEquals(401, get("/api/v1/notifications", bearer = null).statusCode.value())
    }
}
