package com.alphatalk.coreapi.stream

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class StreamEndToEndTest {
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

    private val mapper = ObjectMapper()
    private lateinit var token: String

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM stream_event")
        jdbc.update("DELETE FROM refresh_tokens")
        jdbc.update("DELETE FROM users")
        jdbc.update("DELETE FROM stock_master")
        jdbc.update(
            """
            INSERT INTO stock_master (code, name, market, shares_outstanding, is_active) VALUES
            ('005930', '삼성전자',   'KOSPI', 5846278000, true),
            ('000660', 'SK하이닉스', 'KOSPI',  712702000, true),
            ('001234', '폐지된종목', 'KOSPI',   10000000, false)
            """.trimIndent(),
        )
        (1..5).forEach { i ->
            val type = if (i == 3) "AI" else "NEWS"
            jdbc.update(
                """
                INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
                VALUES (?, '005930', ?, now(), 'hankyung', ?::jsonb)
                """.trimIndent(),
                "01J9Z80000000000000000000$i",
                type,
                """{"title":"이벤트 $i"}""",
            )
        }
        token = login()
    }

    private fun login(): String {
        post("/api/v1/auth/signup", """{"email":"a@b.c","password":"password1","nickname":"민균"}""")
        val body = post("/api/v1/auth/login", """{"email":"a@b.c","password":"password1"}""").body
        return json(body).path("accessToken").asText()
    }

    private fun json(body: String?): JsonNode = mapper.readTree(body)

    private fun post(path: String, body: String) = rest.exchange(
        path,
        HttpMethod.POST,
        HttpEntity(body, HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }),
        String::class.java,
    )

    private fun get(path: String, bearer: String? = token) = rest.exchange(
        path,
        HttpMethod.GET,
        HttpEntity<String?>(null, HttpHeaders().apply { bearer?.let { set("Authorization", "Bearer $it") } }),
        String::class.java,
    )

    @Test
    fun `FR-04 - 최신부터 커서로 과거를 훑는다`() {
        val first = json(get("/api/v1/rooms/005930/stream?limit=2").body)

        assertEquals(2, first.path("items").size())
        assertEquals("01J9Z800000000000000000005", first.path("items")[0].path("eventId").asText())
        assertEquals(true, first.path("pageInfo").path("hasMoreBefore").asBoolean())
        assertEquals(false, first.path("pageInfo").path("hasMoreAfter").asBoolean())

        val oldest = first.path("pageInfo").path("oldest").asText()
        val next = json(get("/api/v1/rooms/005930/stream?limit=2&cursor=$oldest").body)

        assertEquals("01J9Z800000000000000000003", next.path("items")[0].path("eventId").asText())
        assertEquals(true, next.path("pageInfo").path("hasMoreAfter").asBoolean())
    }

    @Test
    fun `FR-06 - 놓친 구간을 after로 오름차순 복구한다`() {
        val recovered = json(
            get("/api/v1/rooms/005930/stream?direction=after&cursor=01J9Z800000000000000000002&limit=2").body,
        )

        val ids = recovered.path("items").map { it.path("eventId").asText() }
        assertEquals(listOf("01J9Z800000000000000000003", "01J9Z800000000000000000004"), ids)
        assertEquals(true, recovered.path("pageInfo").path("hasMoreAfter").asBoolean())
    }

    @Test
    fun `FR-05 - types로 서버에서 걸러낸다`() {
        val aiOnly = json(get("/api/v1/rooms/005930/stream?types=ai").body)

        assertEquals(1, aiOnly.path("items").size())
        assertEquals("AI", aiOnly.path("items")[0].path("type").asText())
        assertEquals(false, aiOnly.path("pageInfo").path("hasMoreBefore").asBoolean())
    }

    @Test
    fun `FR-08 - 시세 캐시가 있으면 실시간으로 답한다`() {
        val redisTemplate = org.springframework.data.redis.core.StringRedisTemplate(
            org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory(
                redis.host,
                redis.getMappedPort(6379),
            ).apply { afterPropertiesSet() },
        )
        redisTemplate.opsForHash<String, String>().putAll(
            "price:005930",
            mapOf(
                "price" to "71200", "prevClose" to "70500", "change" to "700", "changeRate" to "0.99",
                "open" to "70600", "high" to "71500", "low" to "70400", "volume" to "1234567",
                "ts" to "1719500000000",
            ),
        )

        val quote = json(get("/api/v1/rooms/005930/quote").body)

        assertEquals(71200, quote.path("price").asLong())
        assertEquals(false, quote.path("delayed").asBoolean())
    }

    @Test
    fun `없는 종목 시세는 404다`() {
        val response = get("/api/v1/rooms/999999/quote")

        assertEquals(404, response.statusCode.value())
        assertEquals("NOT_FOUND", json(response.body).path("error").path("code").asText())
    }

    @Test
    fun `스트림 조회는 인증이 필요하다`() {
        val response = get("/api/v1/rooms/005930/stream", bearer = null)

        assertEquals(401, response.statusCode.value())
    }

    @Test
    fun `잘못된 커서는 400 VALIDATION_FAILED`() {
        val response = get("/api/v1/rooms/005930/stream?cursor=not-a-ulid")

        assertEquals(400, response.statusCode.value())
        val error = json(response.body).path("error")
        assertEquals("VALIDATION_FAILED", error.path("code").asText())
        assertEquals("cursor", error.path("detail").path("field").asText())
    }

    @Test
    fun `이벤트가 없는 방은 빈 목록과 빈 pageInfo를 준다`() {
        val response = json(get("/api/v1/rooms/000660/stream").body)

        assertEquals(0, response.path("items").size())
        assertTrue(response.path("pageInfo").path("oldest").isNull)
        assertEquals(false, response.path("pageInfo").path("hasMoreBefore").asBoolean())
    }

    @Test
    fun `없거나 상장폐지된 종목의 방은 404다`() {
        val unknown = get("/api/v1/rooms/999999/stream")
        val delisted = get("/api/v1/rooms/001234/stream")

        assertEquals(404, unknown.statusCode.value())
        assertEquals(404, delisted.statusCode.value())
        assertEquals("NOT_FOUND", json(unknown.body).path("error").path("code").asText())
    }
}
