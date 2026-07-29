package com.alphatalk.coreapi

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
class M1EndToEndTest {
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

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM refresh_tokens")
        jdbc.update("DELETE FROM users")
        jdbc.update("DELETE FROM stock_master")
        jdbc.update(
            """
            INSERT INTO stock_master (code, name, market, shares_outstanding, is_active) VALUES
            ('005930', '삼성전자',   'KOSPI', 5846278000, true),
            ('005935', '삼성전자우', 'KOSPI',  822886000, true),
            ('000660', 'SK하이닉스', 'KOSPI',  712702000, true)
            """.trimIndent(),
        )
    }

    private fun json(body: String?): JsonNode = mapper.readTree(body)

    private fun post(path: String, body: String) = rest.exchange(
        path,
        HttpMethod.POST,
        HttpEntity(body, HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }),
        String::class.java,
    )

    private fun get(path: String, token: String?) = rest.exchange(
        path,
        HttpMethod.GET,
        HttpEntity<String?>(
            null,
            HttpHeaders().apply { token?.let { set("Authorization", "Bearer $it") } },
        ),
        String::class.java,
    )

    @Test
    fun `M1 DoD - 가입에서 로그인을 거쳐 종목 검색까지`() {
        val signup = post(
            "/api/v1/auth/signup",
            """{"email":"a@b.c","password":"password1","nickname":"민균"}""",
        )
        assertEquals(201, signup.statusCode.value())

        val login = post("/api/v1/auth/login", """{"email":"a@b.c","password":"password1"}""")
        assertEquals(200, login.statusCode.value())
        val accessToken = json(login.body).path("accessToken").asText()

        val search = get("/api/v1/stocks/search?q=삼성", accessToken)

        assertEquals(200, search.statusCode.value())
        val items = json(search.body).path("items")
        assertEquals(2, items.size())
        assertEquals("005930", items[0].path("code").asText())
        assertEquals("삼성전자", items[0].path("name").asText())
        assertEquals("KOSPI", items[0].path("market").asText())
    }

    @Test
    fun `FR-02 - 두 글자로 상위 10건 안에서 응답한다`() {
        val token = json(
            post("/api/v1/auth/signup", """{"email":"a@b.c","password":"password1","nickname":"민균"}""")
                .let { post("/api/v1/auth/login", """{"email":"a@b.c","password":"password1"}""") }
                .body,
        ).path("accessToken").asText()

        val startedAt = System.nanoTime()
        val response = get("/api/v1/stocks/search?q=삼성", token)
        val elapsedMs = (System.nanoTime() - startedAt) / 1_000_000

        assertEquals(200, response.statusCode.value())
        assertTrue(json(response.body).path("items").size() <= 10)
        assertTrue(elapsedMs < 300, "검색 응답이 느리다: ${elapsedMs}ms")
    }

    @Test
    fun `검색은 인증이 필요하다`() {
        val response = get("/api/v1/stocks/search?q=삼성", null)

        assertEquals(401, response.statusCode.value())
        assertEquals("UNAUTHORIZED", json(response.body).path("error").path("code").asText())
    }

    @Test
    fun `검색어가 없으면 400 VALIDATION_FAILED`() {
        val token = json(
            post("/api/v1/auth/signup", """{"email":"a@b.c","password":"password1","nickname":"민균"}""")
                .let { post("/api/v1/auth/login", """{"email":"a@b.c","password":"password1"}""") }
                .body,
        ).path("accessToken").asText()

        val response = get("/api/v1/stocks/search", token)

        assertEquals(400, response.statusCode.value())
        val error = json(response.body).path("error")
        assertEquals("VALIDATION_FAILED", error.path("code").asText())
        assertEquals("q", error.path("detail").path("field").asText())
    }
}
