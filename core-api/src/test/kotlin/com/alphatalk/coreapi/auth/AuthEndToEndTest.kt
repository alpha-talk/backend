package com.alphatalk.coreapi.auth

import com.alphatalk.auth.TokenIssuer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class AuthEndToEndTest {
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
    private lateinit var issuer: TokenIssuer

    private val mapper = ObjectMapper()

    @BeforeEach
    fun clean() {
        jdbc.update("DELETE FROM refresh_tokens")
        jdbc.update("DELETE FROM users")
    }

    private fun post(path: String, body: String, token: String? = null) =
        rest.exchange(path, HttpMethod.POST, entity(body, token), String::class.java)

    private fun get(path: String, token: String? = null) =
        rest.exchange(path, HttpMethod.GET, entity(null, token), String::class.java)

    private fun entity(body: String?, token: String?): HttpEntity<String?> {
        val headers = HttpHeaders().apply {
            contentType = MediaType.APPLICATION_JSON
            token?.let { set("Authorization", "Bearer $it") }
        }
        return HttpEntity(body, headers)
    }

    private fun json(body: String?): JsonNode = mapper.readTree(body)

    private fun signup(email: String = "a@b.c", nickname: String = "민균") =
        post("/api/v1/auth/signup", """{"email":"$email","password":"password1","nickname":"$nickname"}""")

    private fun login(email: String = "a@b.c"): JsonNode =
        json(post("/api/v1/auth/login", """{"email":"$email","password":"password1"}""").body)

    @Test
    fun `FR-01 DoD - 가입 로그인 보호 API 접근 토큰 재발급`() {
        val created = signup()
        assertEquals(201, created.statusCode.value())
        assertTrue(json(created.body).path("userId").asLong() > 0)

        val tokens = login()
        val access = tokens.path("accessToken").asText()
        assertEquals(1800, tokens.path("accessExpiresIn").asInt())

        val me = get("/api/v1/users/me", access)
        assertEquals(200, me.statusCode.value())
        assertEquals("a@b.c", json(me.body).path("email").asText())
        assertEquals("민균", json(me.body).path("nickname").asText())

        val refreshed = json(post("/api/v1/auth/refresh", """{"refreshToken":"${tokens.path("refreshToken").asText()}"}""").body)
        assertNotEquals(tokens.path("refreshToken").asText(), refreshed.path("refreshToken").asText())

        val meAgain = get("/api/v1/users/me", refreshed.path("accessToken").asText())
        assertEquals(200, meAgain.statusCode.value())
    }

    @Test
    fun `토큰 없이 보호 API를 부르면 401 UNAUTHORIZED`() {
        val response = get("/api/v1/users/me")

        assertEquals(401, response.statusCode.value())
        assertEquals("UNAUTHORIZED", json(response.body).path("error").path("code").asText())
    }

    @Test
    fun `만료된 액세스 토큰은 TOKEN_EXPIRED로 구분된다`() {
        signup()
        val expired = com.alphatalk.auth.JwtTokenProvider(
            "insecure-local-dev-only-secret-0123456789abcdef",
            java.time.Clock.fixed(java.time.Instant.now().minus(Duration.ofHours(2)), java.time.ZoneOffset.UTC),
        ).issue(1L, Duration.ofMinutes(30))

        val response = get("/api/v1/users/me", expired)

        assertEquals(401, response.statusCode.value())
        assertEquals("TOKEN_EXPIRED", json(response.body).path("error").path("code").asText())
    }

    @Test
    fun `중복 이메일 가입은 409 DUPLICATE`() {
        signup()

        val response = signup(nickname = "다른닉")

        assertEquals(409, response.statusCode.value())
        assertEquals("DUPLICATE", json(response.body).path("error").path("code").asText())
    }

    @Test
    fun `약한 비밀번호는 400 VALIDATION_FAILED`() {
        val response = post("/api/v1/auth/signup", """{"email":"a@b.c","password":"short","nickname":"민균"}""")

        assertEquals(400, response.statusCode.value())
        val error = json(response.body).path("error")
        assertEquals("VALIDATION_FAILED", error.path("code").asText())
        assertEquals("password", error.path("detail").path("field").asText())
    }

    @Test
    fun `로그인 실패는 401이고 계정 존재 여부를 흘리지 않는다`() {
        signup()

        val wrongPassword = post("/api/v1/auth/login", """{"email":"a@b.c","password":"wrongpass1"}""")
        val noSuchUser = post("/api/v1/auth/login", """{"email":"nobody@b.c","password":"password1"}""")

        assertEquals(401, wrongPassword.statusCode.value())
        assertEquals(401, noSuchUser.statusCode.value())
        assertEquals(
            json(wrongPassword.body).path("error").path("message").asText(),
            json(noSuchUser.body).path("error").path("message").asText(),
        )
    }

    @Test
    fun `쓴 리프레시를 재사용하면 그 계정 세션이 전부 끊긴다`() {
        signup()
        val first = login()
        val second = json(post("/api/v1/auth/refresh", """{"refreshToken":"${first.path("refreshToken").asText()}"}""").body)

        val reuse = post("/api/v1/auth/refresh", """{"refreshToken":"${first.path("refreshToken").asText()}"}""")
        assertEquals(401, reuse.statusCode.value())

        val afterBreach = post("/api/v1/auth/refresh", """{"refreshToken":"${second.path("refreshToken").asText()}"}""")
        assertEquals(401, afterBreach.statusCode.value())
    }

    @Test
    fun `로그아웃하면 재발급이 막힌다`() {
        signup()
        val tokens = login()

        val logout = post("/api/v1/auth/logout", "", tokens.path("accessToken").asText())
        assertEquals(204, logout.statusCode.value())

        val refresh = post("/api/v1/auth/refresh", """{"refreshToken":"${tokens.path("refreshToken").asText()}"}""")
        assertEquals(401, refresh.statusCode.value())
    }

    @Test
    fun `리프레시 토큰 원문은 저장되지 않는다`() {
        signup()
        val tokens = login()

        val stored = jdbc.queryForList("SELECT token_hash FROM refresh_tokens", String::class.java)

        assertEquals(1, stored.size)
        assertNotEquals(tokens.path("refreshToken").asText(), stored.first())
        assertEquals(64, stored.first().length)
    }
}
