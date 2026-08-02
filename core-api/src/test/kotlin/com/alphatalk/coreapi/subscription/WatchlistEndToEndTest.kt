package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
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
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class WatchlistEndToEndTest {
    companion object {
        private const val PROBE = "probe"

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

    @Autowired
    private lateinit var connectionFactory: RedisConnectionFactory

    private val mapper = ObjectMapper()
    private val published = LinkedBlockingQueue<String>()
    private lateinit var listener: RedisMessageListenerContainer
    private lateinit var token: String
    private var userId = 0L

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM watchlist")
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
        redisTemplate.keys("watchlist:*").orEmpty().takeIf { it.isNotEmpty() }?.let(redisTemplate::delete)

        post("/api/v1/auth/signup", """{"email":"a@b.c","password":"password1","nickname":"민균"}""")
        val login = post("/api/v1/auth/login", """{"email":"a@b.c","password":"password1"}""")
        token = json(login.body).path("accessToken").asText()
        userId = jdbc.queryForObject("SELECT id FROM users WHERE email = 'a@b.c'", Long::class.java)!!

        startListening()
    }

    @AfterEach
    fun stopListening() {
        listener.stop()
        listener.destroy()
    }

    private fun startListening() {
        listener = RedisMessageListenerContainer()
        listener.setConnectionFactory(connectionFactory)
        listener.afterPropertiesSet()
        listener.start()
        listener.addMessageListener(
            { message, _ -> published += String(message.body) },
            ChannelTopic(Channels.WATCHLIST_UPDATED),
        )
        awaitSubscription()
    }

    private fun awaitSubscription() {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (System.nanoTime() < deadline) {
            redisTemplate.convertAndSend(Channels.WATCHLIST_UPDATED, PROBE)
            if (published.poll(200, TimeUnit.MILLISECONDS) != null) {
                published.clear()
                return
            }
        }
        throw IllegalStateException("watchlist:updated 구독이 시작되지 않았다")
    }

    private fun nextEvent(): JsonNode? = published.poll(3, TimeUnit.SECONDS)?.let(mapper::readTree)

    private fun json(body: String?): JsonNode = mapper.readTree(body)

    private fun post(path: String, body: String) = rest.exchange(
        path,
        HttpMethod.POST,
        HttpEntity(body, HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }),
        String::class.java,
    )

    private fun call(method: HttpMethod, path: String, bearer: String? = token) = rest.exchange(
        path,
        method,
        HttpEntity<String?>(null, HttpHeaders().apply { bearer?.let { set("Authorization", "Bearer $it") } }),
        String::class.java,
    )

    private fun mirroredCodes(): Set<String> = redisTemplate.opsForSet().members(Keys.watchlist(userId)).orEmpty()

    @Test
    fun `FR-03 - 종목을 담으면 목록과 게이트웨이 미러에 함께 반영된다`() {
        val subscribe = call(HttpMethod.PUT, "/api/v1/watchlist/005930")
        assertEquals(201, subscribe.statusCode.value())

        val list = call(HttpMethod.GET, "/api/v1/watchlist")
        assertEquals(200, list.statusCode.value())
        val item = json(list.body).path("items").single()
        assertEquals("005930", item.path("code").asText())
        assertEquals("삼성전자", item.path("name").asText())
        assertEquals("KOSPI", item.path("market").asText())
        assertTrue(item.path("subscribedAt").asLong() > 0)

        assertEquals(setOf("005930"), mirroredCodes())

        val event = assertNotNull(nextEvent(), "watchlist:updated가 발행되지 않았다")
        assertEquals(userId, event.path("userId").asLong())
        assertEquals(listOf("005930"), event.path("added").map { it.asText() })
        assertTrue(event.path("removed").isEmpty)
        assertTrue(event.path("ts").asLong() > 0)
    }

    @Test
    fun `같은 종목을 다시 담으면 200이고 복구 이벤트를 다시 발행한다`() {
        call(HttpMethod.PUT, "/api/v1/watchlist/005930")
        assertNotNull(nextEvent())

        val again = call(HttpMethod.PUT, "/api/v1/watchlist/005930")

        assertEquals(200, again.statusCode.value())
        val event = assertNotNull(nextEvent(), "재요청 복구 이벤트가 발행되지 않았다")
        assertEquals(listOf("005930"), event.path("added").map { it.asText() })
        assertEquals(1, json(call(HttpMethod.GET, "/api/v1/watchlist").body).path("items").size())
    }

    @Test
    fun `미러만 어긋나면 재요청 한 번으로 맞춰진다`() {
        call(HttpMethod.PUT, "/api/v1/watchlist/005930")
        redisTemplate.delete(Keys.watchlist(userId))
        published.clear()

        val again = call(HttpMethod.PUT, "/api/v1/watchlist/005930")

        assertEquals(200, again.statusCode.value())
        assertEquals(setOf("005930"), mirroredCodes(), "미러가 복구되지 않았다")
        val event = assertNotNull(nextEvent(), "접속 중인 게이트웨이의 복구 이벤트가 발행되지 않았다")
        assertEquals(listOf("005930"), event.path("added").map { it.asText() })
    }

    @Test
    fun `해지하면 미러에서도 빠지고 removed로 알린다`() {
        call(HttpMethod.PUT, "/api/v1/watchlist/005930")
        call(HttpMethod.PUT, "/api/v1/watchlist/000660")
        published.clear()

        val delete = call(HttpMethod.DELETE, "/api/v1/watchlist/005930")

        assertEquals(204, delete.statusCode.value())
        assertEquals(setOf("000660"), mirroredCodes())
        val event = assertNotNull(nextEvent(), "watchlist:updated가 발행되지 않았다")
        assertEquals(listOf("005930"), event.path("removed").map { it.asText() })
        assertTrue(event.path("added").isEmpty)
    }

    @Test
    fun `담지 않은 종목을 해지해도 204이고 복구 이벤트를 발행한다`() {
        val delete = call(HttpMethod.DELETE, "/api/v1/watchlist/000660")

        assertEquals(204, delete.statusCode.value())
        val event = assertNotNull(nextEvent(), "해지 재요청 복구 이벤트가 발행되지 않았다")
        assertEquals(listOf("000660"), event.path("removed").map { it.asText() })
    }

    @Test
    fun `같은 종목의 구독과 해지가 겹쳐도 DB와 미러가 같은 상태로 끝난다`() {
        val pool = Executors.newFixedThreadPool(2)
        try {
            repeat(10) { attempt ->
                call(HttpMethod.DELETE, "/api/v1/watchlist/005930")
                val barrier = CyclicBarrier(2)
                val requests = listOf(HttpMethod.PUT, HttpMethod.DELETE).map { method ->
                    pool.submit {
                        barrier.await(10, TimeUnit.SECONDS)
                        call(method, "/api/v1/watchlist/005930")
                    }
                }
                requests.forEach { it.get(30, TimeUnit.SECONDS) }

                val stored = jdbc.queryForObject(
                    "SELECT EXISTS(SELECT 1 FROM watchlist WHERE user_id = ? AND code = '005930')",
                    Boolean::class.java,
                    userId,
                )!!
                assertEquals(stored, "005930" in mirroredCodes(), "시도 ${attempt + 1}에서 상태가 어긋났다")
            }
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `없는 종목과 상장폐지 종목은 404다`() {
        assertEquals(404, call(HttpMethod.PUT, "/api/v1/watchlist/999999").statusCode.value())

        val delisted = call(HttpMethod.PUT, "/api/v1/watchlist/001234")
        assertEquals(404, delisted.statusCode.value())
        assertEquals("NOT_FOUND", json(delisted.body).path("error").path("code").asText())
    }

    @Test
    fun `관심목록은 인증이 필요하다`() {
        assertEquals(401, call(HttpMethod.GET, "/api/v1/watchlist", bearer = null).statusCode.value())
        assertEquals(401, call(HttpMethod.PUT, "/api/v1/watchlist/005930", bearer = null).statusCode.value())
    }

    @Test
    fun `한도를 넘기면 422 LIMIT_EXCEEDED`() {
        jdbc.update(
            "INSERT INTO watchlist (user_id, code) SELECT ?, lpad(g::text, 6, '0') FROM generate_series(1, ?) g",
            userId,
            WatchlistService.MAX_ITEMS,
        )

        val response = call(HttpMethod.PUT, "/api/v1/watchlist/005930")

        assertEquals(422, response.statusCode.value())
        assertEquals("LIMIT_EXCEEDED", json(response.body).path("error").path("code").asText())
    }
}
