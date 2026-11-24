package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.WatchlistSnapshots
import com.alphatalk.contracts.envelope.WatchlistUpdated
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
import java.util.UUID
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["ALPHATALK_WATCHLIST_INTERNAL_KEY=watchlist-test-key"],
)
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

    @Autowired
    private lateinit var command: WatchlistCommand

    @Autowired
    private lateinit var broadcaster: RedisWatchlistBroadcaster

    @Autowired
    private lateinit var revisions: WatchlistRevisionJpaRepository

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

        val email = "${UUID.randomUUID()}@example.test"
        val signup = post("/api/v1/auth/signup", """{"email":"$email","password":"password1","nickname":"민균"}""")
        assertEquals(201, signup.statusCode.value())
        val login = post("/api/v1/auth/login", """{"email":"$email","password":"password1"}""")
        assertEquals(200, login.statusCode.value())
        token = json(login.body).path("accessToken").asText()
        userId = jdbc.queryForObject("SELECT id FROM users WHERE email = ?", Long::class.java, email)!!

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

    private fun eventAtRevision(rev: Long): JsonNode? {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
        while (true) {
            val remaining = deadline - System.nanoTime()
            if (remaining <= 0) return null
            val payload = published.poll(remaining, TimeUnit.NANOSECONDS) ?: return null
            val event = json(payload)
            if (event.path("userId").asLong() == userId && event.path("rev").asLong() == rev) return event
        }
    }

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

    private fun snapshots(key: String? = "watchlist-test-key", ids: String = "$userId") = rest.exchange(
        WatchlistSnapshots.PATH,
        HttpMethod.POST,
        HttpEntity("""{"userIds":[$ids]}""", HttpHeaders().apply {
            contentType = MediaType.APPLICATION_JSON
            key?.let { set(WatchlistSnapshots.KEY_HEADER, it) }
        }),
        String::class.java,
    )

    @Test
    fun `FR-03 - 종목을 담으면 목록과 발행된 스냅샷에 반영된다`() {
        val subscribe = call(HttpMethod.PUT, "/api/v1/watchlist/005930")
        assertEquals(201, subscribe.statusCode.value())

        val list = call(HttpMethod.GET, "/api/v1/watchlist")
        assertEquals(200, list.statusCode.value())
        val item = json(list.body).path("items").single()
        assertEquals("005930", item.path("code").asText())
        assertEquals("삼성전자", item.path("name").asText())
        assertEquals("KOSPI", item.path("market").asText())
        assertTrue(item.path("subscribedAt").asLong() > 0)


        val event = assertNotNull(nextEvent(), "watchlist:updated가 발행되지 않았다")
        assertEquals(userId, event.path("userId").asLong())
        assertEquals(listOf("005930"), event.path("codes").map { it.asText() })
        assertEquals(1L, event.path("rev").asLong())
        assertEquals(event, json(redisTemplate.opsForValue().get(Keys.watchlistSnapshot(userId))))
        assertEquals(event, json(snapshots().body).single())
    }

    @Test
    fun `같은 종목을 다시 담으면 200이고 복구 이벤트를 다시 발행한다`() {
        call(HttpMethod.PUT, "/api/v1/watchlist/005930")
        assertNotNull(nextEvent())

        val again = call(HttpMethod.PUT, "/api/v1/watchlist/005930")

        assertEquals(200, again.statusCode.value())
        val event = assertNotNull(eventAtRevision(2), "재요청 복구 이벤트가 발행되지 않았다")
        assertEquals(listOf("005930"), event.path("codes").map { it.asText() })
        assertEquals(1, json(call(HttpMethod.GET, "/api/v1/watchlist").body).path("items").size())
    }

    @Test
    fun `내부 조회는 인증과 배치 한도를 확인하고 빈 목록을 반환한다`() {
        assertEquals(401, snapshots(key = null).statusCode.value())
        assertEquals(401, snapshots(key = "wrong").statusCode.value())
        assertEquals(400, snapshots(ids = "0").statusCode.value())
        assertEquals(400, snapshots(ids = (1..101).joinToString(",")).statusCode.value())
        val result = json(snapshots(ids = "$userId,$userId").body).single()
        assertEquals(0L, result.path("rev").asLong())
        assertTrue(result.path("codes").isArray && result.path("codes").isEmpty)
        val key = Keys.watchlistSnapshot(userId)
        assertEquals(result, json(redisTemplate.opsForValue().get(key)))
        assertTrue(redisTemplate.getExpire(key, TimeUnit.SECONDS) in 1L..359L)
    }

    @Test
    fun `Redis 키 유실 시 기존 관심목록을 원본에서 복원한다`() {
        assertEquals(201, call(HttpMethod.PUT, "/api/v1/watchlist/005930").statusCode.value())
        val key = Keys.watchlistSnapshot(userId)
        redisTemplate.delete(key)

        val result = json(snapshots().body).single()

        assertEquals(listOf("005930"), result.path("codes").map { it.asText() })
        assertEquals(result, json(redisTemplate.opsForValue().get(key)))
    }

    @Test
    fun `커밋 직후 누락된 Redis 변경을 복구하고 늦은 버전을 거부한다`() {
        val committed = command.subscribe(userId, "005930", 100)
        val older = WatchlistUpdated(userId, 0, emptyList())

        broadcaster.retryPending()
        broadcaster.publish(older)

        val expectedRev = requireNotNull(committed.state).rev
        val cached = json(redisTemplate.opsForValue().get(Keys.watchlistSnapshot(userId)))
        assertEquals(expectedRev, cached.path("rev").asLong())
        assertEquals(listOf("005930"), cached.path("codes").map { it.asText() })
        assertEquals(cached, nextEvent())
        assertEquals(null, revisions.findById(userId).orElseThrow().redisPendingSince)
    }

    @Test
    fun `재처리는 Redis에 잘못된 자료형으로 저장된 키를 복구한다`() {
        val key = Keys.watchlistSnapshot(userId)
        redisTemplate.opsForSet().add(key, "005930")
        val committed = command.subscribe(userId, "000660", 100)

        broadcaster.retryPending()

        val cached = json(redisTemplate.opsForValue().get(key))
        assertEquals(requireNotNull(committed.state).rev, cached.path("rev").asLong())
        assertEquals(listOf("000660"), cached.path("codes").map { it.asText() })
        assertTrue(redisTemplate.getExpire(key, TimeUnit.SECONDS) in 1L..359L)
        assertEquals(null, revisions.findById(userId).orElseThrow().redisPendingSince)
    }

    @Test
    fun `해지하면 남은 종목 전체를 발행한다`() {
        call(HttpMethod.PUT, "/api/v1/watchlist/005930")
        call(HttpMethod.PUT, "/api/v1/watchlist/000660")
        published.clear()

        val delete = call(HttpMethod.DELETE, "/api/v1/watchlist/005930")

        assertEquals(204, delete.statusCode.value())
        val event = assertNotNull(eventAtRevision(3), "watchlist:updated가 발행되지 않았다")
        assertEquals(listOf("000660"), event.path("codes").map { it.asText() })
    }

    @Test
    fun `담지 않은 종목을 해지해도 204이고 복구 이벤트를 발행한다`() {
        val delete = call(HttpMethod.DELETE, "/api/v1/watchlist/000660")

        assertEquals(204, delete.statusCode.value())
        val event = assertNotNull(nextEvent(), "해지 재요청 복구 이벤트가 발행되지 않았다")
        assertTrue(event.path("codes").isEmpty)
    }

    @Test
    fun `같은 종목의 구독과 해지가 겹쳐도 DB와 Redis가 같은 상태로 끝난다`() {
        val pool = Executors.newFixedThreadPool(2)
        try {
            repeat(10) { round ->
                assertEquals(204, call(HttpMethod.DELETE, "/api/v1/watchlist/005930").statusCode.value())
                val barrier = CyclicBarrier(2)
                val requests = listOf(HttpMethod.PUT, HttpMethod.DELETE).map { method ->
                    pool.submit {
                        barrier.await(10, TimeUnit.SECONDS)
                        val status = call(method, "/api/v1/watchlist/005930").statusCode.value()
                        assertTrue(status in if (method == HttpMethod.PUT) listOf(200, 201) else listOf(204))
                    }
                }
                requests.forEach { it.get(30, TimeUnit.SECONDS) }

                val cached = json(redisTemplate.opsForValue().get(Keys.watchlistSnapshot(userId)))
                val persisted = json(snapshots().body).single()
                assertEquals(3L * (round + 1), persisted.path("rev").asLong())
                assertEquals(persisted, cached)
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
