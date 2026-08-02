package com.alphatalk.coreapi.notification

import com.alphatalk.contracts.Keys
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class ReadCursorIntegrationTest {
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
    private lateinit var cursorStore: ReadCursorStore

    @Autowired
    private lateinit var cursorCache: CursorCache

    @Autowired
    private lateinit var badgeCache: BadgeCache

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    private var userId = 0L

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM read_cursor")
        jdbc.update("DELETE FROM users")
        jdbc.update(
            "INSERT INTO users (email, password_hash, nickname) VALUES ('cursor@b.c', 'hash', '커서유저')",
        )
        userId = jdbc.queryForObject("SELECT id FROM users WHERE email = 'cursor@b.c'", Long::class.java)!!
        redisTemplate.connectionFactory!!.connection.use { it.serverCommands().flushDb() }
    }

    @Test
    fun `커서가 없으면 만들고 있으면 앞으로만 움직인다`() {
        assertEquals("01J9Z800000000000000000003", cursorStore.advance(userId, "005930", "01J9Z800000000000000000003"))
        assertEquals("01J9Z800000000000000000005", cursorStore.advance(userId, "005930", "01J9Z800000000000000000005"))
        assertEquals("01J9Z800000000000000000005", cursorStore.advance(userId, "005930", "01J9Z800000000000000000004"))

        assertEquals(
            mapOf("005930" to "01J9Z800000000000000000005"),
            cursorStore.find(userId, listOf("005930", "000660")),
        )
    }

    @Test
    fun `일괄 전진은 종목마다 독립적으로 역행을 무시하고 최종 커서를 돌려준다`() {
        cursorStore.advance(userId, "005930", "01J9Z800000000000000000005")

        val finalCursors = cursorStore.advanceAll(
            userId,
            mapOf(
                "005930" to "01J9Z800000000000000000002",
                "000660" to "01J9Z800000000000000000007",
            ),
        )

        assertEquals(
            mapOf(
                "005930" to "01J9Z800000000000000000005",
                "000660" to "01J9Z800000000000000000007",
            ),
            finalCursors,
        )
        assertEquals(finalCursors, cursorStore.find(userId, listOf("005930", "000660")))
    }

    @Test
    fun `Redis 커서 캐시도 앞으로만 움직인다`() {
        cursorCache.advance(userId, "005930", "01J9Z800000000000000000005")
        cursorCache.advance(userId, "005930", "01J9Z800000000000000000002")

        assertEquals(
            "01J9Z800000000000000000005",
            redisTemplate.opsForValue().get(Keys.cursor(userId, "005930")),
        )
        assertEquals(
            mapOf("005930" to "01J9Z800000000000000000005"),
            cursorCache.read(userId, listOf("005930", "000660")),
        )
    }

    @Test
    fun `동시 전진 경합에서도 커서는 최댓값으로 수렴하고 행은 하나다`() {
        val eventIds = (1..16).map { "01J9Z8000000000000000000%02d".format(it) }
        val pool = Executors.newFixedThreadPool(8)
        try {
            val ready = CyclicBarrier(8)
            val futures = eventIds.shuffled().chunked(2).map { chunk ->
                pool.submit {
                    ready.await(10, TimeUnit.SECONDS)
                    chunk.forEach { cursorStore.advance(userId, "005930", it) }
                }
            }
            futures.forEach { it.get(30, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }

        assertEquals(mapOf("005930" to eventIds.max()), cursorStore.find(userId, listOf("005930")))
        assertEquals(
            1,
            jdbc.queryForObject("SELECT count(*) FROM read_cursor WHERE user_id = ?", Long::class.java, userId),
        )
    }

    @Test
    fun `배지 캐시는 저장 후 읽히고 비우면 사라진다`() {
        val badge = BadgeResponse(total = 27, byCode = mapOf("005930" to 12, "000660" to 15))

        badgeCache.store(userId, badge)
        assertEquals(badge, badgeCache.find(userId))

        badgeCache.evict(userId)
        assertNull(badgeCache.find(userId))
    }
}
