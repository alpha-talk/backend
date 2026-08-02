package com.alphatalk.coreapi.subscription

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class TransactionalWatchlistCommandTest {
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
    private lateinit var command: WatchlistCommand

    @Autowired
    private lateinit var store: WatchlistStore

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    private var userId = 0L

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM watchlist")
        jdbc.update("DELETE FROM watchlist_rev")
        jdbc.update("DELETE FROM users")
        jdbc.update("DELETE FROM stock_master")
        jdbc.update(
            """
            INSERT INTO stock_master (code, name, market, shares_outstanding, is_active) VALUES
            ('005930', '삼성전자',     'KOSPI',  5846278000, true),
            ('000660', 'SK하이닉스',   'KOSPI',   712702000, true),
            ('000440', '중앙에너비스', 'KOSDAQ',    6227000, true),
            ('001234', '폐지된종목',   'KOSPI',    10000000, false)
            """.trimIndent(),
        )
        userId = jdbc.queryForObject(
            "INSERT INTO users (email, password_hash, nickname) VALUES ('a@b.c', 'x', '민균') RETURNING id",
            Long::class.java,
        )!!
    }

    @Test
    fun `담은 종목을 종목명과 함께 돌려준다`() {
        assertEquals(SubscribeOutcome.ADDED, command.subscribe(userId, "005930", 100).outcome)

        val item = store.list(userId).single()
        assertEquals("005930", item.code)
        assertEquals("삼성전자", item.name)
        assertEquals("KOSPI", item.market)
        assertTrue(item.subscribedAt > 0)
    }

    @Test
    fun `최근에 담은 종목이 앞에 온다`() {
        command.subscribe(userId, "005930", 100)
        Thread.sleep(5)
        command.subscribe(userId, "000660", 100)

        assertEquals(listOf("000660", "005930"), store.list(userId).map { it.code })
    }

    @Test
    fun `같은 종목을 다시 담으면 이미 담긴 것으로 알린다`() {
        command.subscribe(userId, "005930", 100)

        assertEquals(SubscribeOutcome.ALREADY_SUBSCRIBED, command.subscribe(userId, "005930", 100).outcome)
        assertEquals(1, store.list(userId).size)
    }

    @Test
    fun `없거나 상장폐지된 종목은 담기지 않는다`() {
        val unknown = command.subscribe(userId, "999999", 100)
        val delisted = command.subscribe(userId, "001234", 100)

        assertEquals(SubscribeOutcome.UNKNOWN_STOCK, unknown.outcome)
        assertEquals(SubscribeOutcome.UNKNOWN_STOCK, delisted.outcome)
        assertNull(unknown.state)
        assertTrue(store.list(userId).isEmpty())
    }

    @Test
    fun `이미 담은 종목이 상장폐지돼도 재요청은 통과한다`() {
        command.subscribe(userId, "005930", 100)
        jdbc.update("UPDATE stock_master SET is_active = false WHERE code = '005930'")

        assertEquals(SubscribeOutcome.ALREADY_SUBSCRIBED, command.subscribe(userId, "005930", 100).outcome)
    }

    @Test
    fun `한도를 채우면 더 담지 못한다`() {
        command.subscribe(userId, "005930", 2)
        command.subscribe(userId, "000660", 2)

        val exceeded = command.subscribe(userId, "000440", 2)

        assertEquals(SubscribeOutcome.LIMIT_EXCEEDED, exceeded.outcome)
        assertNull(exceeded.state)
        assertEquals(2, store.list(userId).size)
    }

    @Test
    fun `한도가 찬 뒤에도 이미 담긴 종목 재요청은 통과한다`() {
        command.subscribe(userId, "005930", 1)

        assertEquals(SubscribeOutcome.ALREADY_SUBSCRIBED, command.subscribe(userId, "005930", 1).outcome)
    }

    @Test
    fun `해지하면 목록에서 빠진다`() {
        command.subscribe(userId, "005930", 100)

        assertEquals(UnsubscribeOutcome.REMOVED, command.unsubscribe(userId, "005930").outcome)
        assertEquals(UnsubscribeOutcome.ALREADY_REMOVED, command.unsubscribe(userId, "005930").outcome)
        assertTrue(store.list(userId).isEmpty())
    }

    @Test
    fun `사용자가 사라진 뒤 구독과 해지는 인증 실패 결과를 준다`() {
        jdbc.update("DELETE FROM users WHERE id = ?", userId)

        val put = command.subscribe(userId, "005930", 100)
        val delete = command.unsubscribe(userId, "005930")

        assertEquals(SubscribeOutcome.OWNER_MISSING, put.outcome)
        assertEquals(UnsubscribeOutcome.OWNER_MISSING, delete.outcome)
        assertNull(put.state)
        assertNull(delete.state)
    }

    @Test
    fun `커밋된 변경마다 rev가 커밋 순서대로 증가하고 스냅샷과 함께 온다`() {
        val first = command.subscribe(userId, "005930", 100).state!!
        val second = command.subscribe(userId, "000660", 100).state!!
        val repeated = command.subscribe(userId, "005930", 100).state!!
        val removed = command.unsubscribe(userId, "005930").state!!
        val removedAgain = command.unsubscribe(userId, "005930").state!!

        assertEquals(listOf("005930"), first.codes)
        assertEquals(setOf("005930", "000660"), second.codes.toSet())
        assertEquals(setOf("005930", "000660"), repeated.codes.toSet())
        assertEquals(listOf("000660"), removed.codes)
        assertEquals(listOf("000660"), removedAgain.codes)
        assertEquals(
            listOf(first.rev, second.rev, repeated.rev, removed.rev, removedAgain.rev),
            listOf(first.rev, second.rev, repeated.rev, removed.rev, removedAgain.rev).sorted(),
        )
        assertEquals(5, listOf(first.rev, second.rev, repeated.rev, removed.rev, removedAgain.rev).distinct().size)
    }

    @Test
    fun `다른 사용자의 관심목록은 섞이지 않는다`() {
        val other = jdbc.queryForObject(
            "INSERT INTO users (email, password_hash, nickname) VALUES ('x@y.z', 'x', '다른사람') RETURNING id",
            Long::class.java,
        )!!
        command.subscribe(userId, "005930", 100)
        command.subscribe(other, "000660", 100)

        assertEquals(listOf("005930"), store.list(userId).map { it.code })
        assertEquals(listOf("000660"), store.list(other).map { it.code })
    }

    @Test
    fun `탈퇴하면 관심목록과 rev도 함께 지워진다`() {
        command.subscribe(userId, "005930", 100)

        jdbc.update("DELETE FROM users WHERE id = ?", userId)

        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM watchlist", Int::class.java))
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM watchlist_rev", Int::class.java))
    }

    @Test
    fun `동시에 서로 다른 종목을 담아도 한도를 넘지 않는다`() {
        val barrier = CyclicBarrier(2)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val attempts = listOf("005930", "000660").map { code ->
                pool.submit<SubscribeOutcome> {
                    barrier.await(10, TimeUnit.SECONDS)
                    command.subscribe(userId, code, 1).outcome
                }
            }
            val outcomes = attempts.map { it.get(30, TimeUnit.SECONDS) }

            assertEquals(1, outcomes.count { it == SubscribeOutcome.ADDED }, "결과: $outcomes")
            assertEquals(1, outcomes.count { it == SubscribeOutcome.LIMIT_EXCEEDED }, "결과: $outcomes")
            assertEquals(1, store.list(userId).size, "한도를 넘겨 저장됐다")
        } finally {
            pool.shutdownNow()
        }
    }
}
