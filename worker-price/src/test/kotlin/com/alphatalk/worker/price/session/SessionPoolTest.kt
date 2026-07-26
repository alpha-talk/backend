package com.alphatalk.worker.price.session

import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.test.FakeKisServer
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.awaitility.Awaitility.await
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SessionPoolTest {
    private lateinit var server: FakeKisServer
    private val mapper = jacksonObjectMapper()
    private var now = 0L

    @BeforeTest
    fun setUp() {
        server = FakeKisServer()
        server.startAndAwait()
        now = 0L
    }

    @AfterTest
    fun tearDown() {
        server.close()
    }

    private fun pool(
        accounts: Int = 1,
        maxPerSession: Int = 2,
        graceMillis: Long = 1_000,
    ) = SessionPool(
        accounts = (1..accounts).map { KisAccount("key$it", "app$it", "secret$it") },
        wsUrl = server.url,
        approvalKeys = { "AK" },
        buffer = ConflationBuffer(),
        meters = SimpleMeterRegistry(),
        maxSymbolsPerSession = maxPerSession,
        removalGraceMillis = graceMillis,
        backoff = BackoffPolicy(initialMillis = 50, jitterRatio = 0.0),
        clock = { now },
    )

    private fun subscribesOf(messages: List<String>) =
        messages.map { mapper.readTree(it) }.filter { it.path("header").path("tr_type").asText() == "1" }

    private fun unsubscribesOf(messages: List<String>) =
        messages.map { mapper.readTree(it) }.filter { it.path("header").path("tr_type").asText() == "2" }

    @Test
    fun `용량을 넘는 종목은 강등 목록에 남는다`() {
        val pool = pool(accounts = 1, maxPerSession = 2)

        pool.maintain(linkedSetOf("000001", "000002", "000003"), subscribeAllowed = true)

        server.awaitMessages(2)
        assertEquals(2, subscribesOf(server.receivedMessages).size)
        assertEquals(setOf("000003"), pool.degradedSymbols())
    }

    @Test
    fun `종목은 빈 슬롯이 많은 세션부터 배정된다`() {
        val pool = pool(accounts = 2, maxPerSession = 2)

        pool.maintain(linkedSetOf("000001", "000002", "000003"), subscribeAllowed = true)

        server.awaitConnections(2)
        server.awaitMessages(3)
        assertEquals(3, subscribesOf(server.receivedMessages).size)
        assertTrue(pool.degradedSymbols().isEmpty())
    }

    @Test
    fun `구독 허용 전에는 연결만 하고 구독하지 않는다`() {
        val pool = pool()

        pool.maintain(setOf("005930"), subscribeAllowed = false)

        server.awaitConnections(1)
        Thread.sleep(200)
        assertTrue(server.receivedMessages.isEmpty())
    }

    @Test
    fun `절단되면 백오프 후 재접속해 배정분을 재구독한다`() {
        val pool = pool()
        pool.maintain(setOf("005930"), subscribeAllowed = true)
        server.awaitMessages(1)

        server.closeAllConnections()

        await().atMost(Duration.ofSeconds(10)).until {
            now += 200
            pool.maintain(setOf("005930"), subscribeAllowed = true)
            subscribesOf(server.receivedMessages).size >= 2
        }
    }

    @Test
    fun `해지는 유예가 지난 뒤에만 전송된다`() {
        val pool = pool(graceMillis = 1_000)
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(setOf("000001"), subscribeAllowed = true)
        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())

        now += 1_500
        pool.maintain(setOf("000001"), subscribeAllowed = true)

        server.awaitMessages(3)
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(1, unsubscribed.size)
        assertEquals("000002", unsubscribed[0].path("body").path("input").path("tr_key").asText())
    }

    @Test
    fun `유예 중 재수요가 오면 해지가 취소된다`() {
        val pool = pool(graceMillis = 1_000)
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(setOf("000001"), subscribeAllowed = true)
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)
        now += 2_000
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)

        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())
    }

    @Test
    fun `disconnectAll은 해지 후 연결을 닫는다`() {
        val pool = pool()
        pool.maintain(setOf("005930"), subscribeAllowed = true)
        server.awaitMessages(1)

        pool.disconnectAll()

        server.awaitMessages(2)
        assertEquals(1, unsubscribesOf(server.receivedMessages).size)
        await().atMost(Duration.ofSeconds(5)).until { server.connectionCount == 0 }
    }
}
