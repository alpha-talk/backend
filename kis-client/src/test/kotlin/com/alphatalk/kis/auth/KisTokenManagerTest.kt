package com.alphatalk.kis.auth

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.RecordingKisServer
import com.alphatalk.kis.model.KisAccount
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class KisTokenManagerTest {
    private lateinit var server: RecordingKisServer
    private val account = KisAccount("key1", "app-key", "app-secret")

    @BeforeTest
    fun setUp() {
        server = RecordingKisServer()
    }

    @AfterTest
    fun tearDown() {
        server.close()
    }

    private fun tokenBody(token: String, expiresIn: Long = 86400) =
        """{"access_token":"$token","token_type":"Bearer","expires_in":$expiresIn}"""

    @Test
    fun `발급 후 캐시 히트면 재호출하지 않는다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        val manager = KisTokenManager(server.baseUrl, InMemoryKisTokenStore())

        assertEquals("T1", manager.accessToken(account))
        assertEquals("T1", manager.accessToken(account))

        assertEquals(1, server.countOf("/oauth2/tokenP"))
        val issueBody = server.received.first { it.path == "/oauth2/tokenP" }.body
        assertTrue("\"grant_type\":\"client_credentials\"" in issueBody)
        assertTrue("\"appkey\":\"app-key\"" in issueBody)
        assertTrue("\"appsecret\":\"app-secret\"" in issueBody)
    }

    @Test
    fun `토큰 캐시는 만료 5분 전에 비워지고 그 뒤 재발급된다`() {
        var now = Instant.parse("2026-07-27T00:00:00Z")
        val clock = { now }
        val store = InMemoryKisTokenStore(clock)
        val manager = KisTokenManager(server.baseUrl, store, clock = clock)
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T2"))

        assertEquals("T1", manager.accessToken(account))
        now += Duration.ofSeconds(86400) - Duration.ofMinutes(5) - Duration.ofSeconds(1)
        assertEquals("T1", manager.accessToken(account))
        now += Duration.ofSeconds(2)
        assertEquals("T2", manager.accessToken(account))

        assertEquals(2, server.countOf("/oauth2/tokenP"))
    }

    @Test
    fun `직전 발급 1분 내 캐시 미스는 재발급 없이 실패한다`() {
        var now = Instant.parse("2026-07-27T00:00:00Z")
        val clock = { now }
        val store = InMemoryKisTokenStore(clock)
        val manager = KisTokenManager(server.baseUrl, store, clock = clock)
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))

        assertEquals("T1", manager.accessToken(account))
        store.evict(account.keyId)
        now += Duration.ofSeconds(10)

        assertFailsWith<KisClientException> { manager.accessToken(account) }
        assertEquals(1, server.countOf("/oauth2/tokenP"))
    }

    @Test
    fun `invalidate 후에는 1분 내에도 재발급된다`() {
        var now = Instant.parse("2026-07-27T00:00:00Z")
        val clock = { now }
        val store = InMemoryKisTokenStore(clock)
        val manager = KisTokenManager(server.baseUrl, store, clock = clock)
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T2"))

        assertEquals("T1", manager.accessToken(account))
        manager.invalidate(account.keyId)
        now += Duration.ofSeconds(10)

        assertEquals("T2", manager.accessToken(account))
        assertEquals(2, server.countOf("/oauth2/tokenP"))
    }

    @Test
    fun `동시 요청에도 발급은 한 번이다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        val manager = KisTokenManager(server.baseUrl, InMemoryKisTokenStore(), lockWaitMillis = 20)
        val pool = Executors.newFixedThreadPool(4)

        val results = (1..4)
            .map { pool.submit(Callable { manager.accessToken(account) }) }
            .map { it.get(5, TimeUnit.SECONDS) }
        pool.shutdown()

        assertTrue(results.all { it == "T1" })
        assertEquals(1, server.countOf("/oauth2/tokenP"))
    }

    @Test
    fun `발급 실패 응답이면 예외를 던진다`() {
        server.enqueue("/oauth2/tokenP", 403, """{"error_description":"invalid"}""")
        val manager = KisTokenManager(server.baseUrl, InMemoryKisTokenStore())

        assertFailsWith<KisClientException> { manager.accessToken(account) }
    }

    @Test
    fun `응답이 락 유효기간보다 늦어도 tokenP 호출은 한 번뿐이다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"), delayMillis = 900)
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T2"))
        val store = InMemoryKisTokenStore()
        val first = manager(store)
        val second = manager(store)

        assertFails { first.accessToken(account) }
        assertFailsWith<KisClientException> { second.accessToken(account) }

        assertEquals(1, server.countOf("/oauth2/tokenP"))
    }

    @Test
    fun `응답 유실 뒤에도 재발급 제한이 유지된다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"), delayMillis = 900)
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T2"))
        val store = InMemoryKisTokenStore()
        val manager = manager(store)

        assertFails { manager.accessToken(account) }
        val throttled = assertFailsWith<KisClientException> { manager.accessToken(account) }

        assertTrue("throttled" in throttled.message.orEmpty())
        assertEquals(1, server.countOf("/oauth2/tokenP"))
    }

    @Test
    fun `발급 타임아웃이 락 유효기간보다 길면 기동에 실패한다`() {
        assertFailsWith<IllegalArgumentException> {
            KisTokenManager(
                server.baseUrl,
                InMemoryKisTokenStore(),
                lockTtl = Duration.ofSeconds(3),
                issueTimeout = Duration.ofSeconds(5),
            )
        }
    }

    private fun manager(store: InMemoryKisTokenStore) = KisTokenManager(
        server.baseUrl,
        store,
        lockTtl = Duration.ofMillis(400),
        issueTimeout = Duration.ofMillis(250),
        lockWaitMillis = 50,
    )
}
