package com.alphatalk.kis.rest

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.RecordingKisServer
import com.alphatalk.kis.auth.InMemoryKisTokenStore
import com.alphatalk.kis.auth.KisTokenManager
import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.rate.KisRateLimiters
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class KisRestClientTest {
    private lateinit var server: RecordingKisServer
    private lateinit var client: KisRestClient
    private val account = KisAccount("key1", "app-key", "app-secret")

    @BeforeTest
    fun setUp() {
        server = RecordingKisServer()
        val tokens = KisTokenManager(server.baseUrl, InMemoryKisTokenStore())
        client = KisRestClient(server.baseUrl, tokens, KisRateLimiters(100.0, 1.0))
    }

    @AfterTest
    fun tearDown() {
        server.close()
    }

    private fun tokenBody(token: String) =
        """{"access_token":"$token","token_type":"Bearer","expires_in":86400}"""

    @Test
    fun `공통 헤더와 쿼리 파라미터로 호출한다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue("/uapi/test", 200, """{"rt_cd":"0"}""")

        val json = client.getJson(account, "/uapi/test", "TR123", mapOf("FID_INPUT_ISCD" to "005930"))

        assertEquals("0", json.path("rt_cd").asText())
        val call = server.received.single { it.path == "/uapi/test" }
        assertEquals("Bearer T1", call.headers["authorization"])
        assertEquals("app-key", call.headers["appkey"])
        assertEquals("app-secret", call.headers["appsecret"])
        assertEquals("TR123", call.headers["tr_id"])
        assertEquals("P", call.headers["custtype"])
        assertTrue("FID_INPUT_ISCD=005930" in call.query)
    }

    @Test
    fun `401이면 토큰을 무효화하고 1회 재시도한다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T2"))
        server.enqueue("/uapi/test", 401, "{}")
        server.enqueue("/uapi/test", 200, """{"rt_cd":"0"}""")

        val json = client.getJson(account, "/uapi/test", "TR123", emptyMap())

        assertEquals("0", json.path("rt_cd").asText())
        assertEquals(2, server.countOf("/oauth2/tokenP"))
        val calls = server.received.filter { it.path == "/uapi/test" }
        assertEquals(2, calls.size)
        assertEquals("Bearer T2", calls[1].headers["authorization"])
    }

    @Test
    fun `연속 401이면 재시도는 한 번뿐이고 예외를 던진다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T2"))
        server.enqueue("/uapi/test", 401, "{}")

        assertFailsWith<KisClientException> { client.getJson(account, "/uapi/test", "TR123", emptyMap()) }
        assertEquals(2, server.received.count { it.path == "/uapi/test" })
    }
}
