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

    @Test
    fun `주식현재가 스냅샷을 파싱하고 하락 부호를 음수로 만든다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            200,
            """
            {"rt_cd":"0","msg_cd":"MCA00000","output":{
              "stck_prpr":"71200","prdy_vrss":"700","prdy_vrss_sign":"5","prdy_ctrt":"0.99",
              "stck_oprc":"70600","stck_hgpr":"71500","stck_lwpr":"70400","acml_vol":"1234567",
              "per":"12.10","pbr":"1.35"}}
            """.trimIndent(),
        )

        val snapshot = client.quoteSnapshot(account, "005930")

        assertEquals(71200, snapshot.price)
        assertEquals(-700, snapshot.change)
        assertEquals(-0.99, snapshot.changeRate)
        assertEquals(70600, snapshot.open)
        assertEquals(71500, snapshot.high)
        assertEquals(70400, snapshot.low)
        assertEquals(1234567, snapshot.volume)
        val call = server.received.single { it.path.endsWith("inquire-price") }
        assertEquals("FHKST01010100", call.headers["tr_id"])
        assertTrue("FID_COND_MRKT_DIV_CODE=J" in call.query)
        assertTrue("FID_INPUT_ISCD=005930" in call.query)
    }

    @Test
    fun `스냅샷 rt_cd가 0이 아니면 예외를 던진다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            200,
            """{"rt_cd":"1","msg_cd":"EGW00121","msg1":"invalid"}""",
        )

        assertFailsWith<KisClientException> { client.quoteSnapshot(account, "005930") }
    }
}
