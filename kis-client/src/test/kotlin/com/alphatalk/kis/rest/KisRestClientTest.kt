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

    @Test
    fun `밸류에이션 스냅샷을 파싱한다 - 0은 지표 없음이라 null`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            200,
            """
            {"rt_cd":"0","msg_cd":"MCA00000","output":{
              "stck_prpr":"71200","per":"12.10","pbr":"0.00","eps":"5771.00","bps":"0"}}
            """.trimIndent(),
        )

        val snapshot = client.valuationSnapshot(account, "005930")

        assertEquals(71200, snapshot.price)
        assertEquals("12.10", snapshot.per?.toPlainString())
        assertEquals(null, snapshot.pbr)
        assertEquals(5771, snapshot.eps)
        assertEquals(null, snapshot.bps)
        val call = server.received.single { it.path.endsWith("inquire-price") }
        assertEquals("FHKST01010100", call.headers["tr_id"])
        assertTrue("FID_INPUT_ISCD=005930" in call.query)
    }

    @Test
    fun `밸류에이션 스냅샷은 적자 음수 EPS를 보존한다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            200,
            """
            {"rt_cd":"0","msg_cd":"MCA00000","output":{
              "stck_prpr":"3000","per":"0.00","pbr":"0.45","eps":"-1520.00","bps":"6600.00"}}
            """.trimIndent(),
        )

        val snapshot = client.valuationSnapshot(account, "123456")

        assertEquals(null, snapshot.per)
        assertEquals("0.45", snapshot.pbr?.toPlainString())
        assertEquals(-1520, snapshot.eps)
        assertEquals(6600, snapshot.bps)
    }

    @Test
    fun `투자자별 순매수를 파싱하고 결측 행은 건너뛴다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-investor",
            200,
            """
            {"rt_cd":"0","msg_cd":"MCA00000","output":[
              {"stck_bsop_date":"20260807","prsn_ntby_tr_pbmn":"-12000","frgn_ntby_tr_pbmn":"8000","orgn_ntby_tr_pbmn":"4000"},
              {"stck_bsop_date":"20260806","prsn_ntby_tr_pbmn":"1500","frgn_ntby_tr_pbmn":"-900","orgn_ntby_tr_pbmn":"-600"},
              {"stck_bsop_date":"","prsn_ntby_tr_pbmn":"1","frgn_ntby_tr_pbmn":"1","orgn_ntby_tr_pbmn":"1"},
              {"stck_bsop_date":"20260805","prsn_ntby_tr_pbmn":"","frgn_ntby_tr_pbmn":"1","orgn_ntby_tr_pbmn":"1"}]}
            """.trimIndent(),
        )

        val flows = client.investorFlows(account, "005930")

        assertEquals(2, flows.size)
        assertEquals("20260807", flows[0].date)
        assertEquals(-12000, flows[0].individual)
        assertEquals(8000, flows[0].foreign)
        assertEquals(4000, flows[0].institution)
        val call = server.received.single { it.path.endsWith("inquire-investor") }
        assertEquals("FHKST01010900", call.headers["tr_id"])
        assertTrue("FID_COND_MRKT_DIV_CODE=J" in call.query)
        assertTrue("FID_INPUT_ISCD=005930" in call.query)
    }

    @Test
    fun `투자자별 순매수 rt_cd가 0이 아니면 예외를 던진다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-investor",
            200,
            """{"rt_cd":"1","msg_cd":"EGW00121","msg1":"invalid"}""",
        )

        assertFailsWith<KisClientException> { client.investorFlows(account, "005930") }
    }

    @Test
    fun `기간별 일봉을 파싱하고 빈 행은 건너뛴다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            200,
            """
            {"rt_cd":"0","output2":[
              {"stck_bsop_date":"20260724","stck_oprc":"70600","stck_hgpr":"71500","stck_lwpr":"70400",
               "stck_clpr":"71200","acml_vol":"1234567","acml_tr_pbmn":"87942671300"},
              {"stck_bsop_date":"20260723","stck_oprc":"70100","stck_hgpr":"70900","stck_lwpr":"69800",
               "stck_clpr":"70500","acml_vol":"2234567","acml_tr_pbmn":"97942671300"},
              {"stck_bsop_date":""}]}
            """.trimIndent(),
        )

        val candles = client.dailyCandles(
            account,
            "005930",
            java.time.LocalDate.of(2026, 4, 25),
            java.time.LocalDate.of(2026, 7, 24),
        )

        assertEquals(2, candles.size)
        assertEquals("20260724", candles[0].date)
        assertEquals(71200, candles[0].close)
        assertEquals(87942671300, candles[0].value)
        val call = server.received.single { it.path.endsWith("inquire-daily-itemchartprice") }
        assertEquals("FHKST03010100", call.headers["tr_id"])
        assertTrue("FID_INPUT_DATE_1=20260425" in call.query)
        assertTrue("FID_INPUT_DATE_2=20260724" in call.query)
        assertTrue("FID_PERIOD_DIV_CODE=D" in call.query)
        assertTrue("FID_ORG_ADJ_PRC=0" in call.query)
    }

    @Test
    fun `당일 분봉을 파싱하고 시각은 HHmm으로 자른다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            200,
            """
            {"rt_cd":"0","output1":{"acml_vol":"12345678"},"output2":[
              {"stck_bsop_date":"20260804","stck_cntg_hour":"130400","stck_oprc":"230000","stck_hgpr":"230500",
               "stck_lwpr":"229500","stck_prpr":"230500","cntg_vol":"120000","acml_tr_pbmn":"4700000000000"},
              {"stck_bsop_date":"20260804","stck_cntg_hour":"130300","stck_oprc":"229500","stck_hgpr":"230000",
               "stck_lwpr":"229000","stck_prpr":"230000","cntg_vol":"98000","acml_tr_pbmn":"4699972360000"},
              {"stck_bsop_date":""}]}
            """.trimIndent(),
        )

        val chart = client.minuteCandles(
            account,
            "005930",
            java.time.LocalTime.of(13, 4),
            KisRestClient.MARKET_DIV_UNIFIED,
        )

        val candles = chart.candles
        assertEquals(12345678, chart.dailyVolume)
        assertEquals(2, candles.size)
        assertEquals("1304", candles[0].time)
        assertEquals("20260804", candles[0].date)
        assertEquals(230500, candles[0].close)
        assertEquals(120000, candles[0].volume)
        assertEquals(4700000000000, candles[0].accValue)
        val call = server.received.single { it.path.endsWith("inquire-time-itemchartprice") }
        assertEquals("FHKST03010200", call.headers["tr_id"])
        assertTrue("FID_INPUT_HOUR_1=130400" in call.query)
        assertTrue("FID_PW_DATA_INCU_YN=Y" in call.query)
        assertTrue("FID_COND_MRKT_DIV_CODE=UN" in call.query)
    }

    @Test
    fun `모든 REST 호출은 공용 gate를 계정 키로 통과한다`() {
        val gated = mutableListOf<String>()
        val tokens = KisTokenManager(server.baseUrl, InMemoryKisTokenStore())
        val gatedClient = KisRestClient(
            server.baseUrl,
            tokens,
            KisRateLimiters(100.0, 1.0),
            gate = { keyId -> gated += keyId },
        )
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue("/uapi/test", 200, """{"rt_cd":"0"}""")

        gatedClient.getJson(account, "/uapi/test", "TR123", emptyMap())

        assertEquals(listOf("key1"), gated)
    }

    @Test
    fun `분봉 시장 코드는 호출자가 지정한 값을 그대로 보낸다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            200,
            """{"rt_cd":"0","output2":[]}""",
        )

        val chart = client.minuteCandles(account, "005930", java.time.LocalTime.of(13, 4), KisRestClient.MARKET_DIV_KRX)

        val call = server.received.single { it.path.endsWith("inquire-time-itemchartprice") }
        assertTrue("FID_COND_MRKT_DIV_CODE=J" in call.query)
        assertEquals(0, chart.dailyVolume)
    }

    @Test
    fun `분봉 rt_cd가 0이 아니면 예외를 던진다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            200,
            """{"rt_cd":"1","msg_cd":"EGW00123"}""",
        )

        assertFailsWith<KisClientException> {
            client.minuteCandles(account, "005930", java.time.LocalTime.of(13, 4), KisRestClient.MARKET_DIV_UNIFIED)
        }
    }
    @Test
    fun `투자의견을 파싱한다 - 목표가 0과 빈 직전의견은 null`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/invest-opbysec",
            200,
            """
            {"rt_cd":"0","output":[
              {"stck_bsop_date":"20260806","stck_shrn_iscd":"251270","hts_kor_isnm":"넷마블",
               "invt_opnn":"매수","invt_opnn_cls_code":"2","rgbf_invt_opnn":"중립","rgbf_invt_opnn_cls_code":"3",
               "mbcr_name":"미래에셋","hts_goal_prc":"50000","stck_prpr":"40800"},
              {"stck_bsop_date":"20260806","stck_shrn_iscd":"252990","hts_kor_isnm":"샘씨엔에스",
               "invt_opnn":"NotRated","invt_opnn_cls_code":"2","rgbf_invt_opnn":"","rgbf_invt_opnn_cls_code":"3",
               "mbcr_name":"","hts_goal_prc":"0","stck_prpr":"14160"},
              {"stck_bsop_date":"","stck_shrn_iscd":"","invt_opnn":""}]}
            """.trimIndent(),
        )

        val opinions = client.investOpinions(
            account,
            "005",
            java.time.LocalDate.of(2026, 8, 5),
            java.time.LocalDate.of(2026, 8, 6),
        )

        assertEquals(2, opinions.size)
        val first = opinions[0]
        assertEquals("251270", first.code)
        assertEquals("20260806", first.businessDate)
        assertEquals("매수", first.rating)
        assertEquals("중립", first.previousRating)
        assertEquals(50000L, first.targetPrice)
        assertEquals("미래에셋", first.memberName)
        val second = opinions[1]
        assertEquals("NotRated", second.rating)
        assertEquals(null, second.previousRating)
        assertEquals(null, second.targetPrice)
        assertEquals(null, second.memberName)

        val call = server.received.single { it.path.endsWith("invest-opbysec") }
        assertEquals("FHKST663400C0", call.headers["tr_id"])
        assertTrue("FID_COND_SCR_DIV_CODE=16634" in call.query)
        assertTrue("FID_INPUT_ISCD=005" in call.query)
        assertTrue("FID_DIV_CLS_CODE=0" in call.query)
        assertTrue("FID_INPUT_DATE_1=20260805" in call.query)
        assertTrue("FID_INPUT_DATE_2=20260806" in call.query)
    }

    @Test
    fun `투자의견 회원사 코드는 3자리만 허용한다 - 5자리는 조용한 0행 실패라 요청 전에 막는다`() {
        assertFailsWith<IllegalArgumentException> {
            client.investOpinions(account, "00005", java.time.LocalDate.of(2026, 8, 5), java.time.LocalDate.of(2026, 8, 6))
        }
    }

    @Test
    fun `투자의견 rt_cd가 0이 아니면 예외를 던진다`() {
        server.enqueue("/oauth2/tokenP", 200, tokenBody("T1"))
        server.enqueue(
            "/uapi/domestic-stock/v1/quotations/invest-opbysec",
            200,
            """{"rt_cd":"1","msg_cd":"EGW00121"}""",
        )

        assertFailsWith<KisClientException> {
            client.investOpinions(account, "005", java.time.LocalDate.of(2026, 8, 5), java.time.LocalDate.of(2026, 8, 6))
        }
    }
}
