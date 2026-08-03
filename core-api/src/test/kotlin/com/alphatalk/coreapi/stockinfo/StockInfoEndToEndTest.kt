package com.alphatalk.coreapi.stockinfo

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class StockInfoEndToEndTest {
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

    private val mapper = ObjectMapper()
    private lateinit var token: String

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM financial_summary")
        jdbc.update("DELETE FROM investor_flow_daily")
        jdbc.update("DELETE FROM valuation_daily")
        jdbc.update("DELETE FROM daily_candle")
        jdbc.update("DELETE FROM refresh_tokens")
        jdbc.update("DELETE FROM users")
        jdbc.update("DELETE FROM stock_master")
        jdbc.update("DELETE FROM sector")
        jdbc.update("INSERT INTO sector (code, name) VALUES ('IT', '전기전자')")
        jdbc.update(
            """
            INSERT INTO stock_master (code, name, market, sector_code, shares_outstanding, is_active, listed_at) VALUES
            ('005930', '삼성전자', 'KOSPI', 'IT', 5846278000, true, DATE '1975-06-11'),
            ('001234', '폐지된종목', 'KOSPI', NULL, 10000000, false, NULL)
            """.trimIndent(),
        )
        listOf(
            "20260706" to 70000,
            "20260707" to 70500,
            "20260708" to 71000,
            "20260709" to 71500,
            "20260710" to 72000,
            "20260713" to 72500,
            "20260714" to 73000,
        ).forEach { (date, close) ->
            jdbc.update(
                """
                INSERT INTO daily_candle (code, date, open, high, low, close, volume, value)
                VALUES ('005930', ?, ?, ?, ?, ?, 1000000, 71000000000)
                """.trimIndent(),
                date,
                close - 400,
                close + 500,
                close - 600,
                close,
            )
        }
        jdbc.update(
            """
            INSERT INTO valuation_daily (code, date, per, pbr, eps, bps, market_cap)
            VALUES ('005930', '20260714', 12.3, 1.1, 5800, 65000, 425000000000000)
            """.trimIndent(),
        )
        jdbc.update(
            """
            INSERT INTO investor_flow_daily (code, date, individual, "foreign", institution) VALUES
            ('005930', '20260713', -12000, 8000, 4000),
            ('005930', '20260714', -5000, 3000, 2000)
            """.trimIndent(),
        )
        jdbc.update(
            """
            INSERT INTO financial_summary
                (code, year, reprt_code, fs_div, revenue, operating_profit, net_income, assets, liabilities, equity, disclosed_at)
            VALUES
            ('005930', 2025, '11011', 'CFS', 302000000000000, 35000000000000, 28000000000000,
             450000000000000, 100000000000000, 350000000000000, TIMESTAMPTZ '2026-04-01 09:00:00+09'),
            ('005930', 2026, '11013', 'CFS', 79000000000000, 9000000000000, 7000000000000,
             455000000000000, 101000000000000, 354000000000000, TIMESTAMPTZ '2026-05-15 09:00:00+09')
            """.trimIndent(),
        )
        post("/api/v1/auth/signup", """{"email":"a@b.c","password":"password1","nickname":"민균"}""")
        token = json(post("/api/v1/auth/login", """{"email":"a@b.c","password":"password1"}""").body)
            .path("accessToken").asText()
    }

    private fun json(body: String?): JsonNode = mapper.readTree(body)

    private fun post(path: String, body: String) = rest.exchange(
        path,
        HttpMethod.POST,
        HttpEntity(body, HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }),
        String::class.java,
    )

    private fun get(path: String, bearer: String? = token) = rest.exchange(
        path,
        HttpMethod.GET,
        HttpEntity<String?>(null, HttpHeaders().apply { bearer?.let { set("Authorization", "Bearer $it") } }),
        String::class.java,
    )

    @Test
    fun `종목 개요는 섹터 이름과 상장일을 준다`() {
        val overview = json(get("/api/v1/stocks/005930").body)

        assertEquals("삼성전자", overview.path("name").asText())
        assertEquals("전기전자", overview.path("sector").asText())
        assertEquals("19750611", overview.path("listedAt").asText())
        assertEquals(5846278000, overview.path("sharesOutstanding").asLong())
    }

    @Test
    fun `FR-15 - 일봉은 to 이전 count건을 오름차순으로 준다`() {
        val response = json(get("/api/v1/stocks/005930/candles?period=D&count=3&to=20260710").body)

        assertEquals("D", response.path("period").asText())
        assertEquals(
            listOf("20260708", "20260709", "20260710"),
            response.path("items").map { it.path("date").asText() },
        )
        assertEquals(true, response.path("pageInfo").path("hasMoreBefore").asBoolean())
        assertEquals("20260707", response.path("pageInfo").path("nextTo").asText())
        assertEquals(71000000000L, response.path("items")[0].path("value").asLong())

        val next = json(get("/api/v1/stocks/005930/candles?period=D&count=3&to=20260707").body)
        assertEquals(listOf("20260706", "20260707"), next.path("items").map { it.path("date").asText() })
    }

    @Test
    fun `주봉은 일봉을 ISO 주로 합성한다`() {
        val response = json(get("/api/v1/stocks/005930/candles?period=W&count=5").body)

        val items = response.path("items")
        assertEquals(2, items.size())
        assertEquals("20260710", items[0].path("date").asText())
        assertEquals(69600, items[0].path("open").asLong())
        assertEquals(72000, items[0].path("close").asLong())
        assertEquals(5000000, items[0].path("volume").asLong())
        assertEquals("20260714", items[1].path("date").asText())
        assertEquals(false, response.path("pageInfo").path("hasMoreBefore").asBoolean())
        assertEquals(true, response.path("pageInfo").path("nextTo").isNull)
    }

    @Test
    fun `주봉 다음 페이지는 nextTo로 요청해야 같은 주가 중복 집계되지 않는다`() {
        val first = json(get("/api/v1/stocks/005930/candles?period=W&count=1").body)

        assertEquals(listOf("20260714"), first.path("items").map { it.path("date").asText() })
        assertEquals(true, first.path("pageInfo").path("hasMoreBefore").asBoolean())
        assertEquals("20260712", first.path("pageInfo").path("nextTo").asText())

        val next = json(get("/api/v1/stocks/005930/candles?period=W&count=1&to=20260712").body)
        val week = next.path("items").single()
        assertEquals("20260710", week.path("date").asText())
        assertEquals(69600, week.path("open").asLong())
        assertEquals(5000000, week.path("volume").asLong())
    }

    @Test
    fun `숫자 파라미터에 문자가 오면 500이 아니라 400이다`() {
        val response = get("/api/v1/stocks/005930/candles?count=abc")

        assertEquals(400, response.statusCode.value())
        val error = json(response.body).path("error")
        assertEquals("VALIDATION_FAILED", error.path("code").asText())
        assertEquals("count", error.path("detail").path("field").asText())
    }

    @Test
    fun `FR-14 - 밸류에이션은 시총을 억원으로 주고 asOf를 붙인다`() {
        val valuation = json(get("/api/v1/stocks/005930/valuation").body)

        assertEquals(12.3, valuation.path("per").asDouble())
        assertEquals(4250000, valuation.path("marketCap").asLong())
        assertEquals("20260714", valuation.path("asOf").asText())
    }

    @Test
    fun `재무는 연간과 분기로 나뉘고 금액이 억원이다`() {
        val financials = json(get("/api/v1/stocks/005930/financials?years=3").body)

        val annual = financials.path("annual")
        assertEquals(1, annual.size())
        assertEquals("2025", annual[0].path("period").asText())
        assertEquals(3020000, annual[0].path("revenue").asLong())
        assertEquals("DART", annual[0].path("source").asText())
        assertEquals("20260401", annual[0].path("asOf").asText())

        val quarterly = financials.path("quarterly")
        assertEquals(1, quarterly.size())
        assertEquals("2026Q1", quarterly[0].path("period").asText())
    }

    @Test
    fun `수급은 최신 영업일부터 순매수를 준다`() {
        val investors = json(get("/api/v1/stocks/005930/investors?days=20").body)

        val items = investors.path("items")
        assertEquals(2, items.size())
        assertEquals("20260714", items[0].path("date").asText())
        assertEquals(-5000, items[0].path("individual").asLong())
        assertEquals(3000, items[0].path("foreign").asLong())
        assertEquals(2000, items[0].path("institution").asLong())
    }

    @Test
    fun `폐지 종목과 없는 종목은 404다`() {
        assertEquals(404, get("/api/v1/stocks/001234").statusCode.value())
        assertEquals(404, get("/api/v1/stocks/999999/candles").statusCode.value())
        assertEquals("NOT_FOUND", json(get("/api/v1/stocks/999999").body).path("error").path("code").asText())
    }

    @Test
    fun `종목 정보 API는 인증이 필요하다`() {
        assertEquals(401, get("/api/v1/stocks/005930", bearer = null).statusCode.value())
        assertTrue(json(get("/api/v1/stocks/005930/candles", bearer = null).body).path("error").path("code").asText() == "UNAUTHORIZED")
    }
}
