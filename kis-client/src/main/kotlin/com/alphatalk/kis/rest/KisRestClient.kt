package com.alphatalk.kis.rest

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.KisSigns
import com.alphatalk.kis.auth.KisTokenManager
import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.rate.KisRateGate
import com.alphatalk.kis.rate.KisRateLimiters
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

class KisRestClient(
    private val restBaseUrl: String,
    private val tokens: KisTokenManager,
    private val limiters: KisRateLimiters,
    private val gate: KisRateGate = KisRateGate.NOOP,
    private val http: HttpClient = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build(),
    private val requestTimeout: Duration = REQUEST_TIMEOUT,
) {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    fun quoteSnapshot(account: KisAccount, code: String, marketDiv: String = MARKET_DIV_KRX): KisQuoteSnapshot {
        val json = getJson(
            account,
            INQUIRE_PRICE_PATH,
            TR_INQUIRE_PRICE,
            mapOf(
                "FID_COND_MRKT_DIV_CODE" to marketDiv,
                "FID_INPUT_ISCD" to code,
            ),
        )
        val rtCd = json.path("rt_cd").asText("")
        if (rtCd != "0") {
            throw KisClientException(
                "inquire-price failed: keyId=${account.keyId} code=$code rt_cd=$rtCd msg_cd=${json.path("msg_cd").asText("")}",
            )
        }
        val output = json.path("output")
        val falling = KisSigns.isFalling(output.path("prdy_vrss_sign").asText(""))
        return KisQuoteSnapshot(
            code = code,
            price = output.path("stck_prpr").asText().trim().toLong(),
            change = KisSigns.apply(output.path("prdy_vrss").asText().trim().toLong(), falling),
            changeRate = KisSigns.apply(output.path("prdy_ctrt").asText().trim().toDouble(), falling),
            open = output.path("stck_oprc").asText().trim().toLong(),
            high = output.path("stck_hgpr").asText().trim().toLong(),
            low = output.path("stck_lwpr").asText().trim().toLong(),
            volume = output.path("acml_vol").asText().trim().toLong(),
        )
    }

    fun valuationSnapshot(account: KisAccount, code: String): KisValuationSnapshot {
        val json = getJson(
            account,
            INQUIRE_PRICE_PATH,
            TR_INQUIRE_PRICE,
            mapOf(
                "FID_COND_MRKT_DIV_CODE" to MARKET_DIV_KRX,
                "FID_INPUT_ISCD" to code,
            ),
        )
        val rtCd = json.path("rt_cd").asText("")
        if (rtCd != "0") {
            throw KisClientException(
                "inquire-price failed: keyId=${account.keyId} code=$code rt_cd=$rtCd msg_cd=${json.path("msg_cd").asText("")}",
            )
        }
        val output = json.path("output")
        return KisValuationSnapshot(
            code = code,
            price = output.path("stck_prpr").asText().trim().toLong(),
            per = ratioOrNull(output.path("per").asText("")),
            pbr = ratioOrNull(output.path("pbr").asText("")),
            eps = amountOrNull(output.path("eps").asText("")),
            bps = amountOrNull(output.path("bps").asText("")),
        )
    }

    fun investorFlows(account: KisAccount, code: String): List<KisInvestorFlow> {
        val json = getJson(
            account,
            INQUIRE_INVESTOR_PATH,
            TR_INQUIRE_INVESTOR,
            mapOf(
                "FID_COND_MRKT_DIV_CODE" to MARKET_DIV_KRX,
                "FID_INPUT_ISCD" to code,
            ),
        )
        val rtCd = json.path("rt_cd").asText("")
        if (rtCd != "0") {
            throw KisClientException(
                "inquire-investor failed: keyId=${account.keyId} code=$code rt_cd=$rtCd msg_cd=${json.path("msg_cd").asText("")}",
            )
        }
        return json.path("output").mapNotNull { row ->
            val date = row.path("stck_bsop_date").asText("").trim()
            if (date.isEmpty()) return@mapNotNull null
            KisInvestorFlow(
                code = code,
                date = date,
                individual = requireFlowAmount(row, "prsn_ntby_tr_pbmn", account, code, date),
                foreign = requireFlowAmount(row, "frgn_ntby_tr_pbmn", account, code, date),
                institution = requireFlowAmount(row, "orgn_ntby_tr_pbmn", account, code, date),
            )
        }
    }

    private fun requireFlowAmount(row: JsonNode, field: String, account: KisAccount, code: String, date: String): Long =
        row.path(field).asText("").trim().toLongOrNull()
            ?: throw KisClientException(
                "inquire-investor row malformed - schema drift suspected: keyId=${account.keyId} code=$code date=$date field=$field",
            )

    private fun ratioOrNull(raw: String): BigDecimal? =
        raw.trim().toBigDecimalOrNull()?.takeIf { it.signum() != 0 }

    private fun amountOrNull(raw: String): Int? =
        raw.trim().toBigDecimalOrNull()
            ?.takeIf { it.signum() != 0 }
            ?.setScale(0, RoundingMode.DOWN)
            ?.intValueExact()

    fun dailyCandles(account: KisAccount, code: String, from: LocalDate, to: LocalDate): List<KisDailyCandle> {
        val json = getJson(
            account,
            DAILY_CHART_PATH,
            TR_DAILY_CHART,
            mapOf(
                "FID_COND_MRKT_DIV_CODE" to "J",
                "FID_INPUT_ISCD" to code,
                "FID_INPUT_DATE_1" to from.format(DateTimeFormatter.BASIC_ISO_DATE),
                "FID_INPUT_DATE_2" to to.format(DateTimeFormatter.BASIC_ISO_DATE),
                "FID_PERIOD_DIV_CODE" to "D",
                "FID_ORG_ADJ_PRC" to "0",
            ),
        )
        val rtCd = json.path("rt_cd").asText("")
        if (rtCd != "0") {
            throw KisClientException(
                "daily chart failed: keyId=${account.keyId} code=$code rt_cd=$rtCd msg_cd=${json.path("msg_cd").asText("")}",
            )
        }
        return json.path("output2").mapNotNull { row ->
            val date = row.path("stck_bsop_date").asText("")
            if (date.isBlank()) {
                null
            } else {
                KisDailyCandle(
                    code = code,
                    date = date,
                    open = row.path("stck_oprc").asText().trim().toLong(),
                    high = row.path("stck_hgpr").asText().trim().toLong(),
                    low = row.path("stck_lwpr").asText().trim().toLong(),
                    close = row.path("stck_clpr").asText().trim().toLong(),
                    volume = row.path("acml_vol").asText().trim().toLong(),
                    value = row.path("acml_tr_pbmn").asText().trim().toLong(),
                )
            }
        }
    }

    fun minuteCandles(
        account: KisAccount,
        code: String,
        toTime: LocalTime,
        marketDiv: String,
    ): KisMinuteChart {
        val json = getJson(
            account,
            MINUTE_CHART_PATH,
            TR_MINUTE_CHART,
            mapOf(
                "FID_ETC_CLS_CODE" to "",
                "FID_COND_MRKT_DIV_CODE" to marketDiv,
                "FID_INPUT_ISCD" to code,
                "FID_INPUT_HOUR_1" to toTime.format(DateTimeFormatter.ofPattern("HHmmss")),
                "FID_PW_DATA_INCU_YN" to "Y",
            ),
        )
        val rtCd = json.path("rt_cd").asText("")
        if (rtCd != "0") {
            throw KisClientException(
                "minute chart failed: keyId=${account.keyId} code=$code rt_cd=$rtCd msg_cd=${json.path("msg_cd").asText("")}",
            )
        }
        val dailyVolume = json.path("output1").path("acml_vol").asText().trim().toLongOrNull() ?: 0L
        val candles = json.path("output2").mapNotNull { row ->
            val date = row.path("stck_bsop_date").asText("")
            val hour = row.path("stck_cntg_hour").asText("")
            if (date.isBlank() || hour.length < 4) {
                null
            } else {
                KisMinuteCandle(
                    code = code,
                    date = date,
                    time = hour.take(4),
                    open = row.path("stck_oprc").asText().trim().toLong(),
                    high = row.path("stck_hgpr").asText().trim().toLong(),
                    low = row.path("stck_lwpr").asText().trim().toLong(),
                    close = row.path("stck_prpr").asText().trim().toLong(),
                    volume = row.path("cntg_vol").asText().trim().toLong(),
                    accValue = row.path("acml_tr_pbmn").asText().trim().toLong(),
                )
            }
        }
        return KisMinuteChart(dailyVolume = dailyVolume, candles = candles)
    }

    fun investOpinions(
        account: KisAccount,
        brokerQueryCode: String,
        from: LocalDate,
        to: LocalDate,
    ): List<KisInvestOpinion> {
        require(brokerQueryCode.length == INVEST_OPINION_QUERY_CODE_LENGTH && brokerQueryCode.all(Char::isDigit)) {
            "invest opinion broker query code must be $INVEST_OPINION_QUERY_CODE_LENGTH digits: $brokerQueryCode"
        }
        val json = getJson(
            account,
            INVEST_OPINION_PATH,
            TR_INVEST_OPINION,
            mapOf(
                "FID_COND_MRKT_DIV_CODE" to "J",
                "FID_COND_SCR_DIV_CODE" to "16634",
                "FID_INPUT_ISCD" to brokerQueryCode,
                "FID_DIV_CLS_CODE" to "0",
                "FID_INPUT_DATE_1" to from.format(DateTimeFormatter.BASIC_ISO_DATE),
                "FID_INPUT_DATE_2" to to.format(DateTimeFormatter.BASIC_ISO_DATE),
            ),
        )
        val rtCd = json.path("rt_cd").asText("")
        if (rtCd != "0") {
            throw KisClientException(
                "invest opinion failed: keyId=${account.keyId} broker=$brokerQueryCode rt_cd=$rtCd msg_cd=${json.path("msg_cd").asText("")}",
            )
        }
        return json.path("output").mapNotNull { row ->
            val code = row.path("stck_shrn_iscd").asText("").trim()
            val businessDate = row.path("stck_bsop_date").asText("").trim()
            val rating = row.path("invt_opnn").asText("").trim()
            if (code.isEmpty() || businessDate.isEmpty() || rating.isEmpty()) {
                null
            } else {
                KisInvestOpinion(
                    code = code,
                    businessDate = businessDate,
                    rating = rating,
                    previousRating = row.path("rgbf_invt_opnn").asText("").trim().ifEmpty { null },
                    targetPrice = row.path("hts_goal_prc").asText("").trim().toLongOrNull()?.takeIf { it > 0 },
                    memberName = row.path("mbcr_name").asText("").trim().ifEmpty { null },
                )
            }
        }
    }

    internal fun getJson(account: KisAccount, path: String, trId: String, params: Map<String, String>): JsonNode {
        val first = send(account, path, trId, params)
        if (first.statusCode() == 401) {
            tokens.invalidate(account.keyId)
            return parse(send(account, path, trId, params), account, path)
        }
        return parse(first, account, path)
    }

    private fun send(account: KisAccount, path: String, trId: String, params: Map<String, String>): HttpResponse<String> {
        limiters.acquire(account.keyId)
        gate.acquire(account.keyId)
        val query = params.entries.joinToString("&") { "${encode(it.key)}=${encode(it.value)}" }
        val uri = URI.create(restBaseUrl + path + if (query.isEmpty()) "" else "?$query")
        val request = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(requestTimeout)
            .header("content-type", "application/json; charset=utf-8")
            .header("authorization", "Bearer ${tokens.accessToken(account)}")
            .header("appkey", account.appkey)
            .header("appsecret", account.appsecret)
            .header("tr_id", trId)
            .header("custtype", "P")
            .GET()
            .build()
        return http.send(request, HttpResponse.BodyHandlers.ofString())
    }

    private fun parse(response: HttpResponse<String>, account: KisAccount, path: String): JsonNode {
        if (response.statusCode() != 200) {
            throw KisClientException(
                "rest call failed: keyId=${account.keyId} path=$path status=${response.statusCode()}",
            )
        }
        return mapper.readTree(response.body())
    }

    private fun encode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)

    companion object {
        const val MARKET_DIV_KRX = "J"
        const val MARKET_DIV_UNIFIED = "UN"
        val CONNECT_TIMEOUT: Duration = Duration.ofSeconds(3)
        val REQUEST_TIMEOUT: Duration = Duration.ofSeconds(10)
        const val TR_INQUIRE_PRICE = "FHKST01010100"
        const val TR_DAILY_CHART = "FHKST03010100"
        const val TR_MINUTE_CHART = "FHKST03010200"
        const val TR_INQUIRE_INVESTOR = "FHKST01010900"
        const val TR_INVEST_OPINION = "FHKST663400C0"
        const val INVEST_OPINION_QUERY_CODE_LENGTH = 3
        const val INVEST_OPINION_PAGE_CAP = 100
        private const val INQUIRE_PRICE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"
        private const val DAILY_CHART_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        private const val MINUTE_CHART_PATH = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        private const val INQUIRE_INVESTOR_PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"
        private const val INVEST_OPINION_PATH = "/uapi/domestic-stock/v1/quotations/invest-opbysec"
    }
}
