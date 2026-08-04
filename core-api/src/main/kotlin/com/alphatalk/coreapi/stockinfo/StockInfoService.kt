package com.alphatalk.coreapi.stockinfo

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import org.springframework.stereotype.Service
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

@Service
class StockInfoService(
    private val profiles: StockProfileStore,
    private val candles: CandleStore,
    private val minuteCandles: MinuteCandleStore,
    private val minuteRefresher: MinuteCandleRefresher,
    private val valuations: ValuationStore,
    private val financials: FinancialsStore,
    private val investors: InvestorFlowStore,
) {
    fun overview(rawCode: String): StockOverviewResponse {
        val code = validCode(rawCode)
        val profile = profiles.findActive(code) ?: throw unknownStock(code)
        return StockOverviewResponse(
            code = profile.code,
            name = profile.name,
            market = profile.market,
            sector = profile.sectorName,
            sharesOutstanding = profile.sharesOutstanding,
            listedAt = profile.listedAt?.format(DATE_FORMAT),
            updatedAt = profile.updatedAt.toEpochMilli(),
        )
    }

    fun candles(rawCode: String, rawPeriod: String?, rawCount: Int?, rawTo: String?): CandlesResponse {
        val code = validCode(rawCode)
        val trimmedPeriod = rawPeriod?.trim().orEmpty()
        MinutePeriod.fromToken(trimmedPeriod)?.let { return minuteCandles(code, it, rawCount, rawTo) }
        val period = validPeriod(trimmedPeriod)
        val count = validCount(rawCount)
        val to = validTo(rawTo)
        requireActive(code)
        val fetchLimit = CandleAggregator.fetchLimit(period, count)
        val daily = candles.findLatestUpTo(code, to, fetchLimit)
        val window = CandleAggregator.aggregate(daily, period, count, fetchLimit)
        return CandlesResponse(
            period = period.token,
            items = window.candles.map {
                CandleView(
                    date = it.date,
                    open = it.open,
                    high = it.high,
                    low = it.low,
                    close = it.close,
                    volume = it.volume,
                    value = it.value,
                )
            },
            pageInfo = CandlePageInfo(
                hasMoreBefore = window.hasMoreBefore,
                nextTo = CandleAggregator.nextTo(window, period),
            ),
        )
    }

    private fun minuteCandles(code: String, period: MinutePeriod, rawCount: Int?, rawTo: String?): CandlesResponse {
        val count = validCount(rawCount)
        val to = validMinuteTo(rawTo)
        requireActive(code)
        val today = LocalDate.now(SEOUL).format(DATE_FORMAT)
        if (to == null || to.first >= today) {
            runCatching { minuteRefresher.refresh(code) }
        }
        val fetchLimit = MinuteCandleAggregator.fetchLimit(period, count)
        val rows = minuteCandles.findLatestUpTo(code, to?.first, to?.second, fetchLimit)
        val window = MinuteCandleAggregator.aggregate(rows, period, count, fetchLimit)
        return CandlesResponse(
            period = period.token,
            items = window.buckets.map {
                CandleView(
                    date = it.date,
                    time = it.time,
                    open = it.open,
                    high = it.high,
                    low = it.low,
                    close = it.close,
                    volume = it.volume,
                    value = it.value,
                )
            },
            pageInfo = CandlePageInfo(
                hasMoreBefore = window.hasMoreBefore,
                nextTo = MinuteCandleAggregator.nextTo(window, period),
            ),
        )
    }

    fun valuation(rawCode: String): ValuationResponse {
        val code = validCode(rawCode)
        requireActive(code)
        val latest = valuations.findLatest(code)
            ?: throw ApiException(ErrorCode.NOT_FOUND, "밸류에이션 지표가 아직 없습니다", mapOf("code" to code))
        return ValuationResponse(
            per = latest.per?.toDouble(),
            pbr = latest.pbr?.toDouble(),
            eps = latest.eps,
            bps = latest.bps,
            marketCap = latest.marketCapWon?.let { Math.floorDiv(it, WON_PER_EOK) },
            asOf = latest.date,
        )
    }

    fun financials(rawCode: String, rawYears: Int?): FinancialsResponse {
        val code = validCode(rawCode)
        val years = validYears(rawYears)
        requireActive(code)
        val rows = financials.findAll(code)
        val selectedYears = rows.map(FinancialRecord::year).distinct().sortedDescending().take(years).toSet()
        val annual = rows
            .filter { it.year in selectedYears && it.reprtCode == ANNUAL_REPRT_CODE }
            .sortedByDescending(FinancialRecord::year)
            .map { it.toRow(it.year.toString()) }
        val quarterly = rows
            .filter { it.year in selectedYears && it.reprtCode in QUARTER_BY_REPRT_CODE }
            .sortedWith(compareByDescending(FinancialRecord::year).thenByDescending { QUARTER_BY_REPRT_CODE.getValue(it.reprtCode) })
            .map { it.toRow("${it.year}${QUARTER_BY_REPRT_CODE.getValue(it.reprtCode)}") }
        return FinancialsResponse(annual = annual, quarterly = quarterly)
    }

    fun investors(rawCode: String, rawDays: Int?): InvestorsResponse {
        val code = validCode(rawCode)
        val days = validDays(rawDays)
        requireActive(code)
        return InvestorsResponse(
            items = investors.findLatest(code, days).map {
                InvestorFlowView(
                    date = it.date,
                    individual = it.individual,
                    foreign = it.foreign,
                    institution = it.institution,
                )
            },
        )
    }

    private fun FinancialRecord.toRow(period: String) = FinancialRow(
        period = period,
        revenue = revenue?.let { Math.floorDiv(it, WON_PER_EOK) },
        operatingProfit = operatingProfit?.let { Math.floorDiv(it, WON_PER_EOK) },
        netIncome = netIncome?.let { Math.floorDiv(it, WON_PER_EOK) },
        assets = assets?.let { Math.floorDiv(it, WON_PER_EOK) },
        liabilities = liabilities?.let { Math.floorDiv(it, WON_PER_EOK) },
        equity = equity?.let { Math.floorDiv(it, WON_PER_EOK) },
        source = FINANCIALS_SOURCE,
        asOf = disclosedAt.atZone(SEOUL).toLocalDate().format(DATE_FORMAT),
    )

    private fun requireActive(code: String) {
        if (!profiles.existsActive(code)) throw unknownStock(code)
    }

    private fun unknownStock(code: String) =
        ApiException(ErrorCode.NOT_FOUND, "존재하지 않는 종목입니다", mapOf("code" to code))

    private fun validCode(code: String): String {
        if (!CODE_PATTERN.matches(code)) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "종목 코드는 6자리 숫자여야 합니다", mapOf("field" to "code"))
        }
        return code
    }

    private fun validPeriod(trimmed: String): CandlePeriod {
        if (trimmed.isEmpty()) return CandlePeriod.DAILY
        return CandlePeriod.fromToken(trimmed)
            ?: throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "period는 D, W, M, 1m, 5m, 15m, 30m, 60m 중 하나여야 합니다",
                mapOf("field" to "period"),
            )
    }

    private fun validMinuteTo(to: String?): Pair<String, String>? {
        val trimmed = to?.trim().orEmpty()
        if (trimmed.isEmpty()) return null
        if (!MINUTE_TO_PATTERN.matches(trimmed)) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "분봉의 to는 yyyyMMddHHmm 형식이어야 합니다",
                mapOf("field" to "to"),
            )
        }
        return trimmed.take(8) to trimmed.drop(8)
    }

    private fun validCount(count: Int?): Int {
        val value = count ?: DEFAULT_COUNT
        if (value < MIN_COUNT || value > MAX_COUNT) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "count는 $MIN_COUNT~$MAX_COUNT 사이여야 합니다",
                mapOf("field" to "count"),
            )
        }
        return value
    }

    private fun validTo(to: String?): String? {
        val trimmed = to?.trim().orEmpty()
        if (trimmed.isEmpty()) return null
        if (!DATE_PATTERN.matches(trimmed)) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "to는 yyyyMMdd 형식이어야 합니다", mapOf("field" to "to"))
        }
        return trimmed
    }

    private fun validYears(years: Int?): Int {
        val value = years ?: DEFAULT_YEARS
        if (value < MIN_YEARS || value > MAX_YEARS) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "years는 $MIN_YEARS~$MAX_YEARS 사이여야 합니다",
                mapOf("field" to "years"),
            )
        }
        return value
    }

    private fun validDays(days: Int?): Int {
        val value = days ?: DEFAULT_DAYS
        if (value < MIN_DAYS || value > MAX_DAYS) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "days는 $MIN_DAYS~$MAX_DAYS 사이여야 합니다",
                mapOf("field" to "days"),
            )
        }
        return value
    }

    companion object {
        const val DEFAULT_COUNT = 100
        const val MIN_COUNT = 1
        const val MAX_COUNT = 500
        const val DEFAULT_YEARS = 3
        const val MIN_YEARS = 1
        const val MAX_YEARS = 10
        const val DEFAULT_DAYS = 20
        const val MIN_DAYS = 1
        const val MAX_DAYS = 250
        const val WON_PER_EOK = 100_000_000L
        const val ANNUAL_REPRT_CODE = "11011"
        const val FINANCIALS_SOURCE = "DART"
        val QUARTER_BY_REPRT_CODE = mapOf(
            "11013" to "Q1",
            "11012" to "Q2",
            "11014" to "Q3",
        )
        private val CODE_PATTERN = Regex("^\\d{6}$")
        private val DATE_PATTERN = Regex("^\\d{8}$")
        private val MINUTE_TO_PATTERN = Regex("^\\d{12}$")
        private val DATE_FORMAT = DateTimeFormatter.BASIC_ISO_DATE
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
    }
}
