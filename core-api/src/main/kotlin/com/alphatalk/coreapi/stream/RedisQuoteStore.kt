package com.alphatalk.coreapi.stream

import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

@Repository
class RedisQuoteStore(
    private val redis: StringRedisTemplate,
    private val jdbc: JdbcTemplate,
) : QuoteStore {
    override fun liveQuote(code: String): QuoteResponse? {
        val hash = redis.opsForHash<String, String>().entries(Keys.price(code))
        if (hash.isEmpty()) return null
        return runCatching {
            QuoteResponse(
                code = code,
                price = hash.getValue("price").toLong(),
                prevClose = hash.getValue("prevClose").toLong(),
                change = hash.getValue("change").toLong(),
                changeRate = hash.getValue("changeRate").toDouble(),
                open = hash.getValue("open").toLong(),
                high = hash.getValue("high").toLong(),
                low = hash.getValue("low").toLong(),
                volume = hash.getValue("volume").toLong(),
                ts = hash.getValue("ts").toLong(),
                delayed = false,
            )
        }.getOrNull()
    }

    override fun lastCandleQuote(code: String): QuoteResponse? =
        jdbc.query(LAST_TWO_CANDLES, ::mapCandle, code).takeIf { it.isNotEmpty() }?.let(::toQuote)

    private fun toQuote(candles: List<Candle>): QuoteResponse {
        val latest = candles.first()
        val prevClose = candles.getOrNull(1)?.close ?: latest.close
        val change = latest.close - prevClose
        return QuoteResponse(
            code = latest.code,
            price = latest.close,
            prevClose = prevClose,
            change = change,
            changeRate = if (prevClose == 0L) 0.0 else round2(change * 100.0 / prevClose),
            open = latest.open,
            high = latest.high,
            low = latest.low,
            volume = latest.volume,
            ts = latest.date.atStartOfDay(SEOUL).toInstant().toEpochMilli(),
            delayed = true,
        )
    }

    private fun round2(value: Double): Double = Math.round(value * 100.0) / 100.0

    private data class Candle(
        val code: String,
        val date: LocalDate,
        val open: Long,
        val high: Long,
        val low: Long,
        val close: Long,
        val volume: Long,
    )

    private fun mapCandle(rows: ResultSet, rowNum: Int) = Candle(
        code = rows.getString("code").trim(),
        date = LocalDate.parse(rows.getString("date"), DateTimeFormatter.BASIC_ISO_DATE),
        open = rows.getLong("open"),
        high = rows.getLong("high"),
        low = rows.getLong("low"),
        close = rows.getLong("close"),
        volume = rows.getLong("volume"),
    )

    companion object {
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private val LAST_TWO_CANDLES = """
            SELECT code, date, open, high, low, close, volume
            FROM daily_candle
            WHERE code = ?
            ORDER BY date DESC
            LIMIT 2
        """.trimIndent()
    }
}
