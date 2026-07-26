package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisDailyCandle
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class JdbcDailyCandleStore(
    private val jdbc: JdbcTemplate,
) : DailyCandleStore {
    override fun upsert(candles: List<KisDailyCandle>): Int {
        if (candles.isEmpty()) return 0
        jdbc.batchUpdate(
            """
            INSERT INTO daily_candle (code, date, open, high, low, close, volume, value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (code, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                value = EXCLUDED.value
            """.trimIndent(),
            candles,
            candles.size,
        ) { ps, candle ->
            ps.setString(1, candle.code)
            ps.setString(2, candle.date)
            ps.setLong(3, candle.open)
            ps.setLong(4, candle.high)
            ps.setLong(5, candle.low)
            ps.setLong(6, candle.close)
            ps.setLong(7, candle.volume)
            ps.setLong(8, candle.value)
        }
        return candles.size
    }

    override fun latestDate(code: String): String? =
        jdbc.queryForObject("SELECT max(date) FROM daily_candle WHERE code = ?", String::class.java, code)
}
