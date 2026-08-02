package com.alphatalk.coreapi.stream

import com.alphatalk.contracts.Keys
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.slf4j.LoggerFactory
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Repository
import java.io.Serializable
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

data class DailyCandleId(
    val code: String = "",
    val date: String = "",
) : Serializable

@Entity
@Immutable
@Table(name = "daily_candle")
@IdClass(DailyCandleId::class)
class DailyCandleEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    var code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", length = 8, columnDefinition = "char(8)")
    var date: String = "",
    @Column(name = "open", nullable = false)
    var open: Int = 0,
    @Column(name = "high", nullable = false)
    var high: Int = 0,
    @Column(name = "low", nullable = false)
    var low: Int = 0,
    @Column(name = "close", nullable = false)
    var close: Int = 0,
    @Column(name = "volume", nullable = false)
    var volume: Long = 0,
)

interface DailyCandleJpaRepository : JpaRepository<DailyCandleEntity, DailyCandleId> {
    fun findTop2ByCodeOrderByDateDesc(code: String): List<DailyCandleEntity>
}

@Repository
class RedisQuoteStore(
    private val redis: StringRedisTemplate,
    private val candles: DailyCandleJpaRepository,
) : QuoteStore {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun liveQuote(code: String): QuoteResponse? {
        val hash = runCatching {
            redis.opsForHash<String, String>().entries(Keys.price(code))
        }.getOrElse {
            log.warn("live quote cache read failed: code={}", code, it)
            return null
        }
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
        }.getOrElse {
            log.warn("live quote cache payload is invalid: code={} fields={}", code, hash.keys.sorted(), it)
            null
        }
    }

    override fun lastCandleQuote(code: String): QuoteResponse? =
        candles.findTop2ByCodeOrderByDateDesc(code).takeIf { it.isNotEmpty() }?.let(::toQuote)

    private fun toQuote(candles: List<DailyCandleEntity>): QuoteResponse {
        val latest = candles.first()
        val prevClose = candles.getOrNull(1)?.close?.toLong() ?: latest.close.toLong()
        val change = latest.close - prevClose
        return QuoteResponse(
            code = latest.code.trim(),
            price = latest.close.toLong(),
            prevClose = prevClose,
            change = change,
            changeRate = if (prevClose == 0L) 0.0 else round2(change * 100.0 / prevClose),
            open = latest.open.toLong(),
            high = latest.high.toLong(),
            low = latest.low.toLong(),
            volume = latest.volume,
            ts = LocalDate.parse(latest.date.trim(), DateTimeFormatter.BASIC_ISO_DATE)
                .atTime(MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE)
                .atZone(SEOUL)
                .toInstant()
                .toEpochMilli(),
            delayed = true,
        )
    }

    private fun round2(value: Double): Double = Math.round(value * 100.0) / 100.0

    companion object {
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private const val MARKET_CLOSE_HOUR = 15
        private const val MARKET_CLOSE_MINUTE = 30
    }
}
