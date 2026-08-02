package com.alphatalk.coreapi.stockinfo

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.io.Serializable

data class StockInfoCandleId(
    val code: String = "",
    val date: String = "",
) : Serializable

@Entity(name = "StockInfoCandle")
@Immutable
@Table(name = "daily_candle")
@IdClass(StockInfoCandleId::class)
class StockInfoCandleEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    val code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", length = 8, columnDefinition = "char(8)")
    val date: String = "",
    @Column(name = "open", nullable = false)
    val open: Int = 0,
    @Column(name = "high", nullable = false)
    val high: Int = 0,
    @Column(name = "low", nullable = false)
    val low: Int = 0,
    @Column(name = "close", nullable = false)
    val close: Int = 0,
    @Column(name = "volume", nullable = false)
    val volume: Long = 0,
    @Column(name = "value", nullable = false)
    val value: Long = 0,
)

interface StockInfoCandleJpaRepository : JpaRepository<StockInfoCandleEntity, StockInfoCandleId> {
    fun findByCode(code: String, pageable: PageRequest): List<StockInfoCandleEntity>

    fun findByCodeAndDateLessThanEqual(code: String, date: String, pageable: PageRequest): List<StockInfoCandleEntity>
}

@Repository
class JpaCandleStore(
    private val candles: StockInfoCandleJpaRepository,
) : CandleStore {
    override fun findLatestUpTo(code: String, toDate: String?, limit: Int): List<DailyCandle> {
        val page = PageRequest.of(0, limit, Sort.by(Sort.Direction.DESC, "date"))
        val rows = toDate
            ?.let { candles.findByCodeAndDateLessThanEqual(code, it, page) }
            ?: candles.findByCode(code, page)
        return rows.map {
            DailyCandle(
                date = it.date.trim(),
                open = it.open.toLong(),
                high = it.high.toLong(),
                low = it.low.toLong(),
                close = it.close.toLong(),
                volume = it.volume,
                value = it.value,
            )
        }
    }
}
