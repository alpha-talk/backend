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
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable

data class StockInfoMinuteCandleId(
    val code: String = "",
    val date: String = "",
    val time: String = "",
) : Serializable

@Entity(name = "StockInfoMinuteCandle")
@Immutable
@Table(name = "minute_candle")
@IdClass(StockInfoMinuteCandleId::class)
class StockInfoMinuteCandleEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    val code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", length = 8, columnDefinition = "char(8)")
    val date: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "time", length = 4, columnDefinition = "char(4)")
    val time: String = "",
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

interface StockInfoMinuteCandleJpaRepository : JpaRepository<StockInfoMinuteCandleEntity, StockInfoMinuteCandleId> {
    fun findByCode(code: String, pageable: PageRequest): List<StockInfoMinuteCandleEntity>

    @Query(
        "select c from StockInfoMinuteCandle c where c.code = :code " +
            "and (c.date < :date or (c.date = :date and c.time <= :time))",
    )
    fun findUpTo(
        @Param("code") code: String,
        @Param("date") date: String,
        @Param("time") time: String,
        pageable: PageRequest,
    ): List<StockInfoMinuteCandleEntity>
}

@Repository
@Transactional(readOnly = true)
class JpaMinuteCandleStore(
    private val candles: StockInfoMinuteCandleJpaRepository,
) : MinuteCandleStore {
    override fun findLatestUpTo(code: String, toDate: String?, toTime: String?, limit: Int): List<MinuteCandleRow> {
        val page = PageRequest.of(0, limit, Sort.by(Sort.Direction.DESC, "date", "time"))
        val rows = if (toDate != null && toTime != null) {
            candles.findUpTo(code, toDate, toTime, page)
        } else {
            candles.findByCode(code, page)
        }
        return rows.map {
            MinuteCandleRow(
                date = it.date.trim(),
                time = it.time.trim(),
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
