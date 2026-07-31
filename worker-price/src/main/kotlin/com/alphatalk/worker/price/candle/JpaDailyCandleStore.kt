package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisDailyCandle
import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable

@Embeddable
data class DailyCandleId(
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", nullable = false, length = 8)
    val date: String = "",
) : Serializable

@Entity
@Table(name = "daily_candle")
class DailyCandleEntity(
    @EmbeddedId
    val id: DailyCandleId = DailyCandleId(),
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
    @Column(name = "value", nullable = false)
    var tradedValue: Long = 0,
)

interface DailyCandleJpaRepository : JpaRepository<DailyCandleEntity, DailyCandleId> {
    @Query("select max(c.id.date) from DailyCandleEntity c where c.id.code = :code")
    fun findLatestDate(@Param("code") code: String): String?
}

@Repository
class JpaDailyCandleStore(
    private val repository: DailyCandleJpaRepository,
    private val entityManager: EntityManager,
) : DailyCandleStore {
    @Transactional
    override fun upsert(candles: List<KisDailyCandle>): Int {
        if (candles.isEmpty()) return 0
        val existing = repository.findAllById(candles.map { DailyCandleId(it.code, it.date) })
            .associateBy(DailyCandleEntity::id)
        candles.forEach { candle ->
            val id = DailyCandleId(code = candle.code, date = candle.date)
            val entity = existing[id]
            if (entity == null) {
                entityManager.persist(
                    DailyCandleEntity(
                        id = id,
                        open = Math.toIntExact(candle.open),
                        high = Math.toIntExact(candle.high),
                        low = Math.toIntExact(candle.low),
                        close = Math.toIntExact(candle.close),
                        volume = candle.volume,
                        tradedValue = candle.value,
                    ),
                )
            } else {
                entity.apply {
                    open = Math.toIntExact(candle.open)
                    high = Math.toIntExact(candle.high)
                    low = Math.toIntExact(candle.low)
                    close = Math.toIntExact(candle.close)
                    volume = candle.volume
                    tradedValue = candle.value
                }
            }
        }
        return candles.size
    }

    @Transactional(readOnly = true)
    override fun latestDate(code: String): String? = repository.findLatestDate(code)
}
