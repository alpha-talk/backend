package com.alphatalk.worker.price.candle

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable

@Embeddable
data class MinuteCandleId(
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", nullable = false, length = 8)
    val date: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "time", nullable = false, length = 4)
    val time: String = "",
) : Serializable

@Entity
@Table(name = "minute_candle")
class MinuteCandleEntity(
    @EmbeddedId
    val id: MinuteCandleId = MinuteCandleId(),
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

interface MinuteCandleJpaRepository : JpaRepository<MinuteCandleEntity, MinuteCandleId> {
    @Query("select max(c.id.time) from MinuteCandleEntity c where c.id.code = :code and c.id.date = :date")
    fun findLatestTime(@Param("code") code: String, @Param("date") date: String): String?

    @Query(
        "select coalesce(sum(c.tradedValue), 0) from MinuteCandleEntity c " +
            "where c.id.code = :code and c.id.date = :date and c.id.time < :time",
    )
    fun sumValueBefore(@Param("code") code: String, @Param("date") date: String, @Param("time") time: String): Long

    @Query("select distinct c.id.code from MinuteCandleEntity c where c.id.date = :date")
    fun findCodesOn(@Param("date") date: String): List<String>

    @Modifying
    @Query("delete from MinuteCandleEntity c where c.id.date < :date")
    fun deleteBefore(@Param("date") date: String): Int
}

@Repository
class JpaMinuteCandleStore(
    private val repository: MinuteCandleJpaRepository,
    private val entityManager: EntityManager,
) : MinuteCandleStore {
    @Transactional
    override fun upsert(candles: List<MinuteCandle>): Int {
        if (candles.isEmpty()) return 0
        val existing = repository.findAllById(candles.map { MinuteCandleId(it.code, it.date, it.time) })
            .associateBy(MinuteCandleEntity::id)
        candles.forEach { candle ->
            val id = MinuteCandleId(code = candle.code, date = candle.date, time = candle.time)
            val entity = existing[id]
            if (entity == null) {
                entityManager.persist(
                    MinuteCandleEntity(
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
    override fun latestTime(code: String, date: String): String? = repository.findLatestTime(code, date)

    @Transactional(readOnly = true)
    override fun sumValueBefore(code: String, date: String, timeExclusive: String): Long =
        repository.sumValueBefore(code, date, timeExclusive)

    @Transactional(readOnly = true)
    override fun codesOn(date: String): Set<String> = repository.findCodesOn(date).toSet()

    @Transactional
    override fun purgeBefore(dateExclusive: String): Int = repository.deleteBefore(dateExclusive)
}
