package com.alphatalk.worker.price.candle

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.slf4j.LoggerFactory
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query

@Entity
@Table(name = "stock_master")
class StockMasterEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @Column(name = "is_active", nullable = false)
    val active: Boolean = true,
)

interface StockMasterCodeRepository : JpaRepository<StockMasterEntity, String> {
    @Query("select s.code from StockMasterEntity s where s.active = true")
    fun findActiveCodes(): List<String>
}

class MasterCandleUniverse(
    private val activeCodes: () -> List<String>,
    private val fallback: () -> Set<String>,
) : CandleUniverse {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun symbols(): Set<String> {
        val codes = try {
            activeCodes()
        } catch (e: Exception) {
            log.warn("stock_master lookup failed - falling back to demanded symbols", e)
            return fallback()
        }
        if (codes.isEmpty()) {
            log.warn("stock_master is empty - falling back to demanded symbols")
            return fallback()
        }
        return codes.toSet()
    }
}
