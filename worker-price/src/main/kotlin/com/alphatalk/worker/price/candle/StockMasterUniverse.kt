package com.alphatalk.worker.price.candle

import com.alphatalk.worker.price.session.BackoffPolicy
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

class CandleUniverseUnavailableException(attempts: Int, cause: Throwable) :
    IllegalStateException("stock_master 조회가 ${attempts}회 연속 실패했다", cause)

class MasterCandleUniverse(
    private val activeCodes: () -> List<String>,
    private val fallback: () -> Set<String>,
    private val maxAttempts: Int = 3,
    private val backoff: BackoffPolicy = BackoffPolicy(initialMillis = 2_000, maxMillis = 30_000),
    private val sleep: (Long) -> Unit = Thread::sleep,
) : CandleUniverse {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun symbols(): Set<String> {
        var lastFailure: Throwable? = null
        repeat(maxAttempts) { attempt ->
            try {
                val codes = activeCodes()
                if (codes.isNotEmpty()) return codes.toSet()
                log.warn("stock_master is empty - falling back to demanded symbols (초기 구축 전)")
                return fallback()
            } catch (e: Exception) {
                lastFailure = e
                log.warn("stock_master lookup failed ({}/{})", attempt + 1, maxAttempts, e)
                if (attempt < maxAttempts - 1) sleep(backoff.delayFor(attempt + 1))
            }
        }
        throw CandleUniverseUnavailableException(maxAttempts, lastFailure!!)
    }
}
