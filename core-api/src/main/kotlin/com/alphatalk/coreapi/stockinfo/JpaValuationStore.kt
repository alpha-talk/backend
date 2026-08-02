package com.alphatalk.coreapi.stockinfo

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.io.Serializable
import java.math.BigDecimal

data class ValuationDailyId(
    val code: String = "",
    val date: String = "",
) : Serializable

@Entity
@Immutable
@Table(name = "valuation_daily")
@IdClass(ValuationDailyId::class)
class ValuationDailyEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    val code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", length = 8, columnDefinition = "char(8)")
    val date: String = "",
    @Column(name = "per")
    val per: BigDecimal? = null,
    @Column(name = "pbr")
    val pbr: BigDecimal? = null,
    @Column(name = "eps")
    val eps: Int? = null,
    @Column(name = "bps")
    val bps: Int? = null,
    @Column(name = "market_cap")
    val marketCap: Long? = null,
)

interface ValuationDailyJpaRepository : JpaRepository<ValuationDailyEntity, ValuationDailyId> {
    fun findTopByCodeOrderByDateDesc(code: String): ValuationDailyEntity?
}

@Repository
class JpaValuationStore(
    private val valuations: ValuationDailyJpaRepository,
) : ValuationStore {
    override fun findLatest(code: String): ValuationRecord? =
        valuations.findTopByCodeOrderByDateDesc(code)?.let {
            ValuationRecord(
                date = it.date.trim(),
                per = it.per,
                pbr = it.pbr,
                eps = it.eps,
                bps = it.bps,
                marketCapWon = it.marketCap,
            )
        }
}
