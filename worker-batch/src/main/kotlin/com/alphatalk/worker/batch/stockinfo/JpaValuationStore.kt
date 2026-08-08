package com.alphatalk.worker.batch.stockinfo

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable
import java.math.BigDecimal

@Embeddable
data class ValuationDailyId(
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", nullable = false, length = 8)
    val date: String = "",
) : Serializable

@Entity
@Table(name = "valuation_daily")
class ValuationDailyEntity(
    @EmbeddedId
    val id: ValuationDailyId = ValuationDailyId(),
    @Column(name = "per")
    var per: BigDecimal? = null,
    @Column(name = "pbr")
    var pbr: BigDecimal? = null,
    @Column(name = "eps")
    var eps: Int? = null,
    @Column(name = "bps")
    var bps: Int? = null,
    @Column(name = "market_cap")
    var marketCap: Long? = null,
)

interface ValuationDailyJpaRepository : JpaRepository<ValuationDailyEntity, ValuationDailyId>

@Repository
class JpaValuationStore(
    private val repository: ValuationDailyJpaRepository,
    private val entityManager: EntityManager,
) : ValuationStore {
    @Transactional
    override fun upsert(rows: List<ValuationRow>): Int {
        if (rows.isEmpty()) return 0
        val existing = repository.findAllById(rows.map { ValuationDailyId(it.code, it.date) })
            .associateBy(ValuationDailyEntity::id)
        rows.forEach { row ->
            val id = ValuationDailyId(code = row.code, date = row.date)
            val entity = existing[id]
            if (entity == null) {
                entityManager.persist(
                    ValuationDailyEntity(
                        id = id,
                        per = row.per,
                        pbr = row.pbr,
                        eps = row.eps,
                        bps = row.bps,
                        marketCap = row.marketCap,
                    ),
                )
            } else {
                entity.apply {
                    per = row.per
                    pbr = row.pbr
                    eps = row.eps
                    bps = row.bps
                    marketCap = row.marketCap
                }
            }
        }
        return rows.size
    }
}
