package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisStockMaster
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.DynamicUpdate
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.time.Instant
import java.time.LocalDate

@Entity
@Table(name = "stock_master")
@DynamicUpdate
class StockMasterEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @Column(name = "name", nullable = false)
    var name: String = "",
    @Column(name = "market", nullable = false)
    var market: String = "",
    @Column(name = "sector_code")
    var sectorCode: String? = null,
    @Column(name = "dart_induty_code")
    var dartIndutyCode: String? = null,
    @Column(name = "shares_outstanding")
    var sharesOutstanding: Long? = null,
    @Column(name = "is_active", nullable = false)
    var isActive: Boolean = true,
    @Column(name = "listed_at")
    var listedAt: LocalDate? = null,
    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.EPOCH,
)

interface StockMasterJpaRepository : JpaRepository<StockMasterEntity, String> {
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update StockMasterEntity s
        set s.isActive = false, s.updatedAt = :now
        where s.isActive = true and s.code not in :activeCodes
        """,
    )
    fun deactivateAllNotIn(@Param("activeCodes") activeCodes: Collection<String>, @Param("now") now: Instant): Int
}

@Repository
class JpaStockMasterStore(
    private val repository: StockMasterJpaRepository,
    private val entityManager: EntityManager,
    private val clock: Clock = Clock.systemUTC(),
) : StockMasterStore {
    @Transactional
    override fun upsertAll(stocks: List<KisStockMaster>): Int {
        if (stocks.isEmpty()) return 0
        val now = clock.instant()
        val existing = repository.findAllById(stocks.map(KisStockMaster::code)).associateBy(StockMasterEntity::code)
        stocks.forEach { stock ->
            val entity = existing[stock.code]
            if (entity == null) {
                entityManager.persist(
                    StockMasterEntity(
                        code = stock.code,
                        name = stock.name,
                        market = stock.market.name,
                        sharesOutstanding = stock.sharesOutstanding,
                        isActive = true,
                        listedAt = stock.listedAt,
                        updatedAt = now,
                    ),
                )
            } else {
                entity.apply {
                    name = stock.name
                    market = stock.market.name
                    sharesOutstanding = stock.sharesOutstanding
                    isActive = true
                    listedAt = stock.listedAt
                    updatedAt = now
                }
            }
        }
        return stocks.size
    }

    @Transactional
    override fun deactivateMissing(activeCodes: Collection<String>): Int {
        if (activeCodes.isEmpty()) return 0
        return repository.deactivateAllNotIn(activeCodes, clock.instant())
    }
}
