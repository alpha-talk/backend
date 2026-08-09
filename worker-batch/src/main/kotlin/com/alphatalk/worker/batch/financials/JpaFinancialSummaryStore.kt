package com.alphatalk.worker.batch.financials

import com.alphatalk.worker.batch.industry.DartCorpMapEntity
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
import java.time.Instant

@Embeddable
data class FinancialSummaryId(
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @Column(name = "year", nullable = false)
    val year: Short = 0,
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "reprt_code", nullable = false, length = 5)
    val reprtCode: String = "",
) : Serializable

@Entity
@Table(name = "financial_summary")
class FinancialSummaryEntity(
    @EmbeddedId
    val id: FinancialSummaryId = FinancialSummaryId(),
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "fs_div", nullable = false, length = 3)
    var fsDiv: String = "",
    @Column(name = "revenue")
    var revenue: Long? = null,
    @Column(name = "operating_profit")
    var operatingProfit: Long? = null,
    @Column(name = "net_income")
    var netIncome: Long? = null,
    @Column(name = "assets")
    var assets: Long? = null,
    @Column(name = "liabilities")
    var liabilities: Long? = null,
    @Column(name = "equity")
    var equity: Long? = null,
    @Column(name = "disclosed_at", nullable = false)
    var disclosedAt: Instant = Instant.EPOCH,
)

interface FinancialSummaryJpaRepository : JpaRepository<FinancialSummaryEntity, FinancialSummaryId> {
    @Query("select distinct trim(f.id.code) from FinancialSummaryEntity f where f.id.year <= :year")
    fun distinctCodesWithYearAtMost(@Param("year") year: Short): List<String>
}

interface FinancialCorpMapJpaRepository : JpaRepository<DartCorpMapEntity, String> {
    @Query(
        """
        select m from DartCorpMapEntity m
        where m.code in :codes
        """,
    )
    fun findByCodes(@Param("codes") codes: Collection<String>): List<DartCorpMapEntity>
}

@Repository
class JpaCorpDirectory(
    private val repository: FinancialCorpMapJpaRepository,
) : CorpDirectory {
    override fun corpCodesFor(codes: Collection<String>): Map<String, String> {
        if (codes.isEmpty()) return emptyMap()
        return codes.chunked(CHUNK).flatMap(repository::findByCodes)
            .mapNotNull { entity -> entity.code?.let { it.trim() to entity.corpCode } }
            .toMap()
    }

    private companion object {
        const val CHUNK = 1000
    }
}

@Repository
class JpaFinancialSummaryStore(
    private val repository: FinancialSummaryJpaRepository,
    private val entityManager: EntityManager,
) : FinancialSummaryStore {
    override fun codesWithRowOnOrBefore(year: Int): Set<String> =
        repository.distinctCodesWithYearAtMost(year.toShort()).toSet()

    @Transactional
    override fun upsertAll(rows: List<FinancialSummaryRow>) {
        rows.forEach(::upsert)
    }

    @Transactional
    override fun upsert(row: FinancialSummaryRow) {
        val id = FinancialSummaryId(code = row.code, year = row.year.toShort(), reprtCode = row.reprtCode)
        val entity = repository.findById(id).orElse(null)
        if (entity == null) {
            entityManager.persist(
                FinancialSummaryEntity(
                    id = id,
                    fsDiv = row.fsDiv,
                    revenue = row.figures.revenue,
                    operatingProfit = row.figures.operatingProfit,
                    netIncome = row.figures.netIncome,
                    assets = row.figures.assets,
                    liabilities = row.figures.liabilities,
                    equity = row.figures.equity,
                    disclosedAt = row.disclosedAt,
                ),
            )
        } else {
            entity.apply {
                fsDiv = row.fsDiv
                revenue = row.figures.revenue
                operatingProfit = row.figures.operatingProfit
                netIncome = row.figures.netIncome
                assets = row.figures.assets
                liabilities = row.figures.liabilities
                equity = row.figures.equity
                disclosedAt = row.disclosedAt
            }
        }
    }
}
