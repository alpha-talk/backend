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
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable
import java.time.Instant

data class FinancialSummaryId(
    val code: String = "",
    val year: Short = 0,
    val reprtCode: String = "",
) : Serializable

@Entity
@Immutable
@Table(name = "financial_summary")
@IdClass(FinancialSummaryId::class)
class FinancialSummaryEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    val code: String = "",
    @Id
    @Column(name = "year", nullable = false)
    val year: Short = 0,
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "reprt_code", length = 5, columnDefinition = "char(5)")
    val reprtCode: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "fs_div", nullable = false, length = 3, columnDefinition = "char(3)")
    val fsDiv: String = "",
    @Column(name = "revenue")
    val revenue: Long? = null,
    @Column(name = "operating_profit")
    val operatingProfit: Long? = null,
    @Column(name = "net_income")
    val netIncome: Long? = null,
    @Column(name = "assets")
    val assets: Long? = null,
    @Column(name = "liabilities")
    val liabilities: Long? = null,
    @Column(name = "equity")
    val equity: Long? = null,
    @Column(name = "disclosed_at")
    val disclosedAt: Instant? = null,
)

interface FinancialSummaryJpaRepository : JpaRepository<FinancialSummaryEntity, FinancialSummaryId> {
    fun findByCodeOrderByYearDesc(code: String): List<FinancialSummaryEntity>
}

@Repository
@Transactional(readOnly = true)
class JpaFinancialsStore(
    private val financials: FinancialSummaryJpaRepository,
) : FinancialsStore {
    override fun findAll(code: String): List<FinancialRecord> =
        financials.findByCodeOrderByYearDesc(code).map {
            FinancialRecord(
                year = it.year.toInt(),
                reprtCode = it.reprtCode.trim(),
                fsDiv = it.fsDiv.trim(),
                revenue = it.revenue,
                operatingProfit = it.operatingProfit,
                netIncome = it.netIncome,
                assets = it.assets,
                liabilities = it.liabilities,
                equity = it.equity,
                disclosedAt = it.disclosedAt,
            )
        }
}
