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
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable

data class InvestorFlowId(
    val code: String = "",
    val date: String = "",
) : Serializable

@Entity
@Immutable
@Table(name = "investor_flow_daily")
@IdClass(InvestorFlowId::class)
class InvestorFlowEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    val code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", length = 8, columnDefinition = "char(8)")
    val date: String = "",
    @Column(name = "individual", nullable = false)
    val individual: Long = 0,
    @Column(name = "`foreign`", nullable = false)
    val foreign: Long = 0,
    @Column(name = "institution", nullable = false)
    val institution: Long = 0,
)

interface InvestorFlowJpaRepository : JpaRepository<InvestorFlowEntity, InvestorFlowId> {
    fun findByCode(code: String, pageable: PageRequest): List<InvestorFlowEntity>
}

@Repository
@Transactional(readOnly = true)
class JpaInvestorFlowStore(
    private val flows: InvestorFlowJpaRepository,
) : InvestorFlowStore {
    override fun findLatest(code: String, days: Int): List<InvestorFlowRecord> =
        flows.findByCode(code, PageRequest.of(0, days, Sort.by(Sort.Direction.DESC, "date")))
            .map {
                InvestorFlowRecord(
                    date = it.date.trim(),
                    individual = it.individual,
                    foreign = it.foreign,
                    institution = it.institution,
                )
            }
}
