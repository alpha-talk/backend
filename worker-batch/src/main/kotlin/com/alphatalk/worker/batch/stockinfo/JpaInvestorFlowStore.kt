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

@Embeddable
data class InvestorFlowId(
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", nullable = false, length = 8)
    val date: String = "",
) : Serializable

@Entity
@Table(name = "investor_flow_daily")
class InvestorFlowEntity(
    @EmbeddedId
    val id: InvestorFlowId = InvestorFlowId(),
    @Column(name = "individual", nullable = false)
    var individual: Long = 0,
    @Column(name = "`foreign`", nullable = false)
    var foreign: Long = 0,
    @Column(name = "institution", nullable = false)
    var institution: Long = 0,
)

interface InvestorFlowJpaRepository : JpaRepository<InvestorFlowEntity, InvestorFlowId>

@Repository
class JpaInvestorFlowStore(
    private val repository: InvestorFlowJpaRepository,
    private val entityManager: EntityManager,
) : InvestorFlowStore {
    @Transactional
    override fun upsert(rows: List<InvestorFlowRow>): Int {
        if (rows.isEmpty()) return 0
        val existing = repository.findAllById(rows.map { InvestorFlowId(it.code, it.date) })
            .associateBy(InvestorFlowEntity::id)
        rows.forEach { row ->
            val id = InvestorFlowId(code = row.code, date = row.date)
            val entity = existing[id]
            if (entity == null) {
                entityManager.persist(
                    InvestorFlowEntity(
                        id = id,
                        individual = row.individual,
                        foreign = row.foreign,
                        institution = row.institution,
                    ),
                )
            } else {
                entity.apply {
                    individual = row.individual
                    foreign = row.foreign
                    institution = row.institution
                }
            }
        }
        return rows.size
    }
}
