package com.alphatalk.coreapi.community

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.Clock
import java.time.Instant

@Entity
@Table(name = "report")
class ReportEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "id", length = 26, columnDefinition = "char(26)")
    var id: String = "",
    @Column(name = "target_type", nullable = false)
    var targetType: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "target_id", nullable = false, length = 26, columnDefinition = "char(26)")
    var targetId: String = "",
    @Column(name = "reporter_id", nullable = false)
    var reporterId: Long = 0,
    @Column(name = "reason", nullable = false)
    var reason: String = "",
    @Column(name = "detail")
    var detail: String? = null,
    @Column(name = "status", nullable = false)
    var status: String = "RECEIVED",
    @Column(name = "created_at", nullable = false)
    var createdAt: Instant = Instant.EPOCH,
)

interface ReportJpaRepository : JpaRepository<ReportEntity, String>

@Repository
class JpaReportStore(
    private val reports: ReportJpaRepository,
    private val clock: Clock = Clock.systemUTC(),
) : ReportStore {
    override fun create(
        id: String,
        targetType: ReportTargetType,
        targetId: String,
        reporterId: Long,
        reason: ReportReason,
        detail: String?,
    ) {
        reports.save(
            ReportEntity(
                id = id,
                targetType = targetType.name,
                targetId = targetId,
                reporterId = reporterId,
                reason = reason.name,
                detail = detail,
                status = "RECEIVED",
                createdAt = clock.instant(),
            ),
        )
    }
}
