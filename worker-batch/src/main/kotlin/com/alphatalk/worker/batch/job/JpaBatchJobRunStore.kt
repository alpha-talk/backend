package com.alphatalk.worker.batch.job

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

@Entity
@Table(name = "batch_job_run")
class BatchJobRunEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null,
    @Column(name = "job", nullable = false)
    val job: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "run_date", nullable = false, length = 8)
    val runDate: String = "",
    @Column(name = "status", nullable = false)
    var status: String = "",
    @Column(name = "ok_count", nullable = false)
    var okCount: Int = 0,
    @Column(name = "fail_count", nullable = false)
    var failCount: Int = 0,
    @Column(name = "started_at", nullable = false)
    var startedAt: Instant = Instant.EPOCH,
    @Column(name = "finished_at")
    var finishedAt: Instant? = null,
    @Column(name = "error")
    var error: String? = null,
)

interface BatchJobRunJpaRepository : JpaRepository<BatchJobRunEntity, Long> {
    fun findByJobAndRunDate(job: String, runDate: String): BatchJobRunEntity?

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update BatchJobRunEntity r
        set r.status = :running, r.startedAt = :startedAt,
            r.finishedAt = null, r.error = null, r.okCount = 0, r.failCount = 0
        where r.job = :job and r.runDate = :runDate and r.status <> :success
        """,
    )
    fun restartUnlessSucceeded(
        @Param("job") job: String,
        @Param("runDate") runDate: String,
        @Param("startedAt") startedAt: Instant,
        @Param("running") running: String,
        @Param("success") success: String,
    ): Int
}

@Repository
class JpaBatchJobRunStore(
    private val repository: BatchJobRunJpaRepository,
) : BatchJobRunStore {
    @Transactional
    override fun start(job: String, runDate: String, startedAt: Instant): Long? {
        val existing = repository.findByJobAndRunDate(job, runDate)
            ?: return repository.save(
                BatchJobRunEntity(job = job, runDate = runDate, status = RUNNING, startedAt = startedAt),
            ).id
        val restarted = repository.restartUnlessSucceeded(job, runDate, startedAt, RUNNING, SUCCESS)
        return if (restarted == 1) existing.id else null
    }

    @Transactional
    override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
        repository.findById(id).ifPresent { run ->
            run.status = SUCCESS
            run.okCount = okCount
            run.failCount = failCount
            run.finishedAt = finishedAt
        }
    }

    @Transactional
    override fun fail(id: Long, error: String, finishedAt: Instant) {
        repository.findById(id).ifPresent { run ->
            run.status = FAILED
            run.error = error.take(ERROR_MAX_LENGTH)
            run.finishedAt = finishedAt
        }
    }

    companion object {
        private const val RUNNING = "RUNNING"
        private const val SUCCESS = "SUCCESS"
        private const val FAILED = "FAILED"
        private const val ERROR_MAX_LENGTH = 1000
    }
}
