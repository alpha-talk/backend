package com.alphatalk.worker.batch.job

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.dao.DataIntegrityViolationException
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

    @Transactional
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

    @Transactional
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update BatchJobRunEntity r
        set r.status = :running, r.startedAt = :startedAt,
            r.finishedAt = null, r.error = null, r.okCount = 0, r.failCount = 0
        where r.job = :job and r.runDate = :runDate
        """,
    )
    fun restartAlways(
        @Param("job") job: String,
        @Param("runDate") runDate: String,
        @Param("startedAt") startedAt: Instant,
        @Param("running") running: String,
    ): Int

    @Transactional
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update BatchJobRunEntity r
        set r.status = :status, r.okCount = :okCount, r.failCount = :failCount, r.finishedAt = :finishedAt
        where r.id = :id
        """,
    )
    fun finishCounted(
        @Param("id") id: Long,
        @Param("status") status: String,
        @Param("okCount") okCount: Int,
        @Param("failCount") failCount: Int,
        @Param("finishedAt") finishedAt: Instant,
    ): Int

    @Transactional
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update BatchJobRunEntity r
        set r.status = :status, r.error = :error, r.finishedAt = :finishedAt
        where r.id = :id
        """,
    )
    fun finishWithError(
        @Param("id") id: Long,
        @Param("status") status: String,
        @Param("error") error: String,
        @Param("finishedAt") finishedAt: Instant,
    ): Int

    @Transactional
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update BatchJobRunEntity r
        set r.status = :status, r.okCount = :okCount, r.failCount = :failCount,
            r.error = :error, r.finishedAt = :finishedAt
        where r.id = :id
        """,
    )
    fun finishWithErrorCounted(
        @Param("id") id: Long,
        @Param("status") status: String,
        @Param("okCount") okCount: Int,
        @Param("failCount") failCount: Int,
        @Param("error") error: String,
        @Param("finishedAt") finishedAt: Instant,
    ): Int
}

@Repository
class JpaBatchJobRunStore(
    private val repository: BatchJobRunJpaRepository,
) : BatchJobRunStore {
    override fun start(job: String, runDate: String, startedAt: Instant): Long? {
        val existing = repository.findByJobAndRunDate(job, runDate)
            ?: return insertRunning(job, runDate, startedAt) ?: restartAfterLostRace(job, runDate, startedAt)
        return if (restarted(job, runDate, startedAt)) existing.id else null
    }

    override fun restart(job: String, runDate: String, startedAt: Instant): Long {
        if (repository.restartAlways(job, runDate, startedAt, RUNNING) == 1) {
            return requireNotNull(repository.findByJobAndRunDate(job, runDate)?.id)
        }
        insertRunning(job, runDate, startedAt)?.let { return it }
        check(repository.restartAlways(job, runDate, startedAt, RUNNING) == 1) {
            "batch_job_run restart lost race twice: job=$job runDate=$runDate"
        }
        return requireNotNull(repository.findByJobAndRunDate(job, runDate)?.id)
    }

    override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
        repository.finishCounted(id, SUCCESS, okCount, failCount, finishedAt)
    }

    override fun fail(id: Long, error: String, finishedAt: Instant) {
        repository.finishWithError(id, FAILED, error.take(ERROR_MAX_LENGTH), finishedAt)
    }

    override fun failCounted(id: Long, okCount: Int, failCount: Int, error: String, finishedAt: Instant) {
        repository.finishWithErrorCounted(id, FAILED, okCount, failCount, error.take(ERROR_MAX_LENGTH), finishedAt)
    }

    private fun insertRunning(job: String, runDate: String, startedAt: Instant): Long? =
        try {
            repository.save(
                BatchJobRunEntity(job = job, runDate = runDate, status = RUNNING, startedAt = startedAt),
            ).id
        } catch (e: DataIntegrityViolationException) {
            repository.findByJobAndRunDate(job, runDate) ?: throw e
            null
        }

    private fun restartAfterLostRace(job: String, runDate: String, startedAt: Instant): Long? =
        if (restarted(job, runDate, startedAt)) repository.findByJobAndRunDate(job, runDate)?.id else null

    private fun restarted(job: String, runDate: String, startedAt: Instant): Boolean =
        repository.restartUnlessSucceeded(job, runDate, startedAt, RUNNING, SUCCESS) == 1

    companion object {
        private const val RUNNING = "RUNNING"
        private const val SUCCESS = "SUCCESS"
        private const val FAILED = "FAILED"
        private const val ERROR_MAX_LENGTH = 1000
    }
}
