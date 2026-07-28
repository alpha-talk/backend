package com.alphatalk.worker.batch.job

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.sql.Timestamp
import java.time.Instant

@Component
class JdbcBatchJobRunStore(
    private val jdbc: JdbcTemplate,
) : BatchJobRunStore {
    override fun start(job: String, runDate: String, startedAt: Instant): Long? =
        jdbc.query(
            """
            INSERT INTO batch_job_run (job, run_date, status, started_at)
            VALUES (?, ?, '$RUNNING', ?)
            ON CONFLICT (job, run_date) DO UPDATE SET
                status = '$RUNNING',
                started_at = EXCLUDED.started_at,
                finished_at = NULL,
                error = NULL,
                ok_count = 0,
                fail_count = 0
            WHERE batch_job_run.status <> '$SUCCESS'
            RETURNING id
            """.trimIndent(),
            { rows, _ -> rows.getLong("id") },
            job,
            runDate,
            Timestamp.from(startedAt),
        ).firstOrNull()

    override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
        jdbc.update(
            "UPDATE batch_job_run SET status = '$SUCCESS', ok_count = ?, fail_count = ?, finished_at = ? WHERE id = ?",
            okCount,
            failCount,
            Timestamp.from(finishedAt),
            id,
        )
    }

    override fun fail(id: Long, error: String, finishedAt: Instant) {
        jdbc.update(
            "UPDATE batch_job_run SET status = '$FAILED', error = ?, finished_at = ? WHERE id = ?",
            error.take(ERROR_MAX_LENGTH),
            Timestamp.from(finishedAt),
            id,
        )
    }

    companion object {
        private const val RUNNING = "RUNNING"
        private const val SUCCESS = "SUCCESS"
        private const val FAILED = "FAILED"
        private const val ERROR_MAX_LENGTH = 1000
    }
}
