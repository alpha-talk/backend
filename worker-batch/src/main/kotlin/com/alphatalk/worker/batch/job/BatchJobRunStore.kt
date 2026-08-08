package com.alphatalk.worker.batch.job

import java.time.Instant

interface BatchJobRunStore {
    fun start(job: String, runDate: String, startedAt: Instant): Long?
    fun restart(job: String, runDate: String, startedAt: Instant): Long
    fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant)
    fun fail(id: Long, error: String, finishedAt: Instant)

    fun failCounted(id: Long, okCount: Int, failCount: Int, error: String, finishedAt: Instant) {
        fail(id, error, finishedAt)
    }
}
