package com.alphatalk.worker.price.candle

import java.time.Duration

interface MinuteRefreshLock {
    data class Claim(val jobId: String, val acquired: Boolean)

    sealed interface Result {
        data class Succeeded(val synced: Int) : Result
        data object Failed : Result
    }

    fun claim(code: String, ttl: Duration): Claim
    fun complete(jobId: String, result: Result)
    fun awaitResult(jobId: String, timeout: Duration): Result?
    fun release(code: String, jobId: String)
}
