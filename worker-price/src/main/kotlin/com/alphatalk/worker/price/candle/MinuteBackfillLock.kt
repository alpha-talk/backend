package com.alphatalk.worker.price.candle

import java.time.Duration

interface MinuteBackfillLock {
    fun tryAcquire(code: String, ttl: Duration): Boolean
    fun release(code: String)
}
