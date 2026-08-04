package com.alphatalk.worker.price.candle

import java.time.Duration

interface MinuteRefreshLock {
    fun tryAcquire(code: String, ttl: Duration): Boolean
    fun release(code: String)
}
