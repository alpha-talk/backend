package com.alphatalk.worker.price.leader

interface LeaderLock {
    fun tryAcquire(): Boolean
    fun release()
}
