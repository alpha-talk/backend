package com.alphatalk.worker.llm.cluster

interface ClusterLock {
    fun <T> withLock(code: String, action: () -> T): T
}
