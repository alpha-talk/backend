package com.alphatalk.worker.price.publish

import com.alphatalk.contracts.envelope.DepthData

fun interface DepthPublisher {
    fun publish(code: String, data: DepthData, ts: Long): Boolean
}
