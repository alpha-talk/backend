package com.alphatalk.worker.batch.opinion

import com.alphatalk.contracts.envelope.StreamData

fun interface OpinionPublisher {
    fun publish(code: String, eventId: String, data: StreamData): Boolean
}
