package com.alphatalk.worker.llm.publish

import com.alphatalk.contracts.envelope.StreamData

interface StreamPublisher {
    fun publish(code: String, eventId: String, data: StreamData)
}
