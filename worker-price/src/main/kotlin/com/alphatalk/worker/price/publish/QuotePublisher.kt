package com.alphatalk.worker.price.publish

import com.alphatalk.contracts.envelope.QuoteData

interface QuotePublisher {
    fun publish(code: String, data: QuoteData, ts: Long)
}
