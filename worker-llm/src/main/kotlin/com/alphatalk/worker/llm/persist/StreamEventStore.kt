package com.alphatalk.worker.llm.persist

import com.alphatalk.contracts.envelope.StreamData
import java.time.Instant

interface StreamEventStore {
    fun insertEvent(eventId: String, code: String, type: String, occurredAt: Instant, source: String?, data: StreamData): Boolean
    fun refreshSources(eventId: String, sourcesJson: String)
    fun digestExists(code: String, date: String): Boolean
}
