package com.alphatalk.worker.llm.persist

import com.alphatalk.contracts.envelope.StreamData
import java.time.Instant

data class StreamEventRow(
    val eventId: String,
    val code: String,
    val type: String,
    val occurredAt: Instant,
    val source: String?,
    val data: StreamData,
)

interface StreamEventStore {
    fun insertEvents(events: List<StreamEventRow>): Set<String>
    fun refreshSources(eventIds: Collection<String>, sourcesJson: String)
    fun digestExists(code: String, date: String): Boolean
}
