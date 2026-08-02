package com.alphatalk.coreapi.stream

import java.time.Instant

data class NewStreamEvent(
    val eventId: String,
    val code: String,
    val type: StreamEventType,
    val occurredAt: Instant,
    val source: String,
    val payload: String,
)

interface StreamEventAppender {
    fun append(event: NewStreamEvent)

    fun markDeleted(eventId: String): Boolean
}
