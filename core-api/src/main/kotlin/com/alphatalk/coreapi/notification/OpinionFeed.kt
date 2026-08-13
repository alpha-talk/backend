package com.alphatalk.coreapi.notification

import java.time.Instant

data class OpinionRecord(
    val eventId: String,
    val code: String,
    val businessDate: String,
    val brokerCode: String,
    val brokerName: String?,
    val rating: String,
    val previousRating: String?,
    val targetPrice: Long?,
    val collectedAt: Instant,
)

interface OpinionFeed {
    fun countNewerThan(afterEventId: String?, fetchLimit: Int): Int

    fun findLatest(beforeEventId: String?, limit: Int): List<OpinionRecord>

    fun hasOlderThan(eventId: String): Boolean

    fun hasNewerThan(eventId: String): Boolean

    fun latestEventId(): String?
}
