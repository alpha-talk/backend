package com.alphatalk.worker.ingest.inbox

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.source.FeedValidators

data class FeedCheckpoint(val version: Long, val validators: FeedValidators)

interface IngestInbox {
    fun checkpoint(feedId: String, url: String): FeedCheckpoint?
    fun capture(
        feedId: String,
        url: String,
        previous: FeedCheckpoint?,
        validators: FeedValidators,
        entries: List<IngestQueueEntry>,
    ): Int
}
