package com.alphatalk.worker.ingest.queue

import com.alphatalk.contracts.queue.IngestQueueEntry

interface IngestQueue {
    fun enqueueIfNew(entry: IngestQueueEntry): EnqueueResult
}

enum class EnqueueResult {
    ENQUEUED,
    ALREADY_ENQUEUED,
}
