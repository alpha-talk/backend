package com.alphatalk.worker.ingest.queue

import com.alphatalk.contracts.queue.IngestQueueEntry

interface DigestJobQueue {
    fun enqueueIfNew(entry: IngestQueueEntry): DigestEnqueueResult
}

enum class DigestEnqueueResult {
    ENQUEUED,
    ALREADY_ENQUEUED,
}
