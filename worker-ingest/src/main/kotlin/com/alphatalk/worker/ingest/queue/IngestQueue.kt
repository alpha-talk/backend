package com.alphatalk.worker.ingest.queue

import com.alphatalk.contracts.queue.IngestQueueEntry

interface IngestQueue {
    fun enqueue(entry: IngestQueueEntry)
}
