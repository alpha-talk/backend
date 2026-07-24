package com.alphatalk.worker.ingest.dedup

interface SeenMarker {
    fun markIfNew(sourceId: String): Boolean
    fun clear(sourceId: String)
}
