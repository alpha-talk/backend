package com.alphatalk.coreapi.stream

interface StreamStore {
    fun find(query: StreamQuery): List<StreamItem>
    fun hasOlderThan(code: String, eventId: String, types: List<StreamEventType>): Boolean
    fun hasNewerThan(code: String, eventId: String, types: List<StreamEventType>): Boolean
}
