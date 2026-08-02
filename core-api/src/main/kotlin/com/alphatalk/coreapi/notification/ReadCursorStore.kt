package com.alphatalk.coreapi.notification

interface ReadCursorStore {
    fun find(userId: Long, codes: Collection<String>): Map<String, String>

    fun advance(userId: Long, code: String, eventId: String): Boolean

    fun advanceAll(userId: Long, cursors: Map<String, String>)
}
