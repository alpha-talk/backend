package com.alphatalk.coreapi.notification

interface OpinionReadCursorStore {
    fun find(userId: Long): String?

    fun advance(userId: Long, eventId: String): String
}
