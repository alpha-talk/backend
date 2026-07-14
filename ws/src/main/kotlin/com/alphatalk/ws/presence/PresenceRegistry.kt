package com.alphatalk.ws.presence

interface PresenceRegistry {
    fun add(userId: Long, sessionId: String)
    fun remove(userId: Long, sessionId: String)

    fun refresh(userIds: Collection<Long>)
}
