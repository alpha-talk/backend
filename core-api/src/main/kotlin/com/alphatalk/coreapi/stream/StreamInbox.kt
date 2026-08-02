package com.alphatalk.coreapi.stream

data class UnreadWindow(
    val code: String,
    val afterEventId: String?,
)

interface StreamInbox {
    fun countUnread(windows: List<UnreadWindow>, perCodeFetchLimit: Int): Map<String, Int>

    fun findUnread(
        windows: List<UnreadWindow>,
        types: List<StreamEventType>,
        beforeEventId: String?,
        limit: Int,
    ): List<StreamItem>

    fun hasUnreadOlderThan(windows: List<UnreadWindow>, types: List<StreamEventType>, eventId: String): Boolean

    fun hasUnreadNewerThan(windows: List<UnreadWindow>, types: List<StreamEventType>, eventId: String): Boolean

    fun latestEventIds(codes: Collection<String>): Map<String, String>
}
