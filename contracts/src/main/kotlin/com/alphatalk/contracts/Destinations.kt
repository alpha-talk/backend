package com.alphatalk.contracts

object Destinations {
    const val USER_QUEUE_QUOTE = "/user/queue/quote"
    const val USER_QUEUE_STREAM = "/user/queue/stream"

    const val QUEUE_QUOTE = "/queue/quote"
    const val QUEUE_STREAM = "/queue/stream"

    fun roomQuote(code: String) = "/topic/rooms/$code/quote"
    fun roomPosts(code: String) = "/topic/rooms/$code/posts"
    fun roomTrade(code: String) = "/topic/rooms/$code/trade"
    fun roomDepth(code: String) = "/topic/rooms/$code/depth"

    private val ROOM_TOPIC = Regex("""^/topic/rooms/(\d{6})/(quote|posts|trade|depth)$""")

    data class RoomTopic(val code: String, val kind: ChannelKind)

    fun parseRoomTopic(destination: String): RoomTopic? {
        val match = ROOM_TOPIC.matchEntire(destination) ?: return null
        val kind = when (match.groupValues[2]) {
            "quote" -> ChannelKind.QUOTE
            "posts" -> ChannelKind.POST
            "trade" -> ChannelKind.TRADE
            else -> ChannelKind.DEPTH
        }
        return RoomTopic(match.groupValues[1], kind)
    }
}
