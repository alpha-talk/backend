package com.alphatalk.coreapi.community

import com.alphatalk.contracts.envelope.PostData

data class PostCommitted(
    val code: String,
    val eventId: String,
    val data: PostData,
)

interface RoomPostBroadcast {
    fun publish(code: String, eventId: String, data: PostData)
}
