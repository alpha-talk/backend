package com.alphatalk.ws.client

import com.alphatalk.contracts.ChannelKind

interface ClientMessageSink {
    fun sendToUser(userId: Long, kind: ChannelKind, payload: Any)

    fun sendToRoom(kind: ChannelKind, code: String, payload: Any)
}
