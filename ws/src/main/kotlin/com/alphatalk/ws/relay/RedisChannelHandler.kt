package com.alphatalk.ws.relay

import com.alphatalk.contracts.ChannelKind

interface RedisChannelHandler {
    val kind: ChannelKind

    fun handle(code: String?, payload: ByteArray)
}
