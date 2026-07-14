package com.alphatalk.ws.relay

interface ChannelSubscriber {
    fun subscribe(channel: String)
    fun unsubscribe(channel: String)
}
