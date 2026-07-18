package com.alphatalk.ws.relay

import com.alphatalk.contracts.Channels
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class RelayBootstrap(private val channelSubscriber: ChannelSubscriber) {
    @EventListener(ApplicationReadyEvent::class)
    fun subscribeGlobalChannels() {
        channelSubscriber.subscribe(Channels.WATCHLIST_UPDATED)
    }
}
