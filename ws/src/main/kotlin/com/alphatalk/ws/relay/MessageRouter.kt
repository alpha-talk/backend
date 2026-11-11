package com.alphatalk.ws.relay

import com.alphatalk.contracts.Channels
import com.alphatalk.ws.watchlist.WatchlistRefresh
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.data.redis.connection.Message
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.connection.SubscriptionListener
import org.springframework.stereotype.Component

@Component
class MessageRouter(
    handlers: List<RedisChannelHandler>,
    meterRegistry: MeterRegistry,
    private val watchlistRefresh: WatchlistRefresh,
) : MessageListener, SubscriptionListener {
    private val log = LoggerFactory.getLogger(javaClass)
    private val byKind = handlers.associateBy { it.kind }
    private val unroutable: Counter = meterRegistry.counter("ws.relay.unroutable")
    private val handlerErrors: Counter = meterRegistry.counter("ws.relay.handler.errors")

    override fun onChannelSubscribed(channel: ByteArray, count: Long) {
        if (channel.toString(Charsets.UTF_8) == Channels.WATCHLIST_UPDATED) watchlistRefresh.reconcile()
    }

    override fun onMessage(message: Message, pattern: ByteArray?) {
        val channel = String(message.channel, Charsets.UTF_8)
        val parsed = Channels.parse(channel) ?: run {
            unroutable.increment()
            return
        }
        val handler = byKind[parsed.kind] ?: run {
            unroutable.increment()
            return
        }
        try {
            handler.handle(parsed.code, message.body)
        } catch (e: Exception) {
            handlerErrors.increment()
            log.warn("relay handler failed. channel={}", channel, e)
        }
    }
}
