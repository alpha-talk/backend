package com.alphatalk.ws.relay

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.alphatalk.ws.subscription.DemandMutator
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class WatchlistUpdateHandler(
    private val demand: DemandMutator,
    private val objectMapper: ObjectMapper,
    meterRegistry: MeterRegistry,
) : RedisChannelHandler {
    private val log = LoggerFactory.getLogger(javaClass)
    private val dropped = meterRegistry.counter("ws.relay.envelope.dropped", "kind", "WatchlistUpdateHandler")

    override val kind = ChannelKind.WATCHLIST

    override fun handle(code: String?, payload: ByteArray) {
        val update = try {
            objectMapper.readValue(payload, WatchlistUpdated::class.java)
        } catch (e: Exception) {
            dropped.increment()
            log.warn("watchlist:updated parse failed, dropped")
            return
        }
        demand.applyWatchlistDiff(update.userId, update.added, update.removed)
    }
}
