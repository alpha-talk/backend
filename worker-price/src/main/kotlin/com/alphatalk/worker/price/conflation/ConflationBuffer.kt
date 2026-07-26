package com.alphatalk.worker.price.conflation

import com.alphatalk.contracts.envelope.QuoteData
import com.alphatalk.kis.ws.KisTick
import java.util.concurrent.ConcurrentHashMap

class ConflationBuffer {
    private val latest = ConcurrentHashMap<String, KisTick>()
    private val dirty = ConcurrentHashMap.newKeySet<String>()

    fun offer(tick: KisTick) {
        latest[tick.code] = tick
        dirty += tick.code
    }

    fun drainDirty(): Map<String, QuoteData> {
        val drained = mutableMapOf<String, QuoteData>()
        for (code in dirty.toList()) {
            dirty.remove(code)
            val tick = latest[code] ?: continue
            drained[code] = toQuoteData(tick)
        }
        return drained
    }

    private fun toQuoteData(tick: KisTick) = QuoteData(
        price = tick.price,
        prevClose = tick.price - tick.change,
        change = tick.change,
        changeRate = tick.changeRate,
        volume = tick.volume,
        open = tick.open,
        high = tick.high,
        low = tick.low,
    )
}
