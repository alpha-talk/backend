package com.alphatalk.worker.price.conflation

import com.alphatalk.contracts.envelope.DepthData
import com.alphatalk.kis.ws.KisDepth
import com.alphatalk.kis.ws.KisDepthLevel
import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
@ConditionalOnKisAccounts
class DepthConflationBuffer {
    private val latest = ConcurrentHashMap<String, KisDepth>()
    private val dirty = ConcurrentHashMap.newKeySet<String>()

    fun offer(depth: KisDepth) {
        latest[depth.code] = depth
        dirty += depth.code
    }

    fun drainDirty(): Map<String, DepthData> {
        val drained = mutableMapOf<String, DepthData>()
        for (code in dirty.toList()) {
            dirty.remove(code)
            val depth = latest[code] ?: continue
            drained[code] = toDepthData(depth)
        }
        return drained
    }

    private fun toDepthData(depth: KisDepth) = DepthData(
        bids = levels(depth.bids),
        asks = levels(depth.asks),
    )

    private fun levels(levels: List<KisDepthLevel>): List<List<Long>> =
        levels.filter { it.price > 0 }.map { listOf(it.price, it.qty) }
}
