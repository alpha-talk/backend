package com.alphatalk.worker.price.market

import java.util.concurrent.ConcurrentHashMap

class InMemoryMarketDivStore(seed: Map<String, String> = emptyMap()) : MarketDivStore {
    val confirmed = ConcurrentHashMap<String, String>()

    init {
        confirmed.putAll(seed)
    }

    override fun get(code: String): String? = confirmed[code]

    override fun confirm(code: String, div: String) {
        confirmed[code] = div
    }
}
