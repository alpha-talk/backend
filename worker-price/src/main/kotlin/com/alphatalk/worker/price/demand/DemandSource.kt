package com.alphatalk.worker.price.demand

fun interface DemandSource {
    fun targetSymbols(): Set<String>

    fun roomDemand(): Map<String, Long> = emptyMap()
}
