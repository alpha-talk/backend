package com.alphatalk.worker.price.demand

class FixedDemandSource(symbols: List<String>) : DemandSource {
    private val fixed = symbols.toSet()

    override fun targetSymbols(): Set<String> = fixed
}
