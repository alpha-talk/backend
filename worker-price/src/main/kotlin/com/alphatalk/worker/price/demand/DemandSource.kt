package com.alphatalk.worker.price.demand

interface DemandSource {
    fun targetSymbols(): Set<String>
}
