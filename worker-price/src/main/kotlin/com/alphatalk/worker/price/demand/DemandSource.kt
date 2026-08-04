package com.alphatalk.worker.price.demand

fun interface DemandSource {
    fun targetSymbols(): Set<String>
}
