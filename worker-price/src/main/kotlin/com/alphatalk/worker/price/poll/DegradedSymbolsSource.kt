package com.alphatalk.worker.price.poll

fun interface DegradedSymbolsSource {
    fun degradedSymbols(): Set<String>
}
