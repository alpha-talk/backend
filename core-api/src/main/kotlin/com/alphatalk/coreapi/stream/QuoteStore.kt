package com.alphatalk.coreapi.stream

interface QuoteStore {
    fun liveQuote(code: String): QuoteResponse?
    fun lastCandleQuote(code: String): QuoteResponse?
}
