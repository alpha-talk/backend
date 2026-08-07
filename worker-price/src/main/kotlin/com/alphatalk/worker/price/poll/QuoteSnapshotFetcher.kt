package com.alphatalk.worker.price.poll

import com.alphatalk.kis.rest.KisQuoteSnapshot

fun interface QuoteSnapshotFetcher {
    fun fetch(code: String, marketDiv: String): KisQuoteSnapshot
}
