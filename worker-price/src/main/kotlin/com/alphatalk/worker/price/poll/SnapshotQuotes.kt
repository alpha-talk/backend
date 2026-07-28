package com.alphatalk.worker.price.poll

import com.alphatalk.contracts.envelope.QuoteData
import com.alphatalk.kis.rest.KisQuoteSnapshot

internal fun KisQuoteSnapshot.toQuoteData(): QuoteData = QuoteData(
    price = price,
    prevClose = price - change,
    change = change,
    changeRate = changeRate,
    volume = volume,
    open = open,
    high = high,
    low = low,
)
