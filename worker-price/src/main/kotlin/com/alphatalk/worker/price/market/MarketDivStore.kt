package com.alphatalk.worker.price.market

interface MarketDivStore {
    fun get(code: String): String?
    fun confirm(code: String, div: String)

    companion object {
        const val UNIFIED = "UN"
        const val KRX = "J"
    }
}
