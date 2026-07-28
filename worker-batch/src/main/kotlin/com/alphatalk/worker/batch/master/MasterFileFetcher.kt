package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisMarket

fun interface MasterFileFetcher {
    fun fetch(market: KisMarket): ByteArray
}
