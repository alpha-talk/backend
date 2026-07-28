package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisStockMaster

interface StockMasterStore {
    fun upsertAll(stocks: List<KisStockMaster>): Int
    fun deactivateMissing(activeCodes: Collection<String>): Int
}
