package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisSector

interface SectorStore {
    fun upsertAll(sectors: List<KisSector>): Int
}
