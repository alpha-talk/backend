package com.alphatalk.kis.rate

fun interface KisRateGate {
    fun acquire(keyId: String)

    companion object {
        val NOOP: KisRateGate = KisRateGate { }
    }
}
