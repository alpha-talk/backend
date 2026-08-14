package com.alphatalk.kis.ws

data class KisDepth(
    val code: String,
    val time: String,
    val asks: List<KisDepthLevel>,
    val bids: List<KisDepthLevel>,
)

data class KisDepthLevel(val price: Long, val qty: Long)
