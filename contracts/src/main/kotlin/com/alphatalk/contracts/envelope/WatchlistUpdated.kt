package com.alphatalk.contracts.envelope

data class WatchlistUpdated(
    val userId: Long,
    val rev: Long,
    val codes: List<String>,
)
