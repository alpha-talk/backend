package com.alphatalk.contracts.envelope

data class WatchlistUpdated(
    val userId: Long,
    val added: List<String> = emptyList(),
    val removed: List<String> = emptyList(),
    val ts: Long,
)
