package com.alphatalk.contracts.envelope

data class DemandUpdated(
    val kind: String,
    val code: String,
    val active: Boolean,
    val ts: Long,
) {
    companion object {
        const val KIND_QUOTE = "quote"
        const val KIND_ROOM = "room"
    }
}
