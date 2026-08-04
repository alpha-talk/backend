package com.alphatalk.ws.subscription

enum class DemandSignalKind {
    QUOTE,
    ROOM,
}

interface DemandSignalPublisher {
    fun increment(kind: DemandSignalKind, code: String)

    fun decrement(kind: DemandSignalKind, code: String)
}
