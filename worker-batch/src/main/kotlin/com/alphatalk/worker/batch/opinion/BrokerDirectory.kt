package com.alphatalk.worker.batch.opinion

data class Broker(
    val code: String,
    val name: String,
) {
    val queryCode: String
        get() = code.takeLast(3)
}

fun interface BrokerDirectory {
    fun brokers(): List<Broker>
}
