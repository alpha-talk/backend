package com.alphatalk.worker.llm.persist

import com.github.f4b6a3.ulid.UlidCreator

fun interface EventIdGenerator {
    fun next(): String
}

class UlidEventIdGenerator : EventIdGenerator {
    override fun next(): String = UlidCreator.getMonotonicUlid().toString()
}
