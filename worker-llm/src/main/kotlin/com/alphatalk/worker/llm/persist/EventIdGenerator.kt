package com.alphatalk.worker.llm.persist

import com.github.f4b6a3.ulid.UlidCreator
import org.springframework.stereotype.Component

fun interface EventIdGenerator {
    fun next(): String
}

@Component
class UlidEventIdGenerator : EventIdGenerator {
    override fun next(): String = UlidCreator.getMonotonicUlid().toString()
}
