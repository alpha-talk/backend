package com.alphatalk.coreapi.community

import com.github.f4b6a3.ulid.UlidCreator
import org.springframework.stereotype.Component

fun interface CommunityIdGenerator {
    fun next(): String
}

@Component
class UlidCommunityIdGenerator : CommunityIdGenerator {
    override fun next(): String = UlidCreator.getMonotonicUlid().toString()
}
