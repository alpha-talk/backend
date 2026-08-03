package com.alphatalk.coreapi.community

object IdempotencyActions {
    const val POST_CREATE = "post:create"
    const val COMMENT_CREATE = "comment:create"
}

data class IdempotencyRecord(
    val action: String,
    val response: String?,
)

interface IdempotencyLedger {
    fun find(userId: Long, key: String): IdempotencyRecord?

    fun claim(userId: Long, key: String, action: String): Boolean

    fun complete(userId: Long, key: String, response: String)
}
