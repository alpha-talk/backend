package com.alphatalk.coreapi.auth

import java.time.Instant

data class UserRecord(
    val id: Long,
    val email: String,
    val passwordHash: String,
    val nickname: String,
    val createdAt: Instant,
)

interface UserStore {
    fun create(email: String, passwordHash: String, nickname: String): Long
    fun findByEmail(email: String): UserRecord?
    fun findById(id: Long): UserRecord?
    fun existsByEmail(email: String): Boolean
    fun existsByNickname(nickname: String): Boolean
    fun nicknames(ids: Collection<Long>): Map<Long, String>
}

class DuplicateUserException(cause: Throwable) : RuntimeException(cause)
