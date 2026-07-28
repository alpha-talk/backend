package com.alphatalk.auth

import java.time.Duration

interface TokenVerifier {
    fun verify(token: String): Long
}

interface TokenIssuer {
    fun issue(userId: Long, ttl: Duration): String
}

open class InvalidTokenException(message: String, cause: Throwable? = null) :
    RuntimeException(message, cause)

class ExpiredTokenException(message: String, cause: Throwable? = null) :
    InvalidTokenException(message, cause)
