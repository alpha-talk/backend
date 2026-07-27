package com.alphatalk.kis.auth

import java.time.Duration
import java.time.Instant

interface KisTokenStore {
    fun get(keyId: String): String?
    fun put(keyId: String, token: String, ttl: Duration)
    fun evict(keyId: String)
    fun tryLock(keyId: String, ttl: Duration): String?
    fun unlock(keyId: String, lockToken: String)
    fun lastIssuedAt(keyId: String): Instant?
    fun markIssued(keyId: String, at: Instant)
}
