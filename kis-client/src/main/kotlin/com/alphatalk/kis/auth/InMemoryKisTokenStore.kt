package com.alphatalk.kis.auth

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

class InMemoryKisTokenStore(
    private val clock: () -> Instant = Instant::now,
) : KisTokenStore {
    private data class Entry(val token: String, val expiresAt: Instant)

    private val tokens = ConcurrentHashMap<String, Entry>()
    private val locks = ConcurrentHashMap<String, Instant>()
    private val issued = ConcurrentHashMap<String, Instant>()

    override fun get(keyId: String): String? {
        val entry = tokens[keyId] ?: return null
        if (clock() >= entry.expiresAt) {
            tokens.remove(keyId)
            return null
        }
        return entry.token
    }

    override fun put(keyId: String, token: String, ttl: Duration) {
        tokens[keyId] = Entry(token, clock() + ttl)
    }

    override fun evict(keyId: String) {
        tokens.remove(keyId)
    }

    override fun tryLock(keyId: String, ttl: Duration): Boolean {
        val now = clock()
        var acquired = false
        locks.compute(keyId) { _, lockedUntil ->
            if (lockedUntil == null || now >= lockedUntil) {
                acquired = true
                now + ttl
            } else {
                lockedUntil
            }
        }
        return acquired
    }

    override fun unlock(keyId: String) {
        locks.remove(keyId)
    }

    override fun lastIssuedAt(keyId: String): Instant? = issued[keyId]

    override fun markIssued(keyId: String, at: Instant) {
        issued[keyId] = at
    }
}
