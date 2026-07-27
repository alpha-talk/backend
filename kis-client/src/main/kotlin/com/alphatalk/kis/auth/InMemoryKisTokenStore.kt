package com.alphatalk.kis.auth

import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class InMemoryKisTokenStore(
    private val clock: () -> Instant = Instant::now,
) : KisTokenStore {
    private data class Entry(val token: String, val expiresAt: Instant)

    private data class Lock(val lockToken: String, val expiresAt: Instant)

    private val tokens = ConcurrentHashMap<String, Entry>()
    private val locks = ConcurrentHashMap<String, Lock>()
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

    override fun tryLock(keyId: String, ttl: Duration): String? {
        val now = clock()
        val candidate = UUID.randomUUID().toString()
        var acquired: String? = null
        locks.compute(keyId) { _, current ->
            if (current == null || now >= current.expiresAt) {
                acquired = candidate
                Lock(candidate, now + ttl)
            } else {
                current
            }
        }
        return acquired
    }

    override fun unlock(keyId: String, lockToken: String) {
        locks.computeIfPresent(keyId) { _, current ->
            if (current.lockToken == lockToken) null else current
        }
    }

    override fun lastIssuedAt(keyId: String): Instant? = issued[keyId]

    override fun markIssued(keyId: String, at: Instant) {
        issued[keyId] = at
    }
}
