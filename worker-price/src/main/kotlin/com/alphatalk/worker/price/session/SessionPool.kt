package com.alphatalk.worker.price.session

import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.model.KisLimits
import com.alphatalk.kis.ws.KisSessionListener
import com.alphatalk.kis.ws.KisTick
import com.alphatalk.kis.ws.KisWebSocketSession
import com.alphatalk.worker.price.conflation.ConflationBuffer
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class SessionPool(
    accounts: List<KisAccount>,
    private val wsUrl: String,
    private val approvalKeys: (KisAccount) -> String,
    private val buffer: ConflationBuffer,
    private val meters: MeterRegistry,
    private val maxSymbolsPerSession: Int = KisLimits.MAX_SYMBOLS_PER_SESSION,
    private val removalGraceMillis: Long = 30_000,
    private val backoff: BackoffPolicy = BackoffPolicy(),
    private val connectTimeoutSeconds: Long = 10,
    private val degradedThreshold: Int = 5,
    private val ackTimeoutMillis: Long = 5_000,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val sessions = accounts.map { PooledSession(it) }
    private val assignments = mutableMapOf<String, PooledSession>()
    private val pendingRemovals = mutableMapOf<String, Long>()
    private val degraded = linkedSetOf<String>()

    init {
        SessionState.entries.forEach { state ->
            Gauge.builder("kis.ws.sessions", this) { pool ->
                pool.sessions.count { it.state == state }.toDouble()
            }.tag("state", state.name.lowercase()).register(meters)
        }
        Gauge.builder("kis.subscribed.symbols", this) { pool ->
            pool.sessions.sumOf { it.confirmed.size }.toDouble()
        }.register(meters)
        Gauge.builder("degraded.symbols", this) { pool ->
            pool.degraded.size.toDouble()
        }.register(meters)
    }

    @Synchronized
    fun maintain(target: Set<String>, subscribeAllowed: Boolean) {
        reconcileAssignments(target)
        val now = clock()
        sessions.forEach { session ->
            session.absorbConnectionLoss()
            if (session.state != SessionState.CONNECTED && session.state != SessionState.CONNECTING &&
                session.assigned.isNotEmpty() && now >= session.nextConnectAttemptAt
            ) {
                session.connect()
            }
            if (session.isConnected && subscribeAllowed) {
                session.syncSubscriptions()
            }
        }
    }

    @Synchronized
    fun disconnectAll() {
        sessions.forEach { it.disconnect() }
    }

    @Synchronized
    fun degradedSymbols(): Set<String> = degraded.toSet()

    @Synchronized
    private fun applyAck(pooled: PooledSession, trKey: String?, success: Boolean) {
        pooled.onAck(trKey, success)
    }

    private fun reconcileAssignments(target: Set<String>) {
        val now = clock()
        degraded.clear()
        target.forEach { symbol ->
            pendingRemovals.remove(symbol)
            if (symbol !in assignments) {
                val candidate = sessions.minByOrNull { it.assigned.size }
                    ?.takeIf { it.assigned.size < maxSymbolsPerSession }
                if (candidate == null) {
                    degraded += symbol
                } else {
                    candidate.assigned += symbol
                    assignments[symbol] = candidate
                }
            }
        }
        assignments.keys.filter { it !in target }.forEach { symbol ->
            pendingRemovals.putIfAbsent(symbol, now + removalGraceMillis)
        }
        pendingRemovals.entries.filter { it.value <= now }.map { it.key }.forEach { symbol ->
            pendingRemovals.remove(symbol)
            assignments.remove(symbol)?.assigned?.remove(symbol)
        }
    }

    private inner class PooledSession(val account: KisAccount) {
        var state: SessionState = SessionState.DISCONNECTED
        val assigned = mutableSetOf<String>()
        val confirmed = mutableSetOf<String>()
        val pending = mutableMapOf<String, Long>()
        var session: KisWebSocketSession? = null
        var nextConnectAttemptAt = 0L
        var consecutiveFailures = 0
        val connectionLost = AtomicBoolean(false)

        val isConnected: Boolean
            get() = state == SessionState.CONNECTED && session?.isOpen == true

        fun absorbConnectionLoss() {
            if (state == SessionState.CONNECTED && session?.isOpen != true) {
                connectionLost.set(true)
            }
            if (!connectionLost.compareAndSet(true, false)) return
            if (session == null) return
            runCatching { session?.close() }
            session = null
            clearSubscriptions()
            registerFailure()
            log.warn("kis ws connection lost: keyId={} failures={}", account.keyId, consecutiveFailures)
        }

        fun connect() {
            state = SessionState.CONNECTING
            try {
                val created = KisWebSocketSession(wsUrl, approvalKeys(account), FrameHandler(this))
                created.connect().get(connectTimeoutSeconds, TimeUnit.SECONDS)
                session = created
                clearSubscriptions()
                state = SessionState.CONNECTED
                consecutiveFailures = 0
                log.info("kis ws connected: keyId={} assigned={}", account.keyId, assigned.size)
            } catch (e: Exception) {
                runCatching { session?.close() }
                session = null
                registerFailure()
                log.warn("kis ws connect failed: keyId={} failures={}", account.keyId, consecutiveFailures, e)
            }
        }

        fun syncSubscriptions() {
            val current = session ?: return
            val now = clock()
            pending.entries.removeIf { now - it.value >= ackTimeoutMillis }
            (assigned - confirmed - pending.keys).forEach { symbol ->
                runCatching { current.subscribe(symbol) }
                    .onSuccess { pending[symbol] = now }
                    .onFailure { log.warn("subscribe failed: keyId={} code={}", account.keyId, symbol, it) }
            }
            (confirmed + pending.keys - assigned).forEach { symbol ->
                runCatching { current.unsubscribe(symbol) }
                confirmed -= symbol
                pending.remove(symbol)
            }
        }

        fun onAck(trKey: String?, success: Boolean) {
            val symbol = trKey ?: return
            if (pending.remove(symbol) == null) return
            if (success) {
                confirmed += symbol
            } else {
                log.warn("subscribe rejected, retrying: keyId={} code={}", account.keyId, symbol)
            }
        }

        fun clearSubscriptions() {
            confirmed.clear()
            pending.clear()
        }

        fun disconnect() {
            session?.let { current ->
                runCatching { (confirmed + pending.keys).forEach { current.unsubscribe(it) } }
                runCatching { current.close() }
            }
            session = null
            clearSubscriptions()
            state = SessionState.DISCONNECTED
            consecutiveFailures = 0
            nextConnectAttemptAt = 0
            connectionLost.set(false)
        }

        private fun registerFailure() {
            consecutiveFailures += 1
            state = if (consecutiveFailures >= degradedThreshold) SessionState.DEGRADED else SessionState.DISCONNECTED
            nextConnectAttemptAt = clock() + backoff.delayFor(consecutiveFailures)
        }
    }

    private inner class FrameHandler(private val pooled: PooledSession) : KisSessionListener {
        override fun onTicks(ticks: List<KisTick>) {
            ticks.forEach(buffer::offer)
            meters.counter("tick.in").increment(ticks.size.toDouble())
        }

        override fun onSubscribeAck(trId: String?, trKey: String?, success: Boolean) {
            applyAck(pooled, trKey, success)
        }

        override fun onEncryptedDropped(trId: String) {
            log.warn("encrypted frame dropped: trId={}", trId)
        }

        override fun onClosed(reason: String?) {
            log.info("kis ws closed: keyId={} {}", pooled.account.keyId, reason)
            pooled.connectionLost.set(true)
        }

        override fun onError(t: Throwable) {
            log.warn("kis ws frame error: keyId={}", pooled.account.keyId, t)
        }

        override fun onTransportError(t: Throwable) {
            log.warn("kis ws transport error: keyId={}", pooled.account.keyId, t)
            pooled.connectionLost.set(true)
        }
    }
}
