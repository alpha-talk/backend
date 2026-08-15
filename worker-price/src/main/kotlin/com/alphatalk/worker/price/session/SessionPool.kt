package com.alphatalk.worker.price.session

import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.model.KisLimits
import com.alphatalk.kis.ws.KisDepth
import com.alphatalk.kis.ws.KisFrameParser
import com.alphatalk.kis.ws.KisSessionListener
import com.alphatalk.kis.ws.KisTick
import com.alphatalk.kis.ws.KisWebSocketSession
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.alphatalk.worker.price.conflation.DepthConflationBuffer
import com.alphatalk.worker.price.market.MarketDivStore
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class SessionPool(
    accounts: List<KisAccount>,
    private val wsUrl: String,
    private val approvalKeys: (KisAccount) -> String,
    private val buffer: ConflationBuffer,
    private val meters: MeterRegistry,
    private val tickTrIds: List<String>,
    private val marketDivs: MarketDivStore,
    private val unifiedTrId: String = KisFrameParser.TR_ID_TICK_TOTAL,
    private val krxTrId: String = KisFrameParser.TR_ID_TICK,
    private val depthBuffer: DepthConflationBuffer = DepthConflationBuffer(),
    private val depthUnifiedTrId: String = KisFrameParser.TR_ID_DEPTH_TOTAL,
    private val depthKrxTrId: String = KisFrameParser.TR_ID_DEPTH,
    private val depthEnabled: Boolean = false,
    private val silenceMillis: Long = 20_000,
    private val maxRegistrationsPerSession: Int = KisLimits.MAX_REGISTRATIONS_PER_SESSION,
    private val removalGraceMillis: Long = 30_000,
    private val backoff: BackoffPolicy = BackoffPolicy(),
    private val connectTimeoutSeconds: Long = 10,
    private val degradedThreshold: Int = 5,
    private val ackTimeoutMillis: Long = 5_000,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    init {
        require(tickTrIds.isNotEmpty()) { "tickTrIds는 최소 1개 필요하다" }
    }

    private val log = LoggerFactory.getLogger(javaClass)
    private val maxSymbolsPerSession = maxRegistrationsPerSession / tickTrIds.size
    private val sessions = accounts.map { PooledSession(it) }
    private val assignments = mutableMapOf<String, PooledSession>()
    private val pendingRemovals = mutableMapOf<String, Long>()
    private val depthRemovals = mutableMapOf<String, Long>()

    @Volatile
    private var depthDroppedCount = 0
    private val degraded = linkedSetOf<String>()
    private val tickDivs = ConcurrentHashMap<String, String>()
    private val lastTickAt = ConcurrentHashMap<String, Long>()
    private val subscribedAt = ConcurrentHashMap<String, Long>()
    private val silenceDegraded = ConcurrentHashMap.newKeySet<String>()
    private val seenTicks = ConcurrentHashMap<String, SeenTick>()

    private data class Registration(val trId: String, val symbol: String)

    private data class UnsubscribeAttempt(val at: Long, val attempts: Int, val retryNow: Boolean = false)

    private companion object {
        const val MAX_UNSUBSCRIBE_ATTEMPTS = 3
    }

    private data class SeenTick(val div: String, val at: Long)

    init {
        SessionState.entries.forEach { state ->
            Gauge.builder("kis.ws.sessions", this) { pool ->
                pool.sessions.count { it.state == state }.toDouble()
            }.tag("state", state.name.lowercase()).register(meters)
        }
        Gauge.builder("kis.subscribed.symbols", this) { pool ->
            pool.sessions.sumOf { session ->
                session.heldRegistrations().map(Registration::symbol).distinct().size
            }.toDouble()
        }.register(meters)
        Gauge.builder("degraded.symbols", this) { pool ->
            pool.degraded.size.toDouble()
        }.register(meters)
        Gauge.builder("depth.symbols", this) { pool ->
            pool.sessions.sumOf { session ->
                session.heldRegistrations().count { it.trId == depthUnifiedTrId || it.trId == depthKrxTrId }
            }.toDouble()
        }.register(meters)
        Gauge.builder("depth.symbols.dropped", this) { pool ->
            pool.depthDroppedCount.toDouble()
        }.register(meters)
    }

    @Synchronized
    fun maintain(target: Set<String>, rooms: List<String>, subscribeAllowed: Boolean) {
        sessions.forEach { it.absorbConnectionLoss() }
        reconcileAssignments(target, rooms)
        reconcileDepth(if (depthEnabled) rooms else emptyList())
        val now = clock()
        if (subscribeAllowed) {
            adoptUpdatedDivs()
            absorbSeenTicks()
            escalateSilent(now)
        }
        sessions.forEach { session ->
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

    private fun trIdsFor(symbol: String): List<String> {
        val chosen = tickDivs.computeIfAbsent(symbol) { marketDivs.get(it) ?: MarketDivStore.UNIFIED }
        val tick = if (chosen == MarketDivStore.KRX) krxTrId else unifiedTrId
        return tickTrIds.map { if (it == unifiedTrId) tick else it }
    }

    private fun depthTrFor(symbol: String): String {
        val chosen = tickDivs.computeIfAbsent(symbol) { marketDivs.get(it) ?: MarketDivStore.UNIFIED }
        return if (chosen == MarketDivStore.KRX) depthKrxTrId else depthUnifiedTrId
    }

    private fun reconcileDepth(depthTarget: List<String>) {
        val now = clock()
        val live = LinkedHashSet(depthTarget)
        live.forEach(depthRemovals::remove)
        val previous = sessions.flatMapTo(mutableSetOf()) { it.depthAssigned }
        previous.filter { it !in live }.forEach { depthRemovals.putIfAbsent(it, now + removalGraceMillis) }
        depthRemovals.entries.removeIf { it.value <= now }
        val holdovers = previous.filter { it in depthRemovals }
        sessions.forEach { it.depthAssigned.clear() }
        var dropped = 0
        live.filter { it in previous }.forEach { if (!tryAssignDepth(it)) dropped += 1 }
        holdovers.forEach(::tryAssignDepth)
        live.filter { it !in previous }.forEach { if (!tryAssignDepth(it)) dropped += 1 }
        depthDroppedCount = dropped
    }

    private fun evictQuoteOnly(roomSet: Set<String>): PooledSession? {
        val victim = assignments.keys.firstOrNull { it in pendingRemovals && it !in roomSet }
            ?: assignments.keys.firstOrNull { it !in roomSet }
            ?: return null
        val session = assignments.remove(victim) ?: return null
        session.assigned.remove(victim)
        pendingRemovals.remove(victim)
        forgetSymbol(victim)
        degraded += victim
        log.info("방 수요가 quote 전용 등록을 선점한다 - REST 폴링으로 넘긴다: evicted={}", victim)
        meters.counter("tick.room.preempted").increment()
        return session
    }

    private fun tryAssignDepth(symbol: String): Boolean {
        val session = assignments[symbol] ?: return false
        if (symbol in session.depthAssigned) return true
        val used = session.assigned.size * tickTrIds.size + session.depthAssigned.size
        if (used >= maxRegistrationsPerSession) return false
        session.depthAssigned += symbol
        return true
    }

    private fun adoptUpdatedDivs() {
        assignments.keys.forEach { symbol ->
            if (lastTickAt.containsKey(symbol)) return@forEach
            val known = marketDivs.get(symbol) ?: return@forEach
            val current = tickDivs[symbol] ?: MarketDivStore.UNIFIED
            if (known == current) return@forEach
            log.info("시장 구분 기록이 갱신됐다 - 재구독한다: code={} {} -> {}", symbol, current, known)
            meters.counter("tick.div.resubscribed").increment()
            tickDivs[symbol] = known
            subscribedAt.remove(symbol)
        }
    }

    private fun escalateSilent(now: Long) {
        assignments.keys.forEach { symbol ->
            if (symbol in silenceDegraded) {
                degraded += symbol
                return@forEach
            }
            val since = subscribedAt[symbol] ?: return@forEach
            if (lastTickAt.containsKey(symbol) || now - since < silenceMillis) return@forEach
            silenceDegraded += symbol
            degraded += symbol
            log.warn(
                "실시간 틱 침묵 - REST 폴링으로 넘긴다: code={} div={} silenceMs={}",
                symbol,
                tickDivs[symbol] ?: MarketDivStore.UNIFIED,
                now - since,
            )
            meters.counter("tick.silence.degraded").increment()
        }
    }

    private fun onSymbolTick(trId: String, symbol: String, now: Long) {
        val div = when (trId) {
            krxTrId -> MarketDivStore.KRX
            unifiedTrId -> MarketDivStore.UNIFIED
            else -> return
        }
        seenTicks[symbol] = SeenTick(div, now)
    }

    private fun absorbSeenTicks() {
        seenTicks.keys.toList().forEach { symbol ->
            val seen = seenTicks.remove(symbol) ?: return@forEach
            if (symbol !in assignments) return@forEach
            if (seen.div != (tickDivs[symbol] ?: MarketDivStore.UNIFIED)) return@forEach
            silenceDegraded.remove(symbol)
            if (lastTickAt.put(symbol, seen.at) != null) return@forEach
            if (seen.div != MarketDivStore.UNIFIED) return@forEach
            marketDivs.confirm(symbol, seen.div)
            meters.counter("tick.market.div", "div", seen.div).increment()
        }
    }

    private fun rearmSilence(symbol: String) {
        seenTicks.remove(symbol)
        subscribedAt.remove(symbol)
        lastTickAt.remove(symbol)
    }

    private fun forgetSymbol(symbol: String) {
        seenTicks.remove(symbol)
        tickDivs.remove(symbol)
        lastTickAt.remove(symbol)
        subscribedAt.remove(symbol)
        silenceDegraded.remove(symbol)
    }

    @Synchronized
    private fun applyAck(pooled: PooledSession, trId: String?, trKey: String?, success: Boolean) {
        pooled.onAck(trId, trKey, success)
    }

    @Synchronized
    private fun applyUnsubscribeAck(pooled: PooledSession, trId: String?, trKey: String?, success: Boolean) {
        pooled.onUnsubscribeAck(trId, trKey, success)
    }

    private fun reconcileAssignments(target: Set<String>, rooms: List<String>) {
        val now = clock()
        degraded.clear()
        val roomSet = rooms.toSet()
        val ordered = LinkedHashSet<String>(rooms.size + target.size)
        rooms.filterTo(ordered) { it in target }
        ordered.addAll(target)
        ordered.forEach { symbol ->
            pendingRemovals.remove(symbol)
            if (symbol !in assignments) {
                val candidate = sessions.minByOrNull { it.assigned.size }
                    ?.takeIf { it.assigned.size < maxSymbolsPerSession }
                    ?: if (symbol in roomSet) evictQuoteOnly(roomSet) else null
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
            forgetSymbol(symbol)
        }
    }

    private inner class PooledSession(val account: KisAccount) {
        var state: SessionState = SessionState.DISCONNECTED
        val assigned = mutableSetOf<String>()
        val depthAssigned = mutableSetOf<String>()
        val confirmed: MutableSet<Registration> = ConcurrentHashMap.newKeySet()
        val pending = mutableMapOf<Registration, Long>()
        val pendingUnsubscribes = ConcurrentHashMap<Registration, UnsubscribeAttempt>()

        fun heldRegistrations(): Set<Registration> = confirmed + pendingUnsubscribes.keys
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
            assigned.forEach { silenceDegraded += it }
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
            val wanted = assigned.flatMapTo(mutableSetOf()) { symbol ->
                trIdsFor(symbol).map { Registration(it, symbol) }
            }
            depthAssigned.mapTo(wanted) { Registration(depthTrFor(it), it) }
            pendingUnsubscribes.keys.removeAll(wanted)
            val retried = pendingUnsubscribes
                .filterValues { it.retryNow || now - it.at >= ackTimeoutMillis }
                .keys
            (confirmed + pending.keys - wanted + retried).forEach { registration ->
                sendUnsubscribe(current, registration, now)
            }
            val awaitingAck = pending.filterValues { now - it < ackTimeoutMillis }.keys
            (wanted - confirmed - awaitingAck).forEach { registration ->
                runCatching { current.subscribe(registration.symbol, registration.trId) }
                    .onSuccess {
                        pending[registration] = now
                        armSilence(registration, now)
                    }
                    .onFailure {
                        log.warn(
                            "subscribe failed: keyId={} trId={} code={}",
                            account.keyId, registration.trId, registration.symbol, it,
                        )
                    }
            }
        }

        private fun armSilence(registration: Registration, now: Long) {
            if (registration.trId == unifiedTrId || registration.trId == krxTrId) {
                subscribedAt.putIfAbsent(registration.symbol, now)
            }
        }

        private fun sendUnsubscribe(current: KisWebSocketSession, registration: Registration, now: Long) {
            val attempts = (pendingUnsubscribes[registration]?.attempts ?: 0) + 1
            if (attempts > MAX_UNSUBSCRIBE_ATTEMPTS) {
                if (connectionLost.compareAndSet(false, true)) {
                    log.error(
                        "unsubscribe 재시도 한도 초과 - 세션을 재접속해 등록을 회수한다: keyId={} trId={} code={}",
                        account.keyId, registration.trId, registration.symbol,
                    )
                    meters.counter("kis.unsubscribe.abandoned").increment()
                }
                return
            }
            runCatching { current.unsubscribe(registration.symbol, registration.trId) }
                .onSuccess {
                    confirmed -= registration
                    pending.remove(registration)
                    pendingUnsubscribes[registration] = UnsubscribeAttempt(now, attempts)
                }
                .onFailure {
                    log.warn(
                        "unsubscribe failed - retried next maintain: keyId={} trId={} code={}",
                        account.keyId, registration.trId, registration.symbol, it,
                    )
                }
        }

        fun onUnsubscribeAck(trId: String?, trKey: String?, success: Boolean) {
            if (trId == null || trKey == null) return
            val registration = Registration(trId, trKey)
            if (success) {
                pendingUnsubscribes.remove(registration)
                return
            }
            val attempt = pendingUnsubscribes[registration] ?: return
            pendingUnsubscribes[registration] = attempt.copy(retryNow = true)
            log.warn("unsubscribe rejected, retrying: keyId={} trId={} code={}", account.keyId, trId, trKey)
        }

        fun onAck(trId: String?, trKey: String?, success: Boolean) {
            if (trId == null || trKey == null) return
            val registration = Registration(trId, trKey)
            if (!success && registration !in pending && pendingUnsubscribes.containsKey(registration)) {
                onUnsubscribeAck(trId, trKey, success)
                return
            }
            if (pending.remove(registration) == null) return
            if (success) {
                confirmed += registration
                if (registration.trId == unifiedTrId || registration.trId == krxTrId) {
                    subscribedAt.putIfAbsent(trKey, clock())
                }
            } else {
                log.warn("subscribe rejected, retrying: keyId={} trId={} code={}", account.keyId, trId, trKey)
            }
        }

        fun clearSubscriptions() {
            confirmed.clear()
            pending.clear()
            pendingUnsubscribes.clear()
            assigned.forEach(::rearmSilence)
        }

        fun disconnect() {
            session?.let { current ->
                runCatching { (confirmed + pending.keys).forEach { current.unsubscribe(it.symbol, it.trId) } }
                runCatching { current.close() }
            }
            session = null
            clearSubscriptions()
            depthAssigned.clear()
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
        override fun onTicks(trId: String, ticks: List<KisTick>) {
            val now = clock()
            ticks.forEach { tick ->
                buffer.offer(tick)
                onSymbolTick(trId, tick.code, now)
            }
            meters.counter("tick.in").increment(ticks.size.toDouble())
        }

        override fun onDepths(trId: String, depths: List<KisDepth>) {
            depths.forEach(depthBuffer::offer)
            meters.counter("depth.in").increment(depths.size.toDouble())
        }

        override fun onSubscribeAck(trId: String?, trKey: String?, success: Boolean) {
            applyAck(pooled, trId, trKey, success)
        }

        override fun onUnsubscribeAck(trId: String?, trKey: String?, success: Boolean) {
            applyUnsubscribeAck(pooled, trId, trKey, success)
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
