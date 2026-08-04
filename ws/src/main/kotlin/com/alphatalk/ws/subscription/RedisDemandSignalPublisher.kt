package com.alphatalk.ws.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.envelope.DemandUpdated
import com.alphatalk.ws.config.WsProperties
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Repository
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Repository
class RedisDemandSignalPublisher(
    private val redis: StringRedisTemplate,
    private val objectMapper: ObjectMapper,
    props: WsProperties,
) : DemandSignalPublisher, SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)

    val gwId: String = UUID.randomUUID().toString().replace("-", "").take(12)

    private val aliveTtl = Duration.ofSeconds(props.demand.aliveTtlSeconds)
    private val hashTtl = Duration.ofSeconds(props.demand.hashTtlSeconds)
    private val hashTtlMillis = hashTtl.toMillis().toString()

    init {
        log.info("demand signal publisher initialized. gwId={}", gwId)
    }

    override fun increment(kind: DemandSignalKind, code: String) = bestEffort("increment") {
        val count = redis.execute(INCR_SCRIPT, listOf(hashKey(kind)), code, hashTtlMillis)
        if (count == 1L) publishTransition(kind, code, active = true)
    }

    override fun decrement(kind: DemandSignalKind, code: String) = bestEffort("decrement") {
        val count = redis.execute(DECR_SCRIPT, listOf(hashKey(kind)), code, hashTtlMillis)
        if (count == 0L) publishTransition(kind, code, active = false)
    }

    @Scheduled(
        fixedDelayString = "\${ws.demand.heartbeat-interval-seconds:5}",
        timeUnit = TimeUnit.SECONDS,
    )
    fun heartbeat() = bestEffort("heartbeat") {
        redis.opsForValue().set(Keys.gwAlive(gwId), "1", aliveTtl)
        redis.expire(Keys.demandQuote(gwId), hashTtl)
        redis.expire(Keys.demandRoom(gwId), hashTtl)
    }

    fun withdraw() = bestEffort("withdraw") {
        redis.delete(listOf(Keys.gwAlive(gwId), Keys.demandQuote(gwId), Keys.demandRoom(gwId)))
    }

    override fun start() {
        if (running.compareAndSet(false, true)) heartbeat()
    }

    override fun stop() {
        if (running.compareAndSet(true, false)) withdraw()
    }

    override fun isRunning(): Boolean = running.get()

    private fun publishTransition(kind: DemandSignalKind, code: String, active: Boolean) {
        val payload = DemandUpdated(
            kind = when (kind) {
                DemandSignalKind.QUOTE -> DemandUpdated.KIND_QUOTE
                DemandSignalKind.ROOM -> DemandUpdated.KIND_ROOM
            },
            code = code,
            active = active,
            ts = System.currentTimeMillis(),
        )
        redis.convertAndSend(Channels.DEMAND_UPDATED, objectMapper.writeValueAsString(payload))
    }

    private fun hashKey(kind: DemandSignalKind): String = when (kind) {
        DemandSignalKind.QUOTE -> Keys.demandQuote(gwId)
        DemandSignalKind.ROOM -> Keys.demandRoom(gwId)
    }

    private inline fun bestEffort(op: String, block: () -> Any?) {
        try {
            block()
        } catch (e: Exception) {
            log.warn("demand signal {} failed (best-effort)", op, e)
        }
    }

    companion object {
        private val INCR_SCRIPT = DefaultRedisScript(
            "local c = redis.call('hincrby', KEYS[1], ARGV[1], 1) " +
                "redis.call('pexpire', KEYS[1], ARGV[2]) " +
                "return c",
            Long::class.java,
        )
        private val DECR_SCRIPT = DefaultRedisScript(
            "local c = redis.call('hincrby', KEYS[1], ARGV[1], -1) " +
                "if c <= 0 then redis.call('hdel', KEYS[1], ARGV[1]) end " +
                "redis.call('pexpire', KEYS[1], ARGV[2]) " +
                "return c",
            Long::class.java,
        )
    }
}
