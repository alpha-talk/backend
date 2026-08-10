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
import org.springframework.stereotype.Repository
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

@Repository
class RedisDemandSynchronizer(
    private val redis: StringRedisTemplate,
    private val objectMapper: ObjectMapper,
    private val snapshotSource: DemandSnapshotSource,
    private val trigger: DemandSyncTrigger,
    props: WsProperties,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)

    val gwId: String = UUID.randomUUID().toString().replace("-", "").take(12)

    private val syncIntervalMillis = props.demand.heartbeatIntervalSeconds * 1_000
    private val aliveTtl = Duration.ofSeconds(props.demand.aliveTtlSeconds)
    private val hashTtlMillis = Duration.ofSeconds(props.demand.hashTtlSeconds).toMillis().toString()

    @Volatile
    private var lastWritten = DemandSnapshot.EMPTY

    private var worker: Thread? = null

    override fun start() {
        if (!running.compareAndSet(false, true)) return
        log.info("demand synchronizer started. gwId={}", gwId)
        worker = thread(name = "demand-sync", isDaemon = true) {
            sync()
            while (running.get()) {
                try {
                    trigger.await(syncIntervalMillis)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@thread
                }
                if (!running.get()) return@thread
                sync()
            }
        }
    }

    override fun stop() {
        if (!running.compareAndSet(true, false)) return
        worker?.interrupt()
        worker?.join(3_000)
        withdraw()
    }

    override fun isRunning(): Boolean = running.get()

    internal fun sync() {
        try {
            val snapshot = snapshotSource.demandSnapshot()
            writeHash(Keys.demandQuote(gwId), snapshot.quote)
            writeHash(Keys.demandRoom(gwId), snapshot.room)
            redis.opsForValue().set(Keys.gwAlive(gwId), "1", aliveTtl)
            redis.opsForSet().add(Keys.GW_REGISTRY, gwId)
            publishTransitions(DemandUpdated.KIND_QUOTE, lastWritten.quote, snapshot.quote)
            publishTransitions(DemandUpdated.KIND_ROOM, lastWritten.room, snapshot.room)
            lastWritten = snapshot
        } catch (e: Exception) {
            log.warn("demand sync failed - retried on next cycle", e)
        }
    }

    internal fun withdraw() {
        try {
            redis.delete(listOf(Keys.gwAlive(gwId), Keys.demandQuote(gwId), Keys.demandRoom(gwId)))
            redis.opsForSet().remove(Keys.GW_REGISTRY, gwId)
        } catch (e: Exception) {
            log.warn("demand withdraw failed - keys expire by TTL, registry entry is swept by the reader", e)
        }
    }

    private fun writeHash(key: String, counts: Map<String, Int>) {
        val args = ArrayList<String>(1 + counts.size * 2)
        args += hashTtlMillis
        counts.forEach { (code, count) ->
            args += code
            args += count.toString()
        }
        redis.execute(REWRITE_SCRIPT, listOf(key), *args.toTypedArray())
    }

    private fun publishTransitions(kind: String, before: Map<String, Int>, after: Map<String, Int>) {
        (before.keys + after.keys).forEach { code ->
            val was = (before[code] ?: 0) > 0
            val now = (after[code] ?: 0) > 0
            if (was != now) {
                val payload = DemandUpdated(kind = kind, code = code, active = now, ts = System.currentTimeMillis())
                redis.convertAndSend(Channels.DEMAND_UPDATED, objectMapper.writeValueAsString(payload))
            }
        }
    }

    companion object {
        private val REWRITE_SCRIPT = DefaultRedisScript(
            "redis.call('del', KEYS[1]) " +
                "for i = 2, #ARGV, 2 do redis.call('hset', KEYS[1], ARGV[i], ARGV[i + 1]) end " +
                "if #ARGV > 1 then redis.call('pexpire', KEYS[1], ARGV[1]) end " +
                "return #ARGV",
            Long::class.java,
        )
    }
}
