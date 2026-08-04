package com.alphatalk.worker.price.demand

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.ScanOptions
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class RedisDemandSource(
    private val redis: StringRedisTemplate,
    private val connectionFactory: RedisConnectionFactory,
    private val baseSymbols: Set<String>,
    private val reconcileIntervalMs: Long = 60_000,
) : DemandSource, SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)

    @Volatile
    private var demanded: Set<String> = emptySet()

    private var container: RedisMessageListenerContainer? = null
    private var reconciler: Thread? = null

    override fun targetSymbols(): Set<String> = baseSymbols + demanded

    fun refresh() {
        runCatching {
            val symbols = HashSet<String>()
            aliveGatewayIds().forEach { gwId ->
                symbols += activeCodes(Keys.demandQuote(gwId))
                symbols += activeCodes(Keys.demandRoom(gwId))
            }
            if (symbols != demanded) {
                log.info("demand changed: {} -> {} symbols", demanded.size, symbols.size)
            }
            demanded = symbols
        }.onFailure {
            log.warn("demand refresh failed - keeping previous {} symbols", demanded.size, it)
        }
    }

    private fun aliveGatewayIds(): List<String> =
        redis.scan(ScanOptions.scanOptions().match("${Keys.GW_ALIVE_PREFIX}*").count(100).build())
            .use { cursor -> cursor.asSequence().map { it.removePrefix(Keys.GW_ALIVE_PREFIX) }.toList() }

    private fun activeCodes(hashKey: String): Set<String> =
        redis.opsForHash<String, String>().entries(hashKey)
            .filterValues { (it.toLongOrNull() ?: 0L) > 0L }
            .keys

    override fun start() {
        if (!running.compareAndSet(false, true)) return
        refresh()
        container = RedisMessageListenerContainer().apply {
            setConnectionFactory(this@RedisDemandSource.connectionFactory)
            addMessageListener({ _, _ -> refresh() }, ChannelTopic(Channels.DEMAND_UPDATED))
            afterPropertiesSet()
            start()
        }
        reconciler = thread(name = "demand-reconcile", isDaemon = true) {
            while (running.get()) {
                try {
                    Thread.sleep(reconcileIntervalMs)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@thread
                }
                refresh()
            }
        }
    }

    override fun stop() {
        if (!running.compareAndSet(true, false)) return
        reconciler?.interrupt()
        container?.let {
            runCatching { it.destroy() }
                .onFailure { e -> log.warn("demand listener container shutdown failed", e) }
        }
    }

    override fun isRunning(): Boolean = running.get()
}
