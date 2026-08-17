package com.alphatalk.worker.price.demand

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import com.alphatalk.worker.price.config.PriceProperties
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.SmartLifecycle
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Component
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

@Component
@ConditionalOnKisAccounts
class RedisDemandSource(
    private val redis: StringRedisTemplate,
    private val connectionFactory: RedisConnectionFactory,
    private val reconcileIntervalMs: Long = 10_000,
) : DemandSource, SmartLifecycle {
    @Autowired
    constructor(redis: StringRedisTemplate, connectionFactory: RedisConnectionFactory, props: PriceProperties) :
        this(redis, connectionFactory, props.demandReconcileMs)

    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)
    private val refreshSignal = Semaphore(0)

    @Volatile
    private var demanded: Set<String> = emptySet()

    private var container: RedisMessageListenerContainer? = null
    private var listenerExecutor: ThreadPoolTaskExecutor? = null
    private var reconciler: Thread? = null

    override fun targetSymbols(): Set<String> = demanded

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

    private fun aliveGatewayIds(): List<String> {
        val registered = redis.opsForSet().members(Keys.GW_REGISTRY).orEmpty().toList()
        if (registered.isEmpty()) return emptyList()
        val heartbeats = redis.opsForValue().multiGet(registered.map(Keys::gwAlive)).orEmpty()
        val alive = registered.filterIndexed { index, _ -> heartbeats.getOrNull(index) != null }
        sweepRegistry(registered - alive.toSet())
        return alive
    }

    internal fun sweepRegistry(dead: List<String>) {
        if (dead.isEmpty()) return
        val keys = listOf(Keys.GW_REGISTRY) + dead.map(Keys::gwAlive)
        runCatching { redis.execute(SWEEP_SCRIPT, keys, *dead.toTypedArray()) }
            .onFailure { log.warn("gateway registry sweep failed - retried on next reconcile", it) }
    }

    companion object {
        private val SWEEP_SCRIPT = DefaultRedisScript(
            "local removed = 0 " +
                "for i = 1, #ARGV do " +
                "if redis.call('exists', KEYS[i + 1]) == 0 then " +
                "removed = removed + redis.call('srem', KEYS[1], ARGV[i]) " +
                "end end " +
                "return removed",
            Long::class.java,
        )
    }

    private fun activeCodes(hashKey: String): Set<String> =
        redis.opsForHash<String, String>().entries(hashKey)
            .filterValues { (it.toLongOrNull() ?: 0L) > 0L }
            .keys

    override fun start() {
        if (!running.compareAndSet(false, true)) return
        refresh()
        listenerExecutor = ThreadPoolTaskExecutor().apply {
            corePoolSize = 2
            setThreadNamePrefix("demand-listener-")
            initialize()
        }
        container = RedisMessageListenerContainer().apply {
            setConnectionFactory(this@RedisDemandSource.connectionFactory)
            setTaskExecutor(listenerExecutor!!)
            addMessageListener({ _, _ -> refreshSignal.release() }, ChannelTopic(Channels.DEMAND_UPDATED))
            afterPropertiesSet()
            start()
        }
        reconciler = thread(name = "demand-reconcile", isDaemon = true) {
            while (running.get()) {
                try {
                    if (refreshSignal.tryAcquire(reconcileIntervalMs, TimeUnit.MILLISECONDS)) {
                        refreshSignal.drainPermits()
                    }
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@thread
                }
                if (!running.get()) return@thread
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
        listenerExecutor?.shutdown()
    }

    override fun isRunning(): Boolean = running.get()
}
