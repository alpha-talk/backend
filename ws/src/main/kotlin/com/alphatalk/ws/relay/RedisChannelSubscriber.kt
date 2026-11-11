package com.alphatalk.ws.relay

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean

@Component
class RedisChannelSubscriber(
    private val container: RedisMessageListenerContainer,
    routerProvider: ObjectProvider<MessageRouter>,
    meters: MeterRegistry,
    @param:Qualifier("watchlistExecutor") private val executor: TaskExecutor,
) : ChannelSubscriber {
    private val router by lazy { routerProvider.getObject() }
    private val log = LoggerFactory.getLogger(javaClass)
    private val failures = meters.counter("ws.relay.subscription.failures")
    private val channels = HashSet<String>()
    private val pending = LinkedHashSet<String>()
    private val retrying = AtomicBoolean()

    @Synchronized
    override fun subscribe(channel: String) {
        if (channels.add(channel)) update(channel)
    }

    @Synchronized
    override fun unsubscribe(channel: String) {
        if (channels.remove(channel)) update(channel)
    }

    @Scheduled(fixedDelay = 5_000)
    fun retryFailedSubscriptions() {
        val failed = synchronized(this) { pending.take(32) }
        if (failed.isEmpty() || !retrying.compareAndSet(false, true)) return
        try {
            executor.execute {
                try {
                    failed.forEach { channel ->
                        synchronized(this) {
                            if (channel in pending) update(channel)
                        }
                    }
                } finally {
                    retrying.set(false)
                }
            }
        } catch (e: RejectedExecutionException) {
            retrying.set(false)
        }
    }

    @Synchronized
    fun subscribedCount(): Int = channels.size

    private fun update(channel: String) {
        pending.remove(channel)
        try {
            if (channel in channels) {
                container.addMessageListener(router, ChannelTopic(channel))
            } else {
                container.removeMessageListener(router, ChannelTopic(channel))
            }
        } catch (e: RuntimeException) {
            pending.add(channel)
            failures.increment()
            log.warn("redis subscription update failed. channel={}", channel, e)
        }
    }
}
