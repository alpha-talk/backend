package com.alphatalk.ws.relay

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.SmartLifecycle
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.data.redis.listener.Topic
import org.springframework.stereotype.Component
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

@Component
class RedisChannelSubscriber(
    private val container: RedisMessageListenerContainer,
    routerProvider: ObjectProvider<MessageRouter>,
    meters: MeterRegistry,
) : ChannelSubscriber, SmartLifecycle {
    private val router by lazy { routerProvider.getObject() }
    private val log = LoggerFactory.getLogger(javaClass)
    private val failures = meters.counter("ws.relay.subscription.failures")
    private val running = AtomicBoolean()
    private val changed = Semaphore(0)
    private val desired = HashSet<String>()

    private var worker: Thread? = null

    @Volatile
    private var registeredCount = 0

    override fun subscribe(channel: String) {
        synchronized(desired) {
            if (desired.add(channel)) wakeWorker()
        }
    }

    override fun unsubscribe(channel: String) {
        synchronized(desired) {
            if (desired.remove(channel)) wakeWorker()
        }
    }

    private fun wakeWorker() {
        if (changed.availablePermits() == 0) changed.release()
    }

    @Synchronized
    override fun start() {
        if (worker?.isAlive == true || !running.compareAndSet(false, true)) return
        val restoreListeners = worker != null
        synchronized(desired) { wakeWorker() }
        worker = thread(name = "redis-subscriptions", isDaemon = true) {
            try {
                runWorker(restoreListeners)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            } finally {
                running.set(false)
            }
        }
    }

    @Synchronized
    override fun stop() {
        if (!running.getAndSet(false)) return
        worker?.interrupt()
        worker?.join(3_000)
    }

    override fun isRunning(): Boolean = running.get()

    fun subscribedCount(): Int = registeredCount

    private fun runWorker(restoreListeners: Boolean) {
        var registered = emptySet<String>()
        var rebuild = restoreListeners
        while (running.get()) {
            changed.acquire()
            if (!running.get()) return

            changed.drainPermits()
            val target = synchronized(desired) { desired.toSet() }
            try {
                if (rebuild || (registered.isNotEmpty() && !container.isListening)) {
                    container.setMessageListeners(mapOf(router to target.toTopics()))
                } else {
                    val added = target - registered
                    val removed = registered - target
                    if (added.isNotEmpty()) container.addMessageListener(router, added.toTopics())
                    if (removed.isNotEmpty()) container.removeMessageListener(router, removed.toTopics())
                }
                registered = target
                registeredCount = target.size
                rebuild = false
            } catch (e: RuntimeException) {
                if (!running.get()) return
                failures.increment()
                log.warn("redis subscription update failed. retrying with current demand", e)
                rebuild = true
                Thread.sleep(REBUILD_DELAY_MILLIS)
                synchronized(desired) { wakeWorker() }
            }
        }
    }

    private fun Set<String>.toTopics(): List<Topic> = map(::ChannelTopic)

    companion object {
        private const val REBUILD_DELAY_MILLIS = 5_000L
    }
}
