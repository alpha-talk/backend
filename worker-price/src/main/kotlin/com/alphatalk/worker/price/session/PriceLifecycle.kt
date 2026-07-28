package com.alphatalk.worker.price.session

import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class PriceLifecycle(
    private val orchestrator: PriceOrchestrator,
    private val maintainIntervalMs: Long,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)
    private var worker: Thread? = null

    override fun start() {
        if (!running.compareAndSet(false, true)) return
        worker = thread(name = "price-orchestrator", isDaemon = true) {
            while (running.get()) {
                runCatching { orchestrator.tick() }
                    .onFailure { log.warn("orchestrator tick failed", it) }
                try {
                    Thread.sleep(maintainIntervalMs)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@thread
                }
            }
        }
    }

    override fun stop() {
        running.set(false)
        worker?.interrupt()
        worker?.join(10_000)
        runCatching { orchestrator.shutdown() }
            .onFailure { log.warn("orchestrator shutdown failed", it) }
    }

    override fun isRunning(): Boolean = running.get()
}
