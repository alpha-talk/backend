package com.alphatalk.worker.price.session

import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import com.alphatalk.worker.price.config.PriceProperties
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

@Component
@ConditionalOnKisAccounts
class PriceLifecycle(
    private val orchestrator: PriceOrchestrator,
    private val maintainIntervalMs: Long,
) : SmartLifecycle {
    @Autowired
    constructor(orchestrator: PriceOrchestrator, props: PriceProperties) :
        this(orchestrator, props.maintainIntervalMs)

    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)
    private var worker: Thread? = null

    override fun start() {
        if (!running.compareAndSet(false, true)) return
        worker = thread(name = "price-orchestrator", isDaemon = true) {
            while (running.get()) {
                try {
                    orchestrator.tick()
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return@thread
                } catch (e: Exception) {
                    log.warn("orchestrator tick failed", e)
                }
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
