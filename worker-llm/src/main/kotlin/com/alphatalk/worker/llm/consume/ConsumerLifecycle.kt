package com.alphatalk.worker.llm.consume

import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

class ConsumerLifecycle(
    private val consumer: IngestConsumer,
    private val claimInterval: Duration,
    private val pendingGauge: AtomicLong,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)
    private var worker: Thread? = null

    override fun start() {
        if (!running.compareAndSet(false, true)) return
        consumer.ensureGroup()
        worker = thread(name = "llm-consumer", isDaemon = true) {
            var lastClaim = System.nanoTime()
            while (running.get()) {
                runCatching { consumer.pollOnce() }
                    .onFailure {
                        log.warn("consumer poll failed, backing off", it)
                        Thread.sleep(1_000)
                    }
                if (System.nanoTime() - lastClaim > claimInterval.toNanos()) {
                    runCatching { consumer.claimStale() }
                        .onFailure { log.warn("stale claim failed", it) }
                    pendingGauge.set(consumer.samplePending())
                    lastClaim = System.nanoTime()
                }
            }
        }
    }

    override fun stop() {
        running.set(false)
        worker?.join(10_000)
    }

    override fun isRunning(): Boolean = running.get()
}
