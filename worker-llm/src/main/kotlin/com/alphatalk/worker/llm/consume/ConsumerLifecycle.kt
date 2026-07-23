package com.alphatalk.worker.llm.consume

import com.alphatalk.worker.llm.config.LlmProperties
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

@Component
@ConditionalOnProperty("alphatalk.llm.consume-enabled", havingValue = "true", matchIfMissing = true)
class ConsumerLifecycle(
    private val consumer: IngestConsumer,
    props: LlmProperties,
    meters: MeterRegistry,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val claimInterval = props.claimInterval
    private val pendingGauge = AtomicLong(0)
    private val running = AtomicBoolean(false)
    private var worker: Thread? = null

    init {
        Gauge.builder("queue.ingest.pending", pendingGauge, AtomicLong::toDouble)
            .description("queue:ingest 소비자 그룹 g:llm PEL 크기 (기획안 §10 알람 대상)")
            .register(meters)
    }

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
