package com.alphatalk.ws.subscription

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

@Component
class DemandLockMetrics(
    private val meters: MeterRegistry,
) {
    private val waitTimers = ConcurrentHashMap<String, Timer>()
    private val holdTimers = ConcurrentHashMap<String, Timer>()
    private val thresholdCounters = ConcurrentHashMap<Pair<String, String>, Counter>()
    private val bound = AtomicBoolean()

    fun bind(lock: ReentrantLock) {
        if (!bound.compareAndSet(false, true)) return
        Gauge.builder("ws.demand.lock.queued", lock) { it.queueLength.toDouble() }
            .description("DemandRegistry 락을 기다리는 스레드 수")
            .register(meters)
    }

    fun record(operation: String, waitNanos: Long, holdNanos: Long) {
        timer(waitTimers, "ws.demand.lock.wait", "DemandRegistry 락 획득 대기 시간", operation)
            .record(waitNanos, TimeUnit.NANOSECONDS)
        timer(holdTimers, "ws.demand.lock.hold", "DemandRegistry 락 보유 시간", operation)
            .record(holdNanos, TimeUnit.NANOSECONDS)
        val waitThreshold = threshold(operation, "wait")
        val holdThreshold = threshold(operation, "hold")
        if (waitNanos > THRESHOLD_NANOS) waitThreshold.increment()
        if (holdNanos > THRESHOLD_NANOS) holdThreshold.increment()
    }

    private fun timer(
        timers: ConcurrentHashMap<String, Timer>,
        name: String,
        description: String,
        operation: String,
    ): Timer = timers.computeIfAbsent(operation) {
        Timer.builder(name)
            .description(description)
            .tag("operation", operation)
            .publishPercentiles(0.95, 0.99)
            .serviceLevelObjectives(*SLO_BUCKETS)
            .maximumExpectedValue(Duration.ofSeconds(5))
            .register(meters)
    }

    private fun threshold(operation: String, phase: String): Counter =
        thresholdCounters.computeIfAbsent(operation to phase) {
            Counter.builder("ws.demand.lock.over.threshold")
                .description("DemandRegistry 락 단계가 100ms를 초과한 횟수")
                .tag("operation", operation)
                .tag("phase", phase)
                .register(meters)
        }

    companion object {
        const val THRESHOLD_MILLIS = 100L
        private val THRESHOLD_NANOS = TimeUnit.MILLISECONDS.toNanos(THRESHOLD_MILLIS)
        private val SLO_BUCKETS = arrayOf(
            Duration.ofMillis(1),
            Duration.ofMillis(5),
            Duration.ofMillis(10),
            Duration.ofMillis(25),
            Duration.ofMillis(50),
            Duration.ofMillis(100),
            Duration.ofMillis(250),
            Duration.ofSeconds(1),
        )
    }
}
