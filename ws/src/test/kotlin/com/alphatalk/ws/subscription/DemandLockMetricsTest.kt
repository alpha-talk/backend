package com.alphatalk.ws.subscription

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class DemandLockMetricsTest {
    private val meters = SimpleMeterRegistry()
    private val metrics = DemandLockMetrics(meters)

    @Test
    fun `wait and hold metrics expose percentiles buckets threshold counters and queue depth`() {
        metrics.bind(ReentrantLock())
        metrics.record(
            operation = "subscribe_room",
            waitNanos = TimeUnit.MILLISECONDS.toNanos(101),
            holdNanos = TimeUnit.MILLISECONDS.toNanos(99),
        )

        assertThat(meters.get("ws.demand.lock.wait").tag("operation", "subscribe_room").timer().count())
            .isEqualTo(1)
        assertThat(meters.get("ws.demand.lock.hold").tag("operation", "subscribe_room").timer().count())
            .isEqualTo(1)
        assertThat(
            meters.get("ws.demand.lock.over.threshold")
                .tags("operation", "subscribe_room", "phase", "wait")
                .counter()
                .count(),
        ).isEqualTo(1.0)
        assertThat(
            meters.get("ws.demand.lock.over.threshold")
                .tags("operation", "subscribe_room", "phase", "hold")
                .counter()
                .count(),
        ).isZero()
        assertThat(meters.get("ws.demand.lock.queued").gauge().value()).isZero()
    }
}
