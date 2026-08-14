package com.alphatalk.worker.price.conflation

import com.alphatalk.worker.price.publish.DepthPublisher
import com.alphatalk.worker.price.publish.QuotePublisher
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock

@Component
@ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
class FlushScheduler(
    private val buffer: ConflationBuffer,
    private val publisher: QuotePublisher,
    private val depthBuffer: DepthConflationBuffer,
    private val depthPublisher: DepthPublisher,
    private val meters: MeterRegistry,
    private val clock: Clock = Clock.systemUTC(),
) {
    @Scheduled(fixedDelayString = "\${alphatalk.price.conflation-ms:200}")
    fun flush() {
        val quotes = buffer.drainDirty()
        val depths = depthBuffer.drainDirty()
        if (quotes.isEmpty() && depths.isEmpty()) return
        val ts = clock.millis()
        if (quotes.isNotEmpty()) {
            quotes.forEach { (code, data) -> publisher.publish(code, data, ts) }
            meters.counter("quote.published").increment(quotes.size.toDouble())
        }
        if (depths.isNotEmpty()) {
            depths.forEach { (code, data) -> depthPublisher.publish(code, data, ts) }
            meters.counter("depth.published").increment(depths.size.toDouble())
        }
    }
}
