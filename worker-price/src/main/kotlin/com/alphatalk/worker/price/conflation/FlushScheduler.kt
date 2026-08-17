package com.alphatalk.worker.price.conflation

import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import com.alphatalk.worker.price.publish.QuotePublisher
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock

@Component
@ConditionalOnKisAccounts
class FlushScheduler(
    private val buffer: ConflationBuffer,
    private val publisher: QuotePublisher,
    private val meters: MeterRegistry,
    private val clock: Clock = Clock.systemUTC(),
) {
    @Scheduled(fixedDelayString = "\${alphatalk.price.conflation-ms:200}")
    fun flush() {
        val drained = buffer.drainDirty()
        if (drained.isEmpty()) return
        val ts = clock.millis()
        drained.forEach { (code, data) -> publisher.publish(code, data, ts) }
        meters.counter("quote.published").increment(drained.size.toDouble())
    }
}
