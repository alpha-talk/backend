package com.alphatalk.worker.price.session

import com.alphatalk.kis.ws.KisSessionListener
import com.alphatalk.kis.ws.KisTick
import com.alphatalk.kis.ws.KisWebSocketSession
import com.alphatalk.worker.price.conflation.ConflationBuffer
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class FixedSubscriptionRunner(
    private val wsUrl: String,
    private val symbols: List<String>,
    private val approvalKey: () -> String,
    private val buffer: ConflationBuffer,
    private val meters: MeterRegistry,
    private val reconnectDelayMillis: Long = 3_000,
    private val connectTimeoutSeconds: Long = 10,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)

    @Volatile
    private var session: KisWebSocketSession? = null
    private var worker: Thread? = null

    override fun start() {
        if (!running.compareAndSet(false, true)) return
        worker = thread(name = "price-session", isDaemon = true) { runLoop() }
    }

    override fun stop() {
        running.set(false)
        session?.let { current ->
            runCatching { symbols.forEach { current.unsubscribe(it) } }
            runCatching { current.close() }
        }
        worker?.join(10_000)
    }

    override fun isRunning(): Boolean = running.get()

    private fun runLoop() {
        while (running.get()) {
            try {
                runSession()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            } catch (e: Exception) {
                log.warn("kis ws session failed", e)
            }
            if (!running.get()) return
            Thread.sleep(reconnectDelayMillis)
        }
    }

    private fun runSession() {
        val closed = CountDownLatch(1)
        val current = KisWebSocketSession(wsUrl, approvalKey(), FrameHandler(closed))
        session = current
        try {
            current.connect().get(connectTimeoutSeconds, TimeUnit.SECONDS)
            symbols.forEach { current.subscribe(it) }
            log.info("kis ws connected: symbols={}", symbols.size)
            while (running.get() && closed.count > 0) {
                closed.await(500, TimeUnit.MILLISECONDS)
            }
        } finally {
            runCatching { current.close() }
            session = null
        }
    }

    private inner class FrameHandler(private val closed: CountDownLatch) : KisSessionListener {
        override fun onTicks(ticks: List<KisTick>) {
            ticks.forEach(buffer::offer)
            meters.counter("tick.in").increment(ticks.size.toDouble())
        }

        override fun onSubscribeAck(trId: String?, trKey: String?, success: Boolean) {
            if (!success) {
                log.warn("subscribe rejected: trId={} trKey={}", trId, trKey)
            }
        }

        override fun onEncryptedDropped(trId: String) {
            log.warn("encrypted frame dropped: trId={}", trId)
        }

        override fun onClosed(reason: String?) {
            log.info("kis ws closed: {}", reason)
            closed.countDown()
        }

        override fun onError(t: Throwable) {
            log.warn("kis ws error", t)
        }
    }
}
