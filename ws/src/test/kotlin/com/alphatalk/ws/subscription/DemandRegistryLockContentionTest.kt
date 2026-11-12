package com.alphatalk.ws.subscription

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.alphatalk.ws.relay.ChannelSubscriber
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Tag("load")
class DemandRegistryLockContentionTest {
    private val contenders = System.getProperty("ws.load.lock.contenders", "10000").toInt()
    private val meters = SimpleMeterRegistry()
    private val registry = DemandRegistry(
        object : ChannelSubscriber {
            override fun subscribe(channel: String) = Unit
            override fun unsubscribe(channel: String) = Unit
        },
        DemandSyncTrigger(),
        DemandLockMetrics(meters),
    )

    @Test
    fun `ten thousand simultaneous operations keep lock wait below 100ms`() {
        val register = contend("register_session") { index ->
            registry.registerSession("session-$index", index + 1L)
        }
        val startWatchlist = contend("start_watchlist") { index ->
            registry.startWatchlist("session-$index")
        }
        val applyWatchlist = contend("apply_watchlist") { index ->
            registry.applyWatchlist(WatchlistUpdated(index + 1L, 0, emptyList()))
        }
        val subscribe = contend("subscribe_room") { index ->
            registry.subscribeRoom(
                "session-$index",
                "subscription-$index",
                ChannelKind.QUOTE,
                ((index % SYMBOLS) + 1).toString().padStart(6, '0'),
            )
        }
        val unsubscribe = contend("unsubscribe_by_id") { index ->
            registry.unsubscribeById("session-$index", "subscription-$index")
        }
        val overThreshold = meters.find("ws.demand.lock.over.threshold").counters().sumOf { it.count() }

        println(
            "ws-lock-load contenders=$contenders register=$register startWatchlist=$startWatchlist " +
                "applyWatchlist=$applyWatchlist subscribe=$subscribe unsubscribe=$unsubscribe " +
                "lockOver100Ms=$overThreshold",
        )

        val results = listOf(register, startWatchlist, applyWatchlist, subscribe, unsubscribe)
        assertThat(results).allMatch { it.completed == contenders }
        assertThat(results).allMatch { it.waitP99Millis < DemandLockMetrics.THRESHOLD_MILLIS }
        assertThat(overThreshold).isZero()
    }

    private fun contend(operation: String, action: (Int) -> Unit): ContentionResult {
        val ready = CountDownLatch(contenders)
        val start = CountDownLatch(1)
        val completed = CountDownLatch(contenders)
        val errors = ConcurrentLinkedQueue<Throwable>()
        val maxQueued = AtomicInteger()
        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            repeat(contenders) { index ->
                executor.submit {
                    ready.countDown()
                    start.await()
                    try {
                        action(index)
                    } catch (error: Throwable) {
                        errors.add(error)
                    } finally {
                        completed.countDown()
                    }
                }
            }
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue()
            val sampler = Executors.newSingleThreadExecutor()
            val started = System.nanoTime()
            start.countDown()
            sampler.submit {
                while (completed.count > 0) {
                    maxQueued.accumulateAndGet(
                        meters.get("ws.demand.lock.queued").gauge().value().toInt(),
                        ::maxOf,
                    )
                    Thread.onSpinWait()
                }
            }
            assertThat(completed.await(30, TimeUnit.SECONDS)).isTrue()
            sampler.shutdown()
            assertThat(sampler.awaitTermination(5, TimeUnit.SECONDS)).isTrue()
            assertThat(errors).isEmpty()
            val elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
            val timer = meters.get("ws.demand.lock.wait").tag("operation", operation).timer()
            return ContentionResult(
                completed = timer.count().toInt(),
                elapsedMillis = elapsedMillis,
                waitP99Millis = percentile(timer),
                waitMaxMillis = timer.max(TimeUnit.MILLISECONDS).toLong(),
                maxQueued = maxQueued.get(),
            )
        }
    }

    private fun percentile(timer: Timer): Long {
        val p99 = timer.takeSnapshot().percentileValues().firstOrNull { it.percentile() == 0.99 }
        return TimeUnit.NANOSECONDS.toMillis(p99?.value()?.toLong() ?: timer.max(TimeUnit.NANOSECONDS).toLong())
    }

    private data class ContentionResult(
        val completed: Int,
        val elapsedMillis: Long,
        val waitP99Millis: Long,
        val waitMaxMillis: Long,
        val maxQueued: Int,
    )

    companion object {
        private const val SYMBOLS = 41
    }
}
