package com.alphatalk.ws

import com.alphatalk.auth.JwtTokenProvider
import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Destinations
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.alphatalk.ws.subscription.DemandLockMetrics
import com.alphatalk.ws.watchlist.WatchlistResolver
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.apache.tomcat.websocket.Constants
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.messaging.simp.stomp.StompHeaders
import org.springframework.messaging.simp.stomp.StompSession
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.web.socket.WebSocketHttpHeaders
import org.springframework.web.socket.client.standard.StandardWebSocketClient
import org.springframework.web.socket.messaging.WebSocketStompClient
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.lang.reflect.Type
import java.time.Duration
import java.util.Collections
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

@Tag("load")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "alphatalk.auth.jwt.secret=$LOAD_TEST_SECRET",
        "ws.watchlist.reconcile-interval=1h",
        "logging.level.com.alphatalk.ws.subscription.SessionEventListener=WARN",
        "logging.level.com.alphatalk.ws.presence.RedisPresenceService=ERROR",
    ],
)
@Testcontainers
@Import(WsLoadSmokeTest.LoadTestConfiguration::class)
class WsLoadSmokeTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)
    }

    @TestConfiguration
    class LoadTestConfiguration {
        @Bean
        @Primary
        fun emptyWatchlistResolver() = WatchlistResolver { userIds ->
            userIds.map { WatchlistUpdated(it, 0, emptyList()) }
        }
    }

    @LocalServerPort
    private var port = 0

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    @Autowired
    private lateinit var meters: MeterRegistry

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private val clients = System.getProperty("ws.load.clients", "500").toInt()
    private val rounds = System.getProperty("ws.load.rounds", "5").toInt()
    private val fanoutSeconds = System.getProperty("ws.load.fanout.seconds", "30").toInt()
    private val connectBatchSize = System.getProperty("ws.load.connect.batch", "50").toInt()
    private val connectPauseMillis = System.getProperty("ws.load.connect.pause.millis", "0").toLong()
    private val connectTimeoutSeconds = System.getProperty("ws.load.connect.timeout.seconds", "30").toLong()
    private val codes = (1..41).map { it.toString().padStart(6, '0') }
    private val tokenProvider = JwtTokenProvider(LOAD_TEST_SECRET)
    private val sessions = Collections.synchronizedList(mutableListOf<StompSession>())
    private val transportErrors = ConcurrentLinkedQueue<Throwable>()
    private val scheduler = ThreadPoolTaskScheduler().apply {
        poolSize = 2
        setThreadNamePrefix("load-stomp-")
        initialize()
    }
    private val webSocketClient = StandardWebSocketClient().apply {
        setUserProperties(
            mapOf(Constants.IO_TIMEOUT_MS_PROPERTY to TimeUnit.SECONDS.toMillis(connectTimeoutSeconds).toString()),
        )
    }
    private val stomp = WebSocketStompClient(webSocketClient).apply {
        taskScheduler = scheduler
        setDefaultHeartbeat(longArrayOf(0, 0))
    }

    @AfterEach
    fun cleanup() {
        disconnectAll()
        stomp.stop()
        scheduler.shutdown()
    }

    @Test
    fun `single gateway keeps demand lock below 100ms and fans out 41 symbols to concurrent sessions`() {
        connectAll()
        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            assertThat(gauge("ws.sessions.connected")).isEqualTo(clients.toDouble())
        }
        val fixedRedisChannels = gauge("ws.redis.channels.subscribed")

        repeat(rounds) { round ->
            val subscriptions = subscribeBurst(round, null)
            unsubscribeBurst(subscriptions)
        }

        val latencies = ConcurrentLinkedQueue<Long>()
        val received = AtomicInteger()
        val subscriptions = subscribeBurst(rounds, latencyHandler(latencies, received))
        val maxOutboundQueue = AtomicInteger()
        val queueSampler = Executors.newSingleThreadScheduledExecutor()
        queueSampler.scheduleAtFixedRate(
            {
                maxOutboundQueue.accumulateAndGet(gauge("ws.client.outbound.queue.depth").toInt(), ::maxOf)
            },
            0,
            10,
            TimeUnit.MILLISECONDS,
        )

        val cycles = fanoutSeconds * QUOTE_HZ
        repeat(cycles) {
            val started = System.nanoTime()
            val timestamp = System.currentTimeMillis()
            codes.forEach { code ->
                redisTemplate.convertAndSend(
                    Channels.quote(code),
                    """{"type":"quote","code":"$code","ts":$timestamp,"data":{"price":71200}}""",
                )
            }
            val remaining = QUOTE_PERIOD_NANOS - (System.nanoTime() - started)
            if (remaining > 0) TimeUnit.NANOSECONDS.sleep(remaining)
        }

        val expected = clients * cycles
        await().atMost(Duration.ofSeconds(15)).until { received.get() >= ceil(expected * 0.99).toInt() }
        queueSampler.shutdownNow()

        val sortedLatencies = latencies.sorted()
        val p95 = percentile(sortedLatencies, 0.95)
        val deliveryRatio = received.get().toDouble() / expected
        val lockP99 = listOf("subscribe_room", "unsubscribe_by_id").associateWith { operation ->
            percentile(timer("ws.demand.lock.wait", operation))
        }
        val overThreshold = meters.find("ws.demand.lock.over.threshold").counters().sumOf { it.count() }
        val presenceFailures = meters.find("ws.presence.failures").counters().sumOf { it.count() }

        println(
            "ws-load clients=$clients rounds=$rounds expected=$expected received=${received.get()} " +
                "deliveryRatio=$deliveryRatio quoteP95Ms=$p95 lockWaitP99Ms=$lockP99 " +
                "lockOver100Ms=$overThreshold presenceFailures=$presenceFailures " +
                "outboundQueueMax=${maxOutboundQueue.get()}",
        )

        assertThat(transportErrors).isEmpty()
        assertThat(deliveryRatio).isGreaterThanOrEqualTo(0.99)
        assertThat(p95).isLessThanOrEqualTo(1_000)
        assertThat(lockP99.values).allMatch { it < DemandLockMetrics.THRESHOLD_MILLIS }
        assertThat(overThreshold).isZero()
        assertThat(presenceFailures).isZero()

        unsubscribeBurst(subscriptions)
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(gauge("ws.client.outbound.queue.depth")).isZero()
            assertThat(gauge("ws.redis.channels.subscribed")).isEqualTo(fixedRedisChannels)
        }
        disconnectAll()
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(gauge("ws.sessions.connected")).isZero()
        }
        assertThat(meters.find("ws.presence.failures").counters().sumOf { it.count() }).isZero()
        sessions.clear()
    }

    private fun disconnectAll() {
        sessions.toList().chunked(connectBatchSize).forEach { batch ->
            batch.forEach { runCatching { it.disconnect() } }
            if (connectPauseMillis > 0) TimeUnit.MILLISECONDS.sleep(connectPauseMillis)
        }
    }

    private fun connectAll() {
        (0 until clients).chunked(connectBatchSize).forEach { batch ->
            val futures = batch.map { index ->
                val connectHeaders = StompHeaders().apply {
                    add(
                        "Authorization",
                        "Bearer ${tokenProvider.issue(index + 1L, Duration.ofMinutes(15))}",
                    )
                }
                stomp.connectAsync(
                    "ws://localhost:$port/ws",
                    null as WebSocketHttpHeaders?,
                    connectHeaders,
                    object : StompSessionHandlerAdapter() {
                        override fun handleTransportError(session: StompSession, exception: Throwable) {
                            transportErrors.add(exception)
                        }
                    },
                )
            }
            futures.forEach { future ->
                try {
                    future.get(connectTimeoutSeconds + 5, TimeUnit.SECONDS).also {
                        sessions += it
                    }
                } catch (exception: Exception) {
                    throw AssertionError(
                        "WebSocket ramp-up failed after ${sessions.size}/$clients connected sessions",
                        exception,
                    )
                }
            }
            if (connectPauseMillis > 0) TimeUnit.MILLISECONDS.sleep(connectPauseMillis)
        }
    }

    private fun subscribeBurst(
        round: Int,
        handler: StompSessionHandlerAdapter?,
    ): List<StompSession.Subscription> {
        val before = timerCount("ws.demand.lock.wait", "subscribe_room")
        val executor = Executors.newFixedThreadPool(minOf(clients, 200))
        val start = CountDownLatch(1)
        val subscriptions = arrayOfNulls<StompSession.Subscription>(clients)
        sessions.forEachIndexed { index, session ->
            executor.submit {
                start.await()
                val headers = StompHeaders().apply {
                    destination = Destinations.roomQuote(codes[index % codes.size])
                    id = "load-$round-$index"
                }
                val subscription = session.subscribe(headers, handler ?: discardHandler())
                subscriptions[index] = subscription
            }
        }
        start.countDown()
        executor.shutdown()
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue()
        await().atMost(Duration.ofSeconds(30)).until {
            timerCount("ws.demand.lock.wait", "subscribe_room") >= before + clients
        }
        assertThat(transportErrors).isEmpty()
        return subscriptions.map { requireNotNull(it) }
    }

    private fun unsubscribeBurst(subscriptions: List<StompSession.Subscription>) {
        val before = timerCount("ws.demand.lock.wait", "unsubscribe_by_id")
        val executor = Executors.newFixedThreadPool(minOf(clients, 200))
        val start = CountDownLatch(1)
        subscriptions.forEach { subscription ->
            executor.submit {
                start.await()
                subscription.unsubscribe()
            }
        }
        start.countDown()
        executor.shutdown()
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue()
        await().atMost(Duration.ofSeconds(15)).until {
            timerCount("ws.demand.lock.wait", "unsubscribe_by_id") >= before + subscriptions.size
        }
    }

    private fun latencyHandler(
        latencies: ConcurrentLinkedQueue<Long>,
        received: AtomicInteger,
    ): StompSessionHandlerAdapter = object : StompSessionHandlerAdapter() {
        override fun getPayloadType(headers: StompHeaders): Type = ByteArray::class.java

        override fun handleFrame(headers: StompHeaders, payload: Any?) {
            val timestamp = objectMapper.readTree(payload as ByteArray).path("ts").asLong()
            latencies.add((System.currentTimeMillis() - timestamp).coerceAtLeast(0))
            received.incrementAndGet()
        }
    }

    private fun discardHandler(): StompSessionHandlerAdapter = object : StompSessionHandlerAdapter() {
        override fun getPayloadType(headers: StompHeaders): Type = ByteArray::class.java
    }

    private fun gauge(name: String): Double = meters.get(name).gauge().value()

    private fun timer(name: String, operation: String): Timer = meters.get(name).tag("operation", operation).timer()

    private fun timerCount(name: String, operation: String): Long =
        meters.find(name).tag("operation", operation).timer()?.count() ?: 0

    private fun percentile(timer: Timer): Long {
        val p99 = timer.takeSnapshot().percentileValues().firstOrNull { it.percentile() == 0.99 }
        return TimeUnit.NANOSECONDS.toMillis(p99?.value()?.toLong() ?: timer.max(TimeUnit.NANOSECONDS).toLong())
    }

    private fun percentile(values: List<Long>, percentile: Double): Long {
        require(values.isNotEmpty())
        val index = ceil(values.size * percentile).toInt().coerceIn(1, values.size) - 1
        return values[index]
    }
}

private const val LOAD_TEST_SECRET = "load-test-secret-0123456789abcdef"
private const val QUOTE_HZ = 4
private val QUOTE_PERIOD_NANOS = TimeUnit.SECONDS.toNanos(1) / QUOTE_HZ
