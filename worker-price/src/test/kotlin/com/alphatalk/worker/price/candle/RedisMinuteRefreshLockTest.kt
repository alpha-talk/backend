package com.alphatalk.worker.price.candle

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.worker.price.candle.MinuteRefreshLock.Result
import org.mockito.Mockito
import org.springframework.data.redis.connection.DefaultMessage
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.data.redis.core.ReactiveValueOperations
import org.springframework.data.redis.core.StringRedisTemplate
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RedisMinuteRefreshLockTest {
    private val jobId = "current"

    @Test
    fun `현재 작업의 알림을 받으면 Redis에서 기록된 성공 또는 실패 결과를 읽는다`() {
        val pool = Executors.newSingleThreadExecutor()
        try {
            val cases = listOf("ok:4" to Result.Succeeded(4), "failed" to Result.Failed)
            cases.forEach { (stored, expected) ->
                val firstRead = CountDownLatch(1)
                val result = AtomicReference<String>()
                val lock = lock {
                    Mono.defer {
                        val value = result.get()
                        firstRead.countDown()
                        Mono.justOrEmpty(value)
                    }
                }
                val waiter = pool.submit<Result?> { lock.awaitResult(jobId, Duration.ofSeconds(2)) }
                assertTrue(firstRead.await(1, TimeUnit.SECONDS))

                result.set(stored)
                lock.onMessage(completion(jobId), null)

                assertEquals(expected, waiter.get(1, TimeUnit.SECONDS))
            }
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `대기 등록 전에 완료된 작업은 첫 결과 조회에서 확인한다`() {
        val lock = lock { Mono.just("ok:0") }

        lock.onMessage(completion(jobId), null)

        assertEquals(Result.Succeeded(0), lock.awaitResult(jobId, Duration.ofSeconds(2)))
        assertEquals(Result.Succeeded(0), lock.awaitResult(jobId, Duration.ofSeconds(2)))
    }

    @Test
    fun `알림 유실 시 대기 만료의 마지막 조회에서 적재 건수를 확인한다`() {
        val reads = AtomicInteger()
        val lock = lock { if (reads.getAndIncrement() == 0) Mono.empty() else Mono.just("ok:17") }

        assertEquals(Result.Succeeded(17), lock.awaitResult(jobId, Duration.ofMillis(300)))
        assertEquals(2, reads.get())
    }

    @Test
    fun `이전 작업의 알림과 결과는 현재 작업에 영향을 주지 않는다`() {
        val reading = CountDownLatch(1)
        val lock = lock { key ->
            if (key == Keys.minuteRefreshResult("previous")) {
                Mono.just("ok:99")
            } else {
                reading.countDown()
                Mono.empty()
            }
        }
        val pool = Executors.newSingleThreadExecutor()
        try {
            val waiter = pool.submit<Result?> { lock.awaitResult(jobId, Duration.ofMillis(400)) }
            assertTrue(reading.await(1, TimeUnit.SECONDS))
            lock.onMessage(completion("previous"), null)

            assertFailsWith<TimeoutException> { waiter.get(50, TimeUnit.MILLISECONDS) }
            assertNull(waiter.get(1, TimeUnit.SECONDS))
            assertNull(lock.awaitResult(jobId, Duration.ofMillis(150)))
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `Redis 조회 실패 후 같은 작업을 다시 기다릴 수 있다`() {
        val reads = AtomicInteger()
        val lock = lock {
            if (reads.getAndIncrement() == 0) Mono.error(IllegalStateException("redis unavailable"))
            else Mono.just("failed")
        }

        assertFailsWith<ExecutionException> { lock.awaitResult(jobId, Duration.ofSeconds(2)) }
        assertEquals(Result.Failed, lock.awaitResult(jobId, Duration.ofSeconds(2)))
    }

    private fun completion(jobId: String): DefaultMessage =
        DefaultMessage(Channels.MINUTE_REFRESH_COMPLETED.toByteArray(), jobId.toByteArray())

    private fun lock(read: (String) -> Mono<String>): RedisMinuteRefreshLock {
        val template = Mockito.mock(ReactiveStringRedisTemplate::class.java)
        @Suppress("UNCHECKED_CAST")
        val ops = Mockito.mock(ReactiveValueOperations::class.java) as ReactiveValueOperations<String, String>
        Mockito.`when`(template.opsForValue()).thenReturn(ops)
        Mockito.`when`(ops.get(Mockito.anyString())).thenAnswer { read(it.getArgument(0)) }
        return RedisMinuteRefreshLock(StringRedisTemplate(), template)
    }
}
