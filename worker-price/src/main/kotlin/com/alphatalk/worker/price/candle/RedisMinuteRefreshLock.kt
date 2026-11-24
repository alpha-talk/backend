package com.alphatalk.worker.price.candle

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.worker.price.candle.MinuteRefreshLock.Claim
import com.alphatalk.worker.price.candle.MinuteRefreshLock.Result
import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import org.springframework.data.redis.connection.Message
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@Component
@ConditionalOnKisAccounts
class RedisMinuteRefreshLock(
    private val redis: StringRedisTemplate,
    private val resultReader: ReactiveStringRedisTemplate,
) : MinuteRefreshLock, MessageListener {
    private val waiters = ConcurrentHashMap<String, CompletableFuture<Unit>>()

    override fun claim(code: String, ttl: Duration): Claim {
        val candidate = UUID.randomUUID().toString()
        val owner = checkNotNull(
            redis.execute(CLAIM_SCRIPT, listOf(Keys.minuteRefreshLock(code)), candidate, ttl.toMillis().toString()),
        )
        return Claim(owner, acquired = owner == candidate)
    }

    override fun complete(jobId: String, result: Result) {
        val value = when (result) {
            is Result.Succeeded -> "ok:${result.synced}"
            Result.Failed -> "failed"
        }
        redis.execute(
            COMPLETE_SCRIPT, listOf(Keys.minuteRefreshResult(jobId)),
            value, RESULT_TTL_SECONDS.toString(), Channels.MINUTE_REFRESH_COMPLETED, jobId,
        )
    }

    override fun release(code: String, jobId: String) {
        redis.execute(RELEASE_SCRIPT, listOf(Keys.minuteRefreshLock(code)), jobId)
    }

    override fun onMessage(message: Message, pattern: ByteArray?) {
        waiters[message.body.toString(Charsets.UTF_8)]?.complete(Unit)
    }

    override fun awaitResult(jobId: String, timeout: Duration): Result? {
        val budget = timeout.toNanos()
        val deadline = System.nanoTime() + budget
        val signalDeadline = deadline - minOf(budget / 10, MAX_FINAL_READ_NANOS)
        val signal = CompletableFuture<Unit>()
        check(waiters.putIfAbsent(jobId, signal) == null)
        try {
            readResult(jobId, signalDeadline)?.let { return it }
            try {
                signal.get((signalDeadline - System.nanoTime()).coerceAtLeast(0), TimeUnit.NANOSECONDS)
            } catch (_: TimeoutException) {
            }
            return readResult(jobId, deadline)
        } finally {
            waiters.remove(jobId, signal)
        }
    }

    private fun readResult(jobId: String, deadline: Long): Result? {
        if (deadline - System.nanoTime() <= 0) return null
        val read = resultReader.opsForValue().get(Keys.minuteRefreshResult(jobId))
            .subscribeOn(Schedulers.boundedElastic())
            .toFuture()
        val value = try {
            read.get((deadline - System.nanoTime()).coerceAtLeast(0), TimeUnit.NANOSECONDS)
        } catch (_: TimeoutException) {
            null
        } finally {
            read.cancel(false)
        } ?: return null
        if (value == "failed") return Result.Failed
        return value.takeIf { it.startsWith("ok:") }?.substring(3)?.toIntOrNull()
            ?.takeIf { it >= 0 }?.let(Result::Succeeded)
    }

    companion object {
        private const val RESULT_TTL_SECONDS = 30L
        private val MAX_FINAL_READ_NANOS = TimeUnit.MILLISECONDS.toNanos(50)

        private val CLAIM_SCRIPT = DefaultRedisScript(
            """
            local existing = redis.call('get', KEYS[1])
            if existing then return existing end
            redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2])
            return ARGV[1]
            """.trimIndent(),
            String::class.java,
        )

        private val COMPLETE_SCRIPT = DefaultRedisScript(
            """
            redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2])
            return redis.call('publish', ARGV[3], ARGV[4])
            """.trimIndent(),
            Long::class.java,
        )

        private val RELEASE_SCRIPT = DefaultRedisScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long::class.java,
        )
    }
}
