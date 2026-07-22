package com.alphatalk.worker.llm.article

import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.net.URI
import java.time.Duration

fun interface ArticleRequestGate {
    fun await(uri: URI)
}

class RedisArticleRequestGate(
    private val redis: StringRedisTemplate,
    private val minInterval: Duration,
    private val sleeper: (Long) -> Unit = ::sleepPreservingInterrupt,
) : ArticleRequestGate {
    init {
        require(!minInterval.isNegative)
    }

    override fun await(uri: URI) {
        val intervalMillis = minInterval.toMillis()
        if (intervalMillis == 0L) return
        val host = requireNotNull(uri.host).lowercase()
        val key = Keys.articleFetchRate(host)
        while (true) {
            val waitMillis = requireNotNull(
                redis.execute(ACQUIRE_SCRIPT, listOf(key), intervalMillis.toString()),
            )
            if (waitMillis <= 0) return
            sleeper(waitMillis.coerceAtLeast(1))
        }
    }

    companion object {
        private fun sleepPreservingInterrupt(millis: Long) {
            try {
                Thread.sleep(millis)
            } catch (error: InterruptedException) {
                Thread.currentThread().interrupt()
                throw error
            }
        }

        private val ACQUIRE_SCRIPT = DefaultRedisScript(
            """
            local ttl = redis.call('pttl', KEYS[1])
            if ttl > 0 then
                return ttl
            end
            redis.call('psetex', KEYS[1], ARGV[1], '1')
            return 0
            """.trimIndent(),
            Long::class.java,
        )
    }
}
