package com.alphatalk.worker.ingest.queue

import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.config.IngestProperties
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Repository

@Repository
class RedisIngestQueue(
    private val redis: StringRedisTemplate,
    props: IngestProperties,
) : IngestQueue {
    private val maxLen = props.queueMaxLen
    private val markerTtlMillis = props.seenTtl.toMillis()

    init {
        require(maxLen > 0)
        require(markerTtlMillis > 0)
    }

    override fun enqueueIfNew(entry: IngestQueueEntry): EnqueueResult =
        enqueue(entry, durable = false)

    fun enqueueCaptured(entry: IngestQueueEntry): EnqueueResult =
        enqueue(entry, durable = true)

    private fun enqueue(entry: IngestQueueEntry, durable: Boolean): EnqueueResult {
        val args = buildList {
            add(maxLen.toString())
            add(markerTtlMillis.toString())
            add(if (durable) "1" else "0")
            entry.toFields().forEach { (field, value) ->
                add(field)
                add(value)
            }
        }
        return when (
            redis.execute(
                ENQUEUE_IF_NEW_SCRIPT,
                listOf(Keys.seenIngest(entry.sourceId), Queues.INGEST),
                *args.toTypedArray(),
            )
        ) {
            1L -> EnqueueResult.ENQUEUED
            0L -> EnqueueResult.ALREADY_ENQUEUED
            else -> error("unexpected enqueue result")
        }
    }

    private companion object {
        val ENQUEUE_IF_NEW_SCRIPT = DefaultRedisScript(
            """
            local marker = redis.call('GET', KEYS[1])
            local durable = ARGV[3] == '1'
            if marker then
                if not durable then
                    return 0
                end
                if marker ~= '1' and #redis.call('XRANGE', KEYS[2], marker, marker, 'COUNT', 1) > 0 then
                    redis.call('PEXPIRE', KEYS[1], ARGV[2])
                    return 0
                end
            end
            local xaddArgs = {'MAXLEN', '~', ARGV[1], '*'}
            for index = 4, #ARGV do
                table.insert(xaddArgs, ARGV[index])
            end
            local id = redis.call('XADD', KEYS[2], unpack(xaddArgs))
            redis.call('SET', KEYS[1], durable and id or '1', 'PX', ARGV[2])
            return 1
            """.trimIndent(),
            Long::class.java,
        )
    }
}
