package com.alphatalk.worker.ingest.queue

import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestProperties
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Repository

@Repository
class RedisDigestJobQueue(
    private val redis: StringRedisTemplate,
    props: IngestProperties,
) : DigestJobQueue {
    private val maxLen = props.queueMaxLen
    private val markerTtlMillis = props.seenTtl.toMillis()

    init {
        require(maxLen > 0)
        require(markerTtlMillis > 0)
    }

    override fun enqueueIfNew(entry: IngestQueueEntry): DigestEnqueueResult {
        require(entry.type == IngestType.DIGEST)
        val args = buildList {
            add(maxLen.toString())
            add(markerTtlMillis.toString())
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
            1L -> DigestEnqueueResult.ENQUEUED
            0L -> DigestEnqueueResult.ALREADY_ENQUEUED
            else -> error("unexpected digest enqueue result")
        }
    }

    private companion object {
        val ENQUEUE_IF_NEW_SCRIPT = DefaultRedisScript(
            """
            if redis.call('EXISTS', KEYS[1]) == 1 then
                return 0
            end
            local xaddArgs = {'MAXLEN', '~', ARGV[1], '*'}
            for index = 3, #ARGV do
                table.insert(xaddArgs, ARGV[index])
            end
            redis.call('XADD', KEYS[2], unpack(xaddArgs))
            redis.call('SET', KEYS[1], '1', 'PX', ARGV[2])
            return 1
            """.trimIndent(),
            Long::class.java,
        )
    }
}
