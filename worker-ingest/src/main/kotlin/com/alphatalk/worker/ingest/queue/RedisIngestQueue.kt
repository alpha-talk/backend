package com.alphatalk.worker.ingest.queue

import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.config.IngestProperties
import org.springframework.data.redis.connection.stream.StreamRecords
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component

@Component
class RedisIngestQueue(
    private val redis: StringRedisTemplate,
    props: IngestProperties,
) : IngestQueue {

    private val maxLen = props.queueMaxLen
    private val seenTtlSeconds = props.seenTtl.seconds

    override fun enqueue(entry: IngestQueueEntry) {
        val record = StreamRecords.mapBacked<String, String, String>(entry.toFields())
            .withStreamKey(Queues.INGEST)
        redis.opsForStream<String, String>().add(record)
        redis.opsForStream<String, String>().trim(Queues.INGEST, maxLen, true)
    }

    override fun enqueueIfNew(entry: IngestQueueEntry): Boolean {
        val args = buildList {
            add(maxLen.toString())
            add(seenTtlSeconds.toString())
            entry.toFields().forEach { (field, value) ->
                add(field)
                add(value)
            }
        }
        val result = redis.execute(
            ENQUEUE_IF_NEW_SCRIPT,
            listOf(Keys.seenIngest(entry.sourceId), Queues.INGEST),
            *args.toTypedArray(),
        )
        return result == 1L
    }

    companion object {
        private val ENQUEUE_IF_NEW_SCRIPT = DefaultRedisScript(
            """
            if redis.call('exists', KEYS[1]) == 1 then
                return 0
            end
            redis.call('xadd', KEYS[2], 'MAXLEN', '~', ARGV[1], '*', unpack(ARGV, 3))
            redis.call('set', KEYS[1], '1', 'EX', ARGV[2])
            return 1
            """.trimIndent(),
            Long::class.java,
        )
    }
}
