package com.alphatalk.worker.ingest.queue

import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.config.IngestProperties
import org.springframework.data.redis.connection.stream.StreamRecords
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Repository

@Repository
class RedisIngestQueue(
    private val redis: StringRedisTemplate,
    props: IngestProperties,
) : IngestQueue {

    private val maxLen = props.queueMaxLen

    override fun enqueue(entry: IngestQueueEntry) {
        val record = StreamRecords.mapBacked<String, String, String>(entry.toFields())
            .withStreamKey(Queues.INGEST)
        redis.opsForStream<String, String>().add(record)
        redis.opsForStream<String, String>().trim(Queues.INGEST, maxLen, true)
    }
}
