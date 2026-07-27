package com.alphatalk.worker.ingest.dedup

import com.alphatalk.contracts.Keys
import com.alphatalk.worker.ingest.config.IngestProperties
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisSeenMarker(
    private val redis: StringRedisTemplate,
    props: IngestProperties,
) : SeenMarker {

    private val ttl = props.seenTtl

    override fun markIfNew(sourceId: String): Boolean =
        redis.opsForValue().setIfAbsent(Keys.seenIngest(sourceId), "1", ttl) == true

    override fun clear(sourceId: String) {
        redis.delete(Keys.seenIngest(sourceId))
    }
}
