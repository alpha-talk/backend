package com.alphatalk.worker.ingest.dedup

import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration

class RedisSeenMarker(
    private val redis: StringRedisTemplate,
    private val ttl: Duration,
) : SeenMarker {

    override fun markIfNew(sourceId: String): Boolean =
        redis.opsForValue().setIfAbsent(Keys.seenIngest(sourceId), "1", ttl) == true

    override fun clear(sourceId: String) {
        redis.delete(Keys.seenIngest(sourceId))
    }
}
