package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.redis.connection.RedisStringCommands.SetOption
import org.springframework.data.redis.core.RedisCallback
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.data.redis.core.types.Expiration
import org.springframework.stereotype.Component

@Component
class RedisWatchlistSnapshots(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
) {
    fun publish(snapshot: WatchlistUpdated) {
        val result = redis.execute(
            REPLACE_AND_PUBLISH,
            listOf(Keys.watchlistSnapshot(snapshot.userId)),
            snapshot.rev.toString(),
            mapper.writeValueAsString(snapshot),
            ttlSeconds(snapshot.userId).toString(),
            Channels.WATCHLIST_UPDATED,
        )
        check(result == 0L || result == 1L)
    }

    fun fillMissing(snapshots: List<WatchlistUpdated>) {
        redis.executePipelined(RedisCallback<Any> { connection ->
            snapshots.forEach { snapshot ->
                connection.stringCommands().set(
                    Keys.watchlistSnapshot(snapshot.userId).encodeToByteArray(),
                    mapper.writeValueAsBytes(snapshot),
                    Expiration.seconds(ttlSeconds(snapshot.userId)),
                    SetOption.SET_IF_ABSENT,
                )
            }
            null
        })
    }

    private fun ttlSeconds(userId: Long) = 300 + userId % 60

    companion object {
        private val REPLACE_AND_PUBLISH = DefaultRedisScript(
            """
            local value = redis.call('MGET', KEYS[1])[1]
            local current = value and string.match(value, '"rev"%s*:%s*(%d+)')
            local incoming = ARGV[1]
            if current and (#current > #incoming or (#current == #incoming and current > incoming)) then
              return 0
            end
            if current ~= incoming then
              redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
            end
            redis.call('PUBLISH', ARGV[4], ARGV[2])
            return 1
            """.trimIndent(),
            Long::class.javaObjectType,
        )
    }
}
