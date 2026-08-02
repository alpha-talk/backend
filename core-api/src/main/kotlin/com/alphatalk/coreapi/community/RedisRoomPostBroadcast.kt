package com.alphatalk.coreapi.community

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.envelope.Envelope
import com.alphatalk.contracts.envelope.PostData
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.event.TransactionalEventListener
import java.time.Clock

@Component
class RedisRoomPostBroadcast(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
    private val clock: Clock = Clock.systemUTC(),
) : RoomPostBroadcast {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun publish(code: String, eventId: String, data: PostData) {
        val envelope = Envelope(
            type = ChannelKind.POST.prefix,
            code = code,
            eventId = eventId,
            ts = clock.millis(),
            data = data,
        )
        runCatching {
            redis.convertAndSend(Channels.post(code), mapper.writeValueAsString(envelope))
        }.onFailure {
            log.warn("post publish failed (best-effort): code={} eventId={}", code, eventId, it)
        }
    }
}

@Component
class PostCommitBroadcaster(
    private val broadcast: RoomPostBroadcast,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @TransactionalEventListener
    fun on(event: PostCommitted) {
        runCatching {
            broadcast.publish(event.code, event.eventId, event.data)
        }.onFailure {
            log.warn("post broadcast listener failed (best-effort): code={} eventId={}", event.code, event.eventId, it)
        }
    }
}
