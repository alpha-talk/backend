package com.alphatalk.worker.llm.publish

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.envelope.Envelope
import com.alphatalk.contracts.envelope.StreamData
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Clock

class RedisStreamPublisher(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
    private val clock: Clock = Clock.systemUTC(),
) : StreamPublisher {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun publish(code: String, eventId: String, data: StreamData) {
        val envelope = Envelope(
            type = ChannelKind.STREAM.prefix,
            code = code,
            eventId = eventId,
            ts = clock.millis(),
            data = data,
        )
        runCatching {
            redis.convertAndSend(Channels.stream(code), mapper.writeValueAsString(envelope))
        }.onFailure {
            log.warn("stream publish failed (best-effort): code={} eventId={}", code, eventId, it)
        }
    }
}
