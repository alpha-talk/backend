package com.alphatalk.worker.batch.opinion

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.envelope.Envelope
import com.alphatalk.contracts.envelope.StreamData
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Clock

@Component
class RedisOpinionPublisher(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
    private val clock: Clock = Clock.systemUTC(),
) : OpinionPublisher {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun publish(code: String, eventId: String, data: StreamData): Boolean {
        val envelope = Envelope(
            type = ChannelKind.STREAM.prefix,
            code = code,
            eventId = eventId,
            ts = clock.millis(),
            data = data,
        )
        return runCatching {
            redis.convertAndSend(Channels.stream(code), mapper.writeValueAsString(envelope))
        }.onFailure {
            log.warn("opinion publish failed, will retry next cycle: code={} eventId={}", code, eventId, it)
        }.isSuccess
    }
}
