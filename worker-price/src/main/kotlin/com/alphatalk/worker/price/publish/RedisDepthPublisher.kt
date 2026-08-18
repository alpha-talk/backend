package com.alphatalk.worker.price.publish

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.envelope.DepthData
import com.alphatalk.contracts.envelope.Envelope
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisDepthPublisher(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
) : DepthPublisher {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun publish(code: String, data: DepthData, ts: Long): Boolean =
        runCatching {
            val envelope = Envelope(type = ChannelKind.DEPTH.prefix, code = code, ts = ts, data = data)
            redis.convertAndSend(Channels.depth(code), mapper.writeValueAsString(envelope))
        }.onFailure {
            log.warn("depth publish failed (best-effort): code={}", code, it)
        }.isSuccess
}
