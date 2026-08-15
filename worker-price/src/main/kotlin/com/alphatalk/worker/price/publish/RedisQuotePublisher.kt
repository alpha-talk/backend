package com.alphatalk.worker.price.publish

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.envelope.Envelope
import com.alphatalk.contracts.envelope.QuoteData
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisQuotePublisher(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
) : QuotePublisher {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun publish(code: String, data: QuoteData, ts: Long): Boolean =
        runCatching {
            redis.opsForHash<String, String>().putAll(
                Keys.price(code),
                mapOf(
                    "price" to data.price.toString(),
                    "prevClose" to data.prevClose.toString(),
                    "change" to data.change.toString(),
                    "changeRate" to data.changeRate.toString(),
                    "open" to data.open.toString(),
                    "high" to data.high.toString(),
                    "low" to data.low.toString(),
                    "volume" to data.volume.toString(),
                    "ts" to ts.toString(),
                ),
            )
            val envelope = Envelope(type = ChannelKind.QUOTE.prefix, code = code, ts = ts, data = data)
            redis.convertAndSend(Channels.quote(code), mapper.writeValueAsString(envelope))
        }.onFailure {
            log.warn("quote publish failed (best-effort): code={}", code, it)
        }.isSuccess
}
