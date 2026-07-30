package com.alphatalk.ws.config

import com.alphatalk.ws.relay.RedisChannelSubscriber
import com.alphatalk.ws.subscription.DemandQuery
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
class MetricsConfig(
    meterRegistry: MeterRegistry,
    demand: DemandQuery,
    subscriber: RedisChannelSubscriber,
) {
    init {
        Gauge.builder("ws.sessions.connected") { demand.connectedSessionCount().toDouble() }
            .description("현재 연결된 STOMP 세션 수")
            .register(meterRegistry)
        Gauge.builder("ws.users.connected") { demand.connectedUserIds().size.toDouble() }
            .description("현재 연결된 유저 수")
            .register(meterRegistry)
        Gauge.builder("ws.redis.channels.subscribed") { subscriber.subscribedCount().toDouble() }
            .description("구독 중인 Redis 채널 수")
            .register(meterRegistry)
    }
}
