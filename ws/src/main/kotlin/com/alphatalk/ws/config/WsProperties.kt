package com.alphatalk.ws.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "ws")
data class WsProperties(
    val transport: Transport = Transport(),
    val presence: Presence = Presence(),
    val features: Features = Features(),
) {
    data class Transport(
        val sendTimeLimitMs: Int = 10_000,
        val sendBufferSizeLimitBytes: Int = 512 * 1024,
    )

    data class Presence(
        val ttlSeconds: Long = 30,
        val refreshIntervalSeconds: Long = 10,
    )

    data class Features(
        val tradeDepthEnabled: Boolean = false,
    )
}
