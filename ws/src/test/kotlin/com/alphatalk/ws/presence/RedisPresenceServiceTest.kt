package com.alphatalk.ws.presence

import com.alphatalk.ws.config.WsProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.data.redis.core.StringRedisTemplate

class RedisPresenceServiceTest {
    @Test
    fun `Redis failure increments the bounded operation counter`() {
        val redis = Mockito.mock(StringRedisTemplate::class.java)
        val meters = SimpleMeterRegistry()
        Mockito.`when`(redis.opsForSet()).thenThrow(IllegalStateException("redis unavailable"))
        val service = RedisPresenceService(redis, WsProperties(), meters)

        service.add(1L, "session")

        assertThat(meters.get("ws.presence.failures").tag("operation", "add").counter().count()).isOne()
        assertThat(meters.get("ws.presence.failures").tag("operation", "remove").counter().count()).isZero()
        assertThat(meters.get("ws.presence.failures").tag("operation", "refresh").counter().count()).isZero()
    }
}
