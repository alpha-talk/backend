package com.alphatalk.ws.relay

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Channels
import com.alphatalk.ws.subscription.DemandRegistry
import com.alphatalk.ws.subscription.DemandSyncTrigger
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class RedisChannelSubscriberTest {
    @Test
    fun `blocked Redis subscribe does not delay other demand changes and latest state wins`() {
        val container = Mockito.mock(RedisMessageListenerContainer::class.java)
        val router = Mockito.mock(MessageRouter::class.java)
        val beans = DefaultListableBeanFactory().apply { registerSingleton("router", router) }
        val subscriber = RedisChannelSubscriber(container, beans.getBeanProvider(MessageRouter::class.java), SimpleMeterRegistry())
        val registry = DemandRegistry(subscriber, DemandSyncTrigger())
        val enteredRedis = CountDownLatch(1)
        val releaseRedis = CountDownLatch(1)
        val firstChannel = ChannelTopic(Channels.quote("005930"))
        val secondChannel = ChannelTopic(Channels.quote("000660"))
        Mockito.`when`(container.isListening).thenReturn(true)
        Mockito.doAnswer {
            enteredRedis.countDown()
            check(releaseRedis.await(5, TimeUnit.SECONDS))
            null
        }.`when`(container).addMessageListener(router, listOf(firstChannel))

        subscriber.start()
        try {
            registry.registerSession("first", 1L)
            registry.registerSession("second", 2L)
            registry.subscribeRoom("first", "quote", ChannelKind.QUOTE, "005930")
            assertThat(enteredRedis.await(3, TimeUnit.SECONDS)).isTrue()

            Executors.newSingleThreadExecutor().use { client ->
                val update = client.submit {
                    registry.subscribeRoom("second", "quote", ChannelKind.QUOTE, "000660")
                    registry.removeSession("first")
                }
                try {
                    update.get(1, TimeUnit.SECONDS)
                    assertThat(registry.isUserConnected(1L)).isFalse()
                } finally {
                    releaseRedis.countDown()
                }
            }

            await().atMost(Duration.ofSeconds(3)).untilAsserted {
                Mockito.verify(container).addMessageListener(router, listOf(secondChannel))
                Mockito.verify(container).removeMessageListener(router, listOf(firstChannel))
                assertThat(subscriber.subscribedCount()).isEqualTo(1)
            }
        } finally {
            releaseRedis.countDown()
            subscriber.stop()
        }
    }
}
