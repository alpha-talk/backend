package com.alphatalk.ws.relay

import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.alphatalk.ws.subscription.DemandRegistry
import com.alphatalk.ws.subscription.DemandSyncTrigger
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.beans.factory.ObjectProvider
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer

class RedisChannelSubscriberTest {
    @Test
    fun `a failed subscription is retried while the remaining snapshot is applied`() {
        val container = Mockito.mock(RedisMessageListenerContainer::class.java)
        val router = Mockito.mock(MessageRouter::class.java)
        @Suppress("UNCHECKED_CAST")
        val provider = Mockito.mock(ObjectProvider::class.java) as ObjectProvider<MessageRouter>
        Mockito.`when`(provider.getObject()).thenReturn(router)
        val meters = SimpleMeterRegistry()
        val subscriber = RedisChannelSubscriber(container, provider, meters) { it.run() }
        val registry = DemandRegistry(subscriber, DemandSyncTrigger())
        val failedTopic = ChannelTopic("quote:005930")
        Mockito.doThrow(IllegalStateException("offline")).doNothing()
            .`when`(container).addMessageListener(router, failedTopic)
        registry.registerSession("s1", 1L)
        registry.startWatchlist("s1")

        registry.applyWatchlist(WatchlistUpdated(1, 1, listOf("005930", "000660")))
        subscriber.retryFailedSubscriptions()

        Mockito.verify(container, Mockito.times(2)).addMessageListener(router, failedTopic)
        Mockito.verify(container).addMessageListener(router, ChannelTopic("stream:005930"))
        Mockito.verify(container).addMessageListener(router, ChannelTopic("quote:000660"))
        Mockito.verify(container).addMessageListener(router, ChannelTopic("stream:000660"))
        assertThat(registry.usersWatching("000660")).containsExactly(1L)
        assertThat(meters.counter("ws.relay.subscription.failures").count()).isEqualTo(1.0)
    }
}
