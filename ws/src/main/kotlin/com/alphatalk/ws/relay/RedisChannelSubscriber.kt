package com.alphatalk.ws.relay

import org.springframework.beans.factory.ObjectProvider
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class RedisChannelSubscriber(
    private val container: RedisMessageListenerContainer,
    routerProvider: ObjectProvider<MessageRouter>, // 순환 참조로 인해 Provider로 주입
) : ChannelSubscriber {
    private val router by lazy { routerProvider.getObject() }
    private val channels: MutableSet<String> = ConcurrentHashMap.newKeySet()

    override fun subscribe(channel: String) {
        if (channels.add(channel)) {
            container.addMessageListener(router, ChannelTopic(channel))
        }
    }

    override fun unsubscribe(channel: String) {
        if (channels.remove(channel)) {
            container.removeMessageListener(router, ChannelTopic(channel))
        }
    }

    fun subscribedCount(): Int = channels.size
}
