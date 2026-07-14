package com.alphatalk.ws.config

import com.alphatalk.ws.auth.StompAuthChannelInterceptor
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.converter.DefaultContentTypeResolver
import org.springframework.messaging.converter.MappingJackson2MessageConverter
import org.springframework.messaging.converter.MessageConverter
import org.springframework.messaging.simp.config.ChannelRegistration
import org.springframework.messaging.simp.config.MessageBrokerRegistry
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import org.springframework.util.MimeTypeUtils
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker
import org.springframework.web.socket.config.annotation.StompEndpointRegistry
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration

@Configuration
@EnableWebSocketMessageBroker
class WebSocketConfig(
    private val props: WsProperties,
    private val authInterceptor: StompAuthChannelInterceptor,
    private val objectMapper: ObjectMapper,
) : WebSocketMessageBrokerConfigurer {
    @Bean
    fun wsHeartbeatScheduler(): ThreadPoolTaskScheduler = ThreadPoolTaskScheduler().apply {
        poolSize = 1
        setThreadNamePrefix("ws-heartbeat-")
        initialize()
    }

    override fun registerStompEndpoints(registry: StompEndpointRegistry) {
        registry.addEndpoint("/ws").setAllowedOriginPatterns("*")
    }

    override fun configureMessageBroker(registry: MessageBrokerRegistry) {
        registry.enableSimpleBroker("/topic", "/queue")
            .setHeartbeatValue(longArrayOf(10_000, 10_000))
            .setTaskScheduler(wsHeartbeatScheduler())
        registry.setUserDestinationPrefix("/user")
    }

    override fun configureClientInboundChannel(registration: ChannelRegistration) {
        registration.interceptors(authInterceptor)
    }

    override fun configureWebSocketTransport(registry: WebSocketTransportRegistration) {
        registry.setSendTimeLimit(props.transport.sendTimeLimitMs)
            .setSendBufferSizeLimit(props.transport.sendBufferSizeLimitBytes)
    }

    override fun configureMessageConverters(messageConverters: MutableList<MessageConverter>): Boolean {
        messageConverters.add(
            MappingJackson2MessageConverter().apply {
                this.objectMapper = this@WebSocketConfig.objectMapper
                contentTypeResolver = DefaultContentTypeResolver().apply {
                    defaultMimeType = MimeTypeUtils.APPLICATION_JSON
                }
            },
        )
        return true
    }
}
