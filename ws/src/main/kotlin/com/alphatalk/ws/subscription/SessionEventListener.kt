package com.alphatalk.ws.subscription

import com.alphatalk.contracts.Destinations
import com.alphatalk.ws.auth.StompPrincipal
import com.alphatalk.ws.presence.PresenceRegistry
import com.alphatalk.ws.watchlist.WatchlistResolver
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.messaging.simp.stomp.StompHeaderAccessor
import org.springframework.stereotype.Component
import org.springframework.web.socket.messaging.SessionConnectedEvent
import org.springframework.web.socket.messaging.SessionDisconnectEvent
import org.springframework.web.socket.messaging.SessionSubscribeEvent
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent

@Component
class SessionEventListener(
    private val demand: DemandMutator,
    private val watchlistResolver: WatchlistResolver,
    private val presence: PresenceRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @EventListener
    fun onConnected(event: SessionConnectedEvent) {
        val sessionId = StompHeaderAccessor.wrap(event.message).sessionId ?: return
        val userId = (event.user as? StompPrincipal)?.userId ?: return
        val watchlist = try {
            watchlistResolver.resolve(userId)
        } catch (e: Exception) {
            log.warn("watchlist resolve failed, starting empty. userId={} sessionId={}", userId, sessionId, e)
            emptySet()
        }
        demand.registerSession(sessionId, userId, watchlist)
        presence.add(userId, sessionId)
        log.info("session connected. userId={} sessionId={} watchlistSize={}", userId, sessionId, watchlist.size)
    }

    @EventListener
    fun onSubscribe(event: SessionSubscribeEvent) {
        val accessor = StompHeaderAccessor.wrap(event.message)
        val destination = accessor.destination ?: return

        val room = Destinations.parseRoomTopic(destination) ?: return
        val sessionId = accessor.sessionId ?: return
        val subscriptionId = accessor.subscriptionId ?: return
        demand.subscribeRoom(sessionId, subscriptionId, room.kind, room.code) // room 구독 말고는 connect 때 처리함
    }

    @EventListener
    fun onUnsubscribe(event: SessionUnsubscribeEvent) {
        val accessor = StompHeaderAccessor.wrap(event.message)
        val sessionId = accessor.sessionId ?: return
        val subscriptionId = accessor.subscriptionId ?: return
        demand.unsubscribeById(sessionId, subscriptionId)
    }

    @EventListener
    fun onDisconnect(event: SessionDisconnectEvent) {
        demand.removeSession(event.sessionId)
        (event.user as? StompPrincipal)?.let { presence.remove(it.userId, event.sessionId) }
        log.info("session disconnected. sessionId={} status={}", event.sessionId, event.closeStatus)
    }
}
