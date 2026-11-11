package com.alphatalk.ws.subscription

import com.alphatalk.contracts.Destinations
import com.alphatalk.ws.auth.StompPrincipal
import com.alphatalk.ws.presence.PresenceRegistry
import com.alphatalk.ws.watchlist.WatchlistRefresh
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
    private val watchlistRefresh: WatchlistRefresh,
    private val presence: PresenceRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @EventListener
    fun onConnected(event: SessionConnectedEvent) {
        val sessionId = StompHeaderAccessor.wrap(event.message).sessionId ?: return
        val userId = (event.user as? StompPrincipal)?.userId ?: return
        demand.registerSession(sessionId, userId)
        presence.add(userId, sessionId)
        log.info("session connected. userId={} sessionId={}", userId, sessionId)
    }

    @EventListener
    fun onSubscribe(event: SessionSubscribeEvent) {
        val accessor = StompHeaderAccessor.wrap(event.message)
        val sessionId = accessor.sessionId ?: return
        if (event.user !is StompPrincipal) return
        demand.startWatchlist(sessionId)?.let(watchlistRefresh::loadInitial)

        val destination = accessor.destination ?: return
        val room = Destinations.parseRoomTopic(destination) ?: return
        val subscriptionId = accessor.subscriptionId ?: return
        demand.subscribeRoom(sessionId, subscriptionId, room.kind, room.code)
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
