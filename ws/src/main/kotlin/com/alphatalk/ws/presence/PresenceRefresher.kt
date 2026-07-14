package com.alphatalk.ws.presence

import com.alphatalk.ws.subscription.DemandQuery
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

@Component
class PresenceRefresher(
    private val demand: DemandQuery,
    private val presence: PresenceRegistry,
) {
    @Scheduled(
        fixedDelayString = "\${ws.presence.refresh-interval-seconds:10}",
        timeUnit = TimeUnit.SECONDS,
    )
    fun refresh() {
        val users = demand.connectedUserIds()
        if (users.isNotEmpty()) presence.refresh(users)
    }
}
