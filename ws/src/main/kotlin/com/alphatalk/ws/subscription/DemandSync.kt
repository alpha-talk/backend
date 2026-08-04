package com.alphatalk.ws.subscription

import org.springframework.stereotype.Component
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

data class DemandSnapshot(
    val quote: Map<String, Int>,
    val room: Map<String, Int>,
) {
    companion object {
        val EMPTY = DemandSnapshot(emptyMap(), emptyMap())
    }
}

interface DemandSnapshotSource {
    fun demandSnapshot(): DemandSnapshot
}

@Component
class DemandSyncTrigger {
    private val signal = Semaphore(0)

    fun request() {
        signal.release()
    }

    fun await(timeoutMillis: Long): Boolean {
        val triggered = signal.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS)
        if (triggered) signal.drainPermits()
        return triggered
    }
}
