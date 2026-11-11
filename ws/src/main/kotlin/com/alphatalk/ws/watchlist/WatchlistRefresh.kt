package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.WatchlistSnapshots
import com.alphatalk.ws.subscription.DemandMutator
import com.alphatalk.ws.subscription.DemandQuery
import com.alphatalk.ws.subscription.WatchlistTarget
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean

@Component
class WatchlistRefresh(
    private val query: DemandQuery,
    private val demand: DemandMutator,
    private val resolver: WatchlistResolver,
    @param:Qualifier("watchlistExecutor") private val executor: TaskExecutor,
    meters: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val failures = meters.counter("ws.watchlist.refresh.failures")
    private val inFlight = ConcurrentHashMap.newKeySet<WatchlistTarget>()
    private val reconciling = AtomicBoolean()
    private val refreshRequested = AtomicBoolean()

    fun loadInitial(target: WatchlistTarget) {
        if (!inFlight.add(target)) return
        submit(onRejected = { inFlight.remove(target) }) { fetch(listOf(target)) }
    }

    @Scheduled(fixedDelayString = "\${ws.watchlist.reconcile-interval:30s}")
    fun reconcile() {
        refreshRequested.set(true)
        startReconciliation()
    }

    private fun startReconciliation() {
        if (!reconciling.compareAndSet(false, true)) return
        submit(onRejected = { reconciling.set(false) }) {
            try {
                while (refreshRequested.getAndSet(false)) {
                    query.watchlistTargets().chunked(WatchlistSnapshots.MAX_BATCH_SIZE).forEach { batch ->
                        val targets = batch.filter(inFlight::add)
                        if (targets.isNotEmpty()) fetch(targets)
                    }
                }
            } finally {
                reconciling.set(false)
                if (refreshRequested.get()) startReconciliation()
            }
        }
    }

    private fun fetch(targets: List<WatchlistTarget>) {
        val byId = targets.associateBy { it.userId }
        try {
            val snapshots = resolver.resolve(byId.keys.toList())
            check(snapshots.size == byId.size && snapshots.map { it.userId }.toSet() == byId.keys) {
                "Watchlist snapshot response does not match request"
            }
            snapshots.forEach { demand.applyWatchlist(it, byId.getValue(it.userId)) }
        } catch (e: Exception) {
            failures.increment()
            log.warn("watchlist refresh failed. users={}", targets.size, e)
        } finally {
            inFlight.removeAll(targets.toSet())
        }
    }

    private fun submit(onRejected: () -> Unit, task: () -> Unit) {
        try {
            executor.execute(task)
        } catch (e: RejectedExecutionException) {
            onRejected()
            failures.increment()
            log.warn("watchlist refresh queue is full")
        }
    }
}
