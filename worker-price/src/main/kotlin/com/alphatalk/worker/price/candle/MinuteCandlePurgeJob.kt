package com.alphatalk.worker.price.candle

import com.alphatalk.worker.price.leader.LeaderLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class MinuteCandlePurgeJob(
    private val store: MinuteCandleStore,
    private val leader: LeaderLock,
    private val retentionDays: Long,
    private val today: () -> LocalDate = { LocalDate.now(ZoneId.of("Asia/Seoul")) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "0 30 4 * * *", zone = "Asia/Seoul")
    fun purgeDaily() {
        if (!leader.tryAcquire()) return
        purgeOnce()
    }

    fun purgeOnce(): Int {
        val cutoff = today().minusDays(retentionDays).format(DateTimeFormatter.BASIC_ISO_DATE)
        val purged = store.purgeBefore(cutoff)
        if (purged > 0) {
            log.info("minute candle purge: cutoff={} rows={}", cutoff, purged)
        }
        return purged
    }
}
