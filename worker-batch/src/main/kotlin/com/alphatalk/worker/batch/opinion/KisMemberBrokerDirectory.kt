package com.alphatalk.worker.batch.opinion

import com.alphatalk.kis.master.KisMemberParser
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.ZoneId

class KisMemberBrokerDirectory(
    private val download: () -> ByteArray,
    private val today: () -> LocalDate = { LocalDate.now(SEOUL) },
) : BrokerDirectory {
    private val log = LoggerFactory.getLogger(javaClass)

    @Volatile
    private var cached: Cached? = null

    override fun brokers(): List<Broker> {
        val date = today()
        cached?.takeIf { it.date == date }?.let { return it.brokers }
        return synchronized(this) {
            cached?.takeIf { it.date == date }?.brokers ?: refresh(date)
        }
    }

    private fun refresh(date: LocalDate): List<Broker> {
        val previous = cached
        val parsed = try {
            KisMemberParser.parse(download())
        } catch (e: Exception) {
            if (previous != null) {
                log.warn("member master refresh failed, using list from {}", previous.date, e)
                return previous.brokers
            }
            throw e
        }
        val brokers = parsed.members.filterNot { it.aggregate }.map { Broker(it.code, it.name) }
        check(brokers.isNotEmpty()) { "KIS 회원사 마스터가 비어 있다 (skipped=${parsed.skippedLines})" }
        if (parsed.skippedLines > 0) {
            if (previous != null) {
                log.warn(
                    "member master refresh is partial (skipped={}), keeping list from {}",
                    parsed.skippedLines, previous.date,
                )
                return previous.brokers
            }
            log.warn("member master is partial with no previous list, using it anyway: skipped={}", parsed.skippedLines)
        }
        cached = Cached(date, brokers)
        return brokers
    }

    private data class Cached(val date: LocalDate, val brokers: List<Broker>)

    companion object {
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
    }
}
