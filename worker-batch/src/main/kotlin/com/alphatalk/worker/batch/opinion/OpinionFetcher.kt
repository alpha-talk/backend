package com.alphatalk.worker.batch.opinion

import com.alphatalk.kis.rest.KisInvestOpinion
import java.time.LocalDate

fun interface OpinionFetcher {
    fun fetch(broker: Broker, from: LocalDate, to: LocalDate): List<KisInvestOpinion>
}
