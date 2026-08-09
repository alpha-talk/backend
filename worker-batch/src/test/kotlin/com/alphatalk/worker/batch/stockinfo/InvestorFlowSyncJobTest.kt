package com.alphatalk.worker.batch.stockinfo

import com.alphatalk.kis.rest.KisInvestorFlow
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InvestorFlowSyncJobTest {
    private val meters = SimpleMeterRegistry()
    private val runs = FakeRuns()
    private val store = FakeInvestorFlowStore()
    private val weekday = LocalDate.parse("2026-08-07")

    private fun flows(code: String) = listOf(
        KisInvestorFlow(code, "20260807", -12000, 8000, 4000),
        KisInvestorFlow(code, "20260806", 1500, -900, -600),
    )

    private fun job(
        fetcher: InvestorFlowFetcher,
        stocks: List<ActiveStock> = listOf(ActiveStock("005930", null), ActiveStock("000660", null)),
        date: LocalDate = weekday,
        holidays: Set<LocalDate> = emptySet(),
    ) = InvestorFlowSyncJob(
        universe = FakeUniverse(stocks),
        fetcher = fetcher,
        store = store,
        runs = runs,
        meters = meters,
        holidays = holidays,
        today = { date },
    )

    @Test
    fun `응답의 모든 일자 행을 업서트해 놓친 날을 자가 회복한다`() {
        val stored = job(fetcher = { code -> flows(code) }).syncOnce()

        assertEquals(4, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        val row = store.rows.single { it.code == "005930" && it.date == "20260807" }
        assertEquals(-12000, row.individual)
        assertEquals(8000, row.foreign)
        assertEquals(4000, row.institution)
    }

    @Test
    fun `잔여 실패는 FAILED로 남겨 당일 재실행이 채울 수 있게 한다 - 전량 실패가 SUCCESS로 굳지 않게`() {
        val attempts = mutableMapOf<String, Int>()
        var brokenCode: String? = "000660"
        val fetcher = InvestorFlowFetcher { code ->
            attempts.merge(code, 1, Int::plus)
            when {
                code == "005930" && attempts.getValue(code) == 1 -> throw IllegalStateException("transient")
                code == brokenCode -> throw IllegalStateException("schema drift")
                else -> flows(code)
            }
        }

        val stored = job(fetcher = fetcher).syncOnce()

        assertEquals(2, stored)
        assertEquals(1, runs.lastFailCount)
        assertEquals(listOf("FAILED"), runs.finished)

        brokenCode = null
        job(fetcher = fetcher).syncOnce()

        assertEquals(listOf("FAILED", "SUCCESS"), runs.finished)
        assertTrue(store.rows.any { it.code == "000660" })
    }

    @Test
    fun `오늘 마스터 동기화가 온전하지 않으면 부분 유니버스를 처리하지 않는다`() {
        runs.unsucceededJobs += "stock_master_sync"

        val stored = job(fetcher = { flows(it) }).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("FAILED"), runs.finished)
        assertTrue(store.rows.isEmpty())
    }

    @Test
    fun `유니버스가 비면 FAILED다 - 선행 마스터 동기화 부재는 성공이 아니다`() {
        val stored = job(fetcher = { flows(it) }, stocks = emptyList()).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("FAILED"), runs.finished)
    }

    @Test
    fun `주말과 휴장일은 실행하지 않는다`() {
        assertEquals(0, job(fetcher = { flows(it) }, date = LocalDate.parse("2026-08-09")).syncOnce())
        assertEquals(0, job(fetcher = { flows(it) }, holidays = setOf(weekday)).syncOnce())
        assertTrue(runs.finished.isEmpty())
    }
}

internal class FakeInvestorFlowStore : InvestorFlowStore {
    val rows = mutableListOf<InvestorFlowRow>()

    override fun upsert(rows: List<InvestorFlowRow>): Int {
        this.rows += rows
        return rows.size
    }
}
