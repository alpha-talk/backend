package com.alphatalk.worker.batch.job

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StartupCatchUpTest {
    private val meters = SimpleMeterRegistry()
    private val seoul = ZoneId.of("Asia/Seoul")

    private fun at(text: String): ZonedDateTime = ZonedDateTime.parse("${text}+09:00[Asia/Seoul]")

    private fun task(name: String, cron: String, ran: MutableList<String>) =
        CatchUpTask(name, cron) { ran += name }

    @Test
    fun `오늘 실행 시각이 이미 지났으면 잡을 돌린다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters,
        ) { at("2026-08-07T18:30:00") }

        assertEquals(listOf("valuation_daily"), catchUp.runPending())
        assertEquals(listOf("valuation_daily"), ran)
        assertEquals(1.0, meters.counter("batch.catchup.triggered", "job", "valuation_daily").count())
    }

    @Test
    fun `자정 크론도 오늘 창으로 인정한다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(listOf(task("midnight_job", "0 0 0 * * *", ran)), meters) {
            at("2026-08-07T09:00:00")
        }

        assertEquals(listOf("midnight_job"), catchUp.runPending())
    }

    @Test
    fun `실행 시각 전에 뜬 기동은 돌리지 않는다 - 정규 스케줄이 곧 쏜다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters,
        ) { at("2026-08-07T10:00:00") }

        assertTrue(catchUp.runPending().isEmpty())
        assertTrue(ran.isEmpty())
    }

    @Test
    fun `오늘 실행 예정이 없는 날은 돌리지 않는다 - 주말`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters,
        ) { at("2026-08-08T23:00:00") }

        assertTrue(catchUp.runPending().isEmpty())
        assertTrue(ran.isEmpty())
    }

    @Test
    fun `등록 순서대로 실행한다 - 마스터가 먼저여야 뒤 잡이 빈 유니버스를 안 본다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            listOf(
                task("stock_master_sync", "0 0 8 * * *", ran),
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
                task("investor_flow_daily", "0 10 17 * * MON-FRI", ran),
                task("financials_sync", "0 0 6 * * *", ran),
            ),
            meters,
        ) { at("2026-08-07T19:00:00") }

        val expected = listOf("stock_master_sync", "valuation_daily", "investor_flow_daily", "financials_sync")
        assertEquals(expected, catchUp.runPending())
        assertEquals(expected, ran)
    }

    @Test
    fun `창이 지난 잡만 골라 돌린다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            listOf(
                task("financials_sync", "0 0 6 * * *", ran),
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
            ),
            meters,
        ) { at("2026-08-07T09:00:00") }

        assertEquals(listOf("financials_sync"), catchUp.runPending())
    }

    @Test
    fun `한 잡이 실패해도 다음 잡을 계속 돌린다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            listOf(
                CatchUpTask("stock_master_sync", "0 0 8 * * *") { throw IllegalStateException("boom") },
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
            ),
            meters,
        ) { at("2026-08-07T19:00:00") }

        assertEquals(listOf("valuation_daily"), catchUp.runPending())
        assertEquals(listOf("valuation_daily"), ran)
        assertEquals(1.0, meters.counter("batch.catchup.failed", "job", "stock_master_sync").count())
    }

    @Test
    fun `스케줄이 꺼진 잡은 따라잡지 않는다 - cron 비활성 값`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(listOf(task("stock_master_sync", "-", ran)), meters) {
            at("2026-08-07T19:00:00")
        }

        assertTrue(catchUp.runPending().isEmpty())
        assertTrue(ran.isEmpty())
        assertEquals(0.0, meters.counter("batch.catchup.failed", "job", "stock_master_sync").count())
    }

    @Test
    fun `앞 잡이 자정을 넘기면 남은 잡은 새 날짜로 다시 판정한다`() {
        val ran = mutableListOf<String>()
        var current = at("2026-08-07T23:50:00")
        val catchUp = StartupCatchUp(
            listOf(
                CatchUpTask("stock_master_sync", "0 0 8 * * *") {
                    ran += "stock_master_sync"
                    current = at("2026-08-08T00:10:00")
                },
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
            ),
            meters,
        ) { current }

        assertEquals(listOf("stock_master_sync"), catchUp.runPending())
        assertEquals(listOf("stock_master_sync"), ran)
    }

    @Test
    fun `등록된 잡이 없으면 기동 이벤트가 아무 일도 하지 않는다`() {
        val catchUp = StartupCatchUp(emptyList(), meters) { at("2026-08-07T19:00:00") }

        catchUp.onApplicationReady()

        assertTrue(catchUp.runPending().isEmpty())
    }
}
